package GuidData

import BasicData.VidTagsWeight.vid_idf_line
import Utils.Defines.TAG_HASH_LENGTH
import Utils.{Hash, TestRedisPool, Tools, Defines}
import Utils.Hash.hash_tf_idf
import Utils.Tools.KeyValueWeight
import breeze.linalg.SparseVector
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.udf

import scala.collection.mutable.ArrayBuffer

/**
  * Created by baronfeng on 2017/9/18.
  */
object GuidVtags {
  val REPARTITION_NUM = 400
  private val now = System.currentTimeMillis / 1000


  val sv_weight: UserDefinedFunction = udf((features_data: Seq[Double], weight: Double)=> {
    features_data.map(data => data * weight)
  })

  /**
    * 获取天级别的guid_vtags数据
    * @param spark 目前运行的spark_session
    * @param guid_input_path
    * @param vid_sv_input_path
    * @param output_path
    */
  def guid_vid_sv_join_daily(spark: SparkSession, guid_input_path: String, vid_sv_input_path:String, output_path: String) : Unit = {
    println("begin to get guid_vid_sv.")
    println("guid_input_path: " + guid_input_path)
    println("vid_sv_input_path: " + vid_sv_input_path)

    import spark.implicits._
    // tags合并用
    spark.sqlContext.udf.register("flatten_long", (xs: Seq[Seq[Long]]) => {
      xs.flatten.distinct
    })
    spark.sqlContext.udf.register("flatten_string", (xs: Seq[Seq[String]]) => {
      xs.flatten.distinct
    })
    // sparsevector 计算用
    spark.sqlContext.udf.register("sv_weight", (features_data: Seq[Double], weight: Double)=> features_data.map(data=> data * weight))
    // 向量的相加
    spark.sqlContext.udf.register("sv_flatten", (features_indice: Seq[Seq[Int]], features_datas: Seq[Seq[Double]]) => {
      if(features_datas.length != features_indice.length) {
        null
      } else {
        val sv_ret = new SparseVector[Double](new Array[Int](0), new Array[Double](0), TAG_HASH_LENGTH)
        for(i <- features_indice.indices) {
          sv_ret += new SparseVector[Double](features_indice(i).toArray, features_datas(i).toArray, TAG_HASH_LENGTH)
        }
        (sv_ret.index, sv_ret.data)
      }
    })

    // guid, ts, vid, playduration, duration, percent
    val guid_data = spark.read.parquet(guid_input_path).repartition(REPARTITION_NUM)
    val vid_sv_data = spark.read.parquet(vid_sv_input_path).as[vid_idf_line]
      .select($"vid" as "vid2", $"tags", $"tags_source", $"features_index", $"features_data").repartition(REPARTITION_NUM)
      .cache()

    val guid_result_temp = guid_data.join(vid_sv_data, guid_data("vid") === vid_sv_data("vid2"), "inner")
    //  .filter($"vid2".isNotNull && $"vid2" =!= "")
      .select($"guid", $"vid",  $"tags", $"tags_source", $"features_index", sv_weight($"features_data", $"percent") as "features_data")
      .repartition(REPARTITION_NUM, $"guid")

    guid_result_temp.createOrReplaceTempView("temp_db")
    // guid: String, vids:Seq[String], tags:Seq[String], features: (Seq[Int], Seq[Double])
    val sql_inner = "select guid, collect_list(vid) as vids, flatten_long(collect_set(tags)) as tags,  flatten_string(collect_set(tags_source)) as tags_source,  sv_flatten(collect_list(features_index), collect_list(features_data)) as features from temp_db group by guid "
    val sql_outer = "select guid, vids, tags, tags_source, features._1 as index, features._2 as data from ( " + sql_inner + " )"
    val guid_result = guid_result_temp.sqlContext.sql(sql_outer).repartition(REPARTITION_NUM)
    guid_result.printSchema()


    val res_with_pair = guid_result.map(line => {  // guid vids tags tags_source features_index features_data
      val guid = line.getString(0)
      val vids = line.getAs[Seq[String]](1)
      val tag_ids = line.getAs[Seq[Long]](2)
      val tags_source = line.getAs[Seq[String]](3)
      val features_index = line.getAs[Seq[Int]](4)
      val features_data = line.getAs[Seq[Double]](5)
      val ret_arr = new ArrayBuffer[(String, Long, Double)]
      for(tag<-tags_source.distinct){
        //区分inter和outer id，映射回来
        if(tag.contains("_inner")) {
          val tag_pure = tag.replace("_inner", "")

          val tag_id = Hash.hash_own(tag_pure)
          val hash_value = hash_tf_idf(Hash.hash_own(tag_pure))
          val index = if(features_index == null || features_index.isEmpty) -1 else features_index.indexOf(hash_value)
          if (index == -1) {
            //println("tag: " + tag + ", hash_value: " + hash_value + ", index: " + index)

          } else {

            val weight = features_data(index)
            ret_arr += ((tag_pure, tag_id, weight))
          }
        } else {  // 先锋id   tag:  xxx##123456这样的形式
          val tag_data = tag.split("##")
          val tag_pure = tag_data(0)
          val tag_id = tag_data(tag_data.length - 1).toLong
          val hash_value = hash_tf_idf(tag_id)

          val index = if(features_index == null || features_index.isEmpty) -1 else features_index.indexOf(hash_value)

          // if exists
          if (index != -1) {
            val weight = features_data(index)
            ret_arr += ((tag_pure, tag_id, weight))
          }
        }
      }
      val arr_normal = ret_arr.map(tp=>{
        (tp._1, tp._2, Tools.normalize(tp._3))
      })
      (guid, vids,  ret_arr, arr_normal)
    }).toDF("guid", "vids", "tagid_weight", "tagid_weight_normalize")
    println("write to parquet, to path: " + output_path)
    res_with_pair.write.mode("overwrite").parquet(output_path)
    println("--------[get guid tags done]--------")

  }

  // 目前monthly去掉vids
  /**
    * 产生本月的所有数据，产生的数量由tools中的monthly确定，务必确认一下，目前视数据规模定为10天和30天，之后可能会扩展为90天。
    * 目前的monthly数据中取出了vids、tag_name，是由于数据规模决定的，没办法更大了，如果更大的话经常运行失败。
    *
    * @param spark 目前运行的spark_session
    * @param guid_vtags_path daily的数据目录，取多少天的，由tools控制
    * @param output_path 线上数据暂定为GuidVtagsMonthly
    */
  def get_guid_vtags_monthly(spark: SparkSession, guid_vtags_path: String, output_path: String): Unit = {
    import spark.implicits._

    // tag_tagid_weight 格式为： tag:String tagid:Long weight: Double
    spark.sqlContext.udf.register("flatten_tagid", (xs: Seq[Seq[Row]]) => {
      val data = xs.map(line=>
        line.map(arr=>(arr.getLong(0), arr.getDouble(1)))
      )
      val ret = data.flatten.groupBy(_._1).map(line=>{
        val tagid = line._1
        val weight = line._2.map(_._2).sum
        (tagid, weight)
      })
      ret.toSeq.sortBy(_._2)(Ordering[Double].reverse).take(200)
    })

    spark.sqlContext.udf.register("flatten_vids", (xs: Seq[Seq[String]]) => {
      xs.flatten.distinct
    })

    val subpaths = Tools.get_last_month_date_str()
      .map(date=>(date, guid_vtags_path + "/" + date))
      .filter(kv => {Tools.is_path_exist(spark, kv._2) && Tools.is_flag_exist(spark, kv._2)})

    val today = Tools.get_n_day(0)
    if(subpaths.length == 0) {
      println("error, we have no vid data here!")
      return
    } else {
      println(f"we have ${subpaths.length}%d days guid vtags data to process.")
    }

    var guid_data_total: DataFrame = null
    for(guid_path_kv <- subpaths) {
      val diff_day = Tools.diff_date(guid_path_kv._1, today)
      val time_weight = if(diff_day == 0) 1 else Math.pow(diff_day.toDouble, -0.35)

      val time_weight_res = {
        if (time_weight > 1) {
          println(s"warning!!, time_weight upper than 1! value:$time_weight")
          1
        } else {
          time_weight
        }
      }

      // daily的数据为 "guid", "vids", "tagid_weight", "tagid_weight_normalize"  删掉最后一个
      val guid_data_daily = spark.read.parquet(guid_path_kv._2).select("guid", "tagid_weight")
        .map(line=>{
          val guid = line.getString(0)
//          val vids = line.getSeq[String](1)
          val tag_tagid_weight = line.getSeq[Row](1).map(kv=>(kv.getLong(1), kv.getDouble(2) * time_weight_res))
          (guid, tag_tagid_weight)
        }).toDF("guid", "tagid_weight")

      if(guid_data_total == null) {
        guid_data_total = guid_data_daily
      } else {
        guid_data_total = guid_data_total.union(guid_data_daily).toDF("guid", "tagid_weight")
      }
    }

    val guid_data_total_res = guid_data_total
    guid_data_total_res.createOrReplaceTempView("guid_data_db")
    val vid_data_sql_inner = "select guid, collect_list(tagid_weight) as tagid_weight from guid_data_db group by guid"
    val vid_data_sql_outer = s"select guid, flatten_tagid(tagid_weight) as tagid_weight from ( $vid_data_sql_inner  ) t"
    val vid_data_res = spark.sql(vid_data_sql_outer).coalesce(REPARTITION_NUM)
    vid_data_res.write.mode("overwrite").parquet(output_path)

  }

  /**
    * 把数据放到redis中去，有可能是先删再放
    *
    * @param path 这个位置的数据必须满足 get_guid_vtags_monthly()中的风格，也就是string || Seq[Long, Double]的风格
    * @param flag flag中如果有delete就先删除掉redis中的数据；如果有put则将数据放入redis中
    *
    */

  def put_guid_tag_to_redis(spark: SparkSession, path : String, flag: ArrayBuffer[String]): Unit = {
    println("put guid_tag to redis")
    import spark.implicits._
    val ip = "100.107.17.215"   //sz1159.show_video_hb_online.redis.com
    val port = 9039
    //val limit_num = 1000
    val bzid = "uc"
    val prefix = "G1"
    val tag_type: Int = 2501
    val data = spark.read.parquet(path)

      .map(line=>{
      val guid = line.getString(0)
      val value_weight = line.getAs[Seq[Row]](1)
        .map(v=>(v.getLong(0).toString, v.getDouble(1))).sortBy(_._2)(Ordering[Double].reverse).take(30).distinct
          .map(tp=>(tp._1, Tools.normalize(tp._2)))
      //_1 tag _2 tag_id _3 weight
      KeyValueWeight(guid, value_weight)
    })

      .cache
    //data.collect().foreach(line=>println(line.key))
    //println("\n\n\n\n")
    //data.filter(d => Tools.boss_guid.contains(d.key)).show()
    val test_redis_pool = new TestRedisPool(ip, port, 40000)
    val broadcast_redis_pool = spark.sparkContext.broadcast(test_redis_pool)
    if(flag.contains("delete")) {
      Tools.delete_to_redis(data, broadcast_redis_pool, bzid, prefix, tag_type /*, limit_num = 1000 */)
      println(s"redis delete done, number: ${data.count}")
    }

    if(flag.contains("put")) {
      Tools.put_to_redis(data, broadcast_redis_pool, bzid, prefix, tag_type /*, limit_num = 1000 */)
      println(s"put to redis done, number: ${data.count}")
    }

  }

  def main(args: Array[String]) {


    /**
      * step1. create SparkSession object
      * 封装了spark sql的执行环境，是spark SQL程序的唯一入口
      */
    //System.setProperty("hadoop.home.dir", "C:\\winutils")

    val spark = SparkSession
      .builder
      .appName(this.getClass.getName.split("\\$").last)
      .getOrCreate()


    val guid_input_path = args(0)
    val vid_sv_input_path = args(1)
    val output_path_daily_total = args(2)
    val date = args(3)
    val monthly_output_path = args(4)  // GuidVtagsMonthly

    val control_flag = args(5).split(Defines.FLAGS_SPLIT_STR, -1).toSet
    println("------------------[begin]-----------------")
    println("control flags is: " + control_flag.mkString("""||"""))

    if(control_flag.contains("daily")) {
      println("we will get guid vid data [daily]")
      guid_vid_sv_join_daily(spark, guid_input_path, vid_sv_input_path, output_path = output_path_daily_total + "/" + date)
    }

    if(control_flag.contains("monthly")) {
      println("we will get guid vid data [monthly]")
      get_guid_vtags_monthly(spark, guid_vtags_path = output_path_daily_total, output_path = monthly_output_path)
    }

    val flag: ArrayBuffer[String] = new ArrayBuffer[String]()
    if(control_flag.contains("delete")) {
      flag.append( "delete")
    }
    if(control_flag.contains("put")) {
      flag.append("put")
    }

    if(flag.nonEmpty) {
      put_guid_tag_to_redis(spark, path = monthly_output_path, flag = flag)
    } else {
      println("there's no control flags of redis put/delete, skip.")
    }
    println("------------------[done]-----------------")
  }

}
