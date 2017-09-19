package GuidData

import BasicData.VidTagsWeight.vid_idf_line
import Utils.Defines.TAG_HASH_LENGTH
import Utils.{Hash, TestRedisPool, Tools}
import Utils.Hash.hash_tf_idf
import Utils.Tools.KeyValueWeight
import breeze.linalg.SparseVector
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.udf

import scala.collection.mutable.ArrayBuffer

/**
  * Created by baronfeng on 2017/9/18.
  */
object GuidVtags {
  val REPARTITION_NUM = 400
  val now = System.currentTimeMillis / 1000


  val sv_weight: UserDefinedFunction = udf((features_data: Seq[Double], weight: Double, ts: Long)=> {
    val time_weight = Math.pow((now - ts).toDouble / (24*60*60), -0.35)
    val time_weight_res = if(time_weight>1) 1 else time_weight
    features_data.map(data => data * weight * time_weight_res)

  })


  def guid_vid_sv_join(spark: SparkSession, guid_input_path: String, vid_sv_input_path:String, output_path: String) : Unit = {
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
    val guid_data = spark.read.parquet(guid_input_path)
//      .filter(line=>{
//      val guid = line.getString(0)
//        Tools.boss_guid.contains(guid)
//      })
      .coalesce(REPARTITION_NUM)
    val vid_sv_data = spark.read.parquet(vid_sv_input_path).as[vid_idf_line]
      .select($"vid" as "vid2", $"tags", $"tags_source", $"features_index", $"features_data").repartition(REPARTITION_NUM)
      .cache()

    val guid_result_temp = guid_data.join(vid_sv_data, guid_data("vid") === vid_sv_data("vid2"), "left")
      .filter($"vid2".isNotNull && $"vid2" =!= "")
      .select($"guid", $"vid",  $"tags", $"tags_source", $"features_index", sv_weight($"features_data", $"percent", $"ts") as "features_data")
      .repartition(REPARTITION_NUM)

    guid_result_temp.createOrReplaceTempView("temp_db")
    // guid: String, vids:Seq[String], tags:Seq[String], features: (Seq[Int], Seq[Double])
    val sql_inner = "select guid, collect_list(vid) as vids, flatten_long(collect_set(tags)) as tags,  flatten_string(collect_set(tags_source)) as tags_source,  sv_flatten(collect_list(features_index), collect_list(features_data)) as features from temp_db group by guid "
    val sql_outer = "select guid, vids, tags, tags_source, features._1 as index, features._2 as data from ( " + sql_inner + " )"
    val guid_result = guid_result_temp.sqlContext.sql(sql_outer)
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
          val index = features_index.indexOf(hash_value)
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

          val index = features_index.indexOf(hash_value)
          if (index == -1) {
            //println("tag: " + tag + ", hash_value: " + hash_value + ", index: " + index)

          } else {

            val weight = features_data(index)
            ret_arr += ((tag_pure, tag_id, weight))
          }
        }
      }
      (guid, vids,  ret_arr)
    }).toDF("guid", "vids", "tagid_weight")
    println("write to parquet, to path: " + output_path)
    res_with_pair.write.mode("overwrite").parquet(output_path)
    println("--------[get guid tags done]--------")

  }

  def put_guid_tag_to_redis(spark: SparkSession, path : String): Unit = {
    println("put guid_tag to redis")
    import spark.implicits._
    val ip = "100.107.17.215"   //sz1159.show_video_hb_online.redis.com
    val port = 9039
    //val limit_num = 1000
    val bzid = "uc"
    val prefix = "G1"
    val tag_type: Int = 2501
    val data = spark.read.parquet(path)
 //     .filter(d => Tools.boss_guid.contains(d.getString(0)))
      .map(line=>{
      val guid = line.getString(0)
      val value_weight = line.getAs[Seq[Row]](2)
        .map(v=>(v.getLong(1).toString, v.getDouble(2))).sortBy(_._2)(Ordering[Double].reverse).take(100).distinct  //_1 tag _2 tag_id _3 weight
      KeyValueWeight(guid, value_weight)
    })

      .cache
    //data.collect().foreach(line=>println(line.key))
    //println("\n\n\n\n")
    //data.filter(d => Tools.boss_guid.contains(d.key)).show()
    val test_redis_pool = new TestRedisPool(ip, port, 40000)
    val broadcast_redis_pool = spark.sparkContext.broadcast(test_redis_pool)
    Tools.put_to_redis(data, broadcast_redis_pool, bzid, prefix, tag_type /*, limit_num = 1000 */)
    println("put to redis done, number: " + data.count)

  }

  def main(args: Array[String]) {


    /**
      * step1. create SparkSession object
      * 封装了spark sql的执行环境，是spark SQL程序的唯一入口
      */
    //System.setProperty("hadoop.home.dir", "C:\\winutils")

    val spark = SparkSession
      .builder
      .appName("GuidVtags")
      .getOrCreate()


    val guid_input_path = args(0)
    val vid_sv_input_path = args(1)
    val output_path = args(2)
    println("------------------[begin]-----------------")

    val guid_vtags_path = output_path
    //guid_vid_sv_join(spark, guid_input_path, vid_sv_input_path, guid_vtags_path)
    put_guid_tag_to_redis(spark, guid_vtags_path)

    println("------------------[done]-----------------")
  }

}
