package BasicData

import Utils.Tools.KeyValueWeight
import Utils.{Defines, TestRedisPool, Tools}
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Row, SparkSession}

import scala.collection.mutable.ArrayBuffer


/**
  * Created by baronfeng on 2017/8/30.
  */
object VidVidRecommanderV2 {
  // input_path: baronfeng/output/tag_recom_ver_1
  // sub_path: vid_idf
  // output_path: baronfeng/output/tag_recom_ver_1/vid_vid_recomm
  // filter_vid_path: /user/chenxuexu/vid_filter_v1/valid_vid_0830
  // return value: output_path

  val REPARTITION_NUM = 400

  def vid_filter(spark: SparkSession, video_idf_path: String, filter_vid_path: String, output_path: String) : Unit = {
    println("--------------[begin to process vid filter]--------------")
    println("filter video idf  from  [" + video_idf_path + "]")
    // vid_filter path 先找到
    val real_filter_vid_path = Tools.get_latest_subpath(spark, filter_vid_path)
    println("filter use vids from  [" + real_filter_vid_path + "]")
    println("filter output path is [" + output_path + "]")

    val video_info = spark.read.parquet(video_idf_path).coalesce(REPARTITION_NUM)
    println("print video_info schema.")
    video_info.printSchema()
    // json形式的过滤列表
    val filter_vid = spark.read.json(real_filter_vid_path).coalesce(REPARTITION_NUM).toDF("key")
    println("print vid_filter schema.")
    filter_vid.printSchema()
    val video_info_clean = video_info.join(filter_vid, "key").cache
    println("begin to write to path: " + output_path)
    video_info_clean.repartition(200).write.mode("overwrite").parquet(output_path)

    println(s"--------------[vid filter done, number ${video_info_clean.count}.]--------------")
  }

  def vid_vid_recomm_v2(spark: SparkSession,
                        tag_df_length: Int,
                        clean_input_path: String,
                        ctr_path: String,
                        output_path: String,
                        vid_recomm_num: Int): String = {
    import spark.implicits._
    spark.sqlContext.udf.register("vid_weight", (index: String, weight: Double) => {
      (index, weight)
    })
    println("--------------[begin recommend, source: " + clean_input_path + "]--------------")
    val clean_vid_data = spark.read.parquet(clean_input_path).as[KeyValueWeight]
      .flatMap(line => {
        val vid = line.key
        line.value_weight.map(vw => {(vid, vw._1.toLong, vw._2)})

      }).toDF("vid", "index", "weight").cache()
    println(s"clean vid number: ${clean_vid_data.count()}: ")
    clean_vid_data.show()

    clean_vid_data.createOrReplaceTempView("temp_db")

    val sql_str = "select index, collect_list(vid_weight(vid, weight)) as vid_weight from temp_db group by index"
    val index_data = spark.sql(sql_str).as[(Long, Seq[(String, Double)])].repartition(REPARTITION_NUM)

      .flatMap(line => {
        val ret_arr = new ArrayBuffer[(Long, String, String, Double)]

        val index = line._1
        val vid_weight_seq = line._2.sortBy(_._2)(Ordering[Double].reverse).take(tag_df_length)
        for (i <- vid_weight_seq.indices) {
          val vid1 = vid_weight_seq(i)._1
          val weight1 = vid_weight_seq(i)._2
          for (j <- vid_weight_seq.indices if j != i) {
            val vid2 = vid_weight_seq(j)._1
            val weight2 = vid_weight_seq(j)._2
            ret_arr.append((index, vid1, vid2, weight1 * weight2))
          }
        }
        ret_arr
      }).toDF("index", "vid1", "vid2", "similarity").repartition(REPARTITION_NUM, $"vid1")
    index_data.createOrReplaceTempView("index_db")


    val sql_str2_inner = "select vid1, vid2,  sum(similarity) as sim, collect_set(index) as indice from index_db group by vid1, vid2"
    val sql_str2_outer = s"select row_number() over (partition by vid1 sort by sim desc) as no, vid1, vid2, sim, indice from ($sql_str2_inner) t "
    val sql_str2_outer2 = s"select no as number, vid1, vid2, sim, indice from ($sql_str2_outer) where no <= $vid_recomm_num  distribute by vid1 sort by vid1, sim desc"
    val cos_data = spark.sql(sql_str2_outer2)

    // 加入ctr的相关数据
    val pure_ctr_path = Tools.get_latest_subpath(spark, ctr_path)
    val ctr_data = spark.read.parquet(pure_ctr_path).select($"vid", $"ctr").cache
    println(s"get CTR marks from path $pure_ctr_path, number ${ctr_data.count()}: ")
    ctr_data.show
    val ret_data = cos_data.select($"vid1", $"vid2", $"sim")
      .join(ctr_data, $"vid2" === ctr_data("vid"), "left")
      .select($"vid1", $"vid2", $"vid" as "vid_c", $"sim", $"ctr")
      .map(line => {
        val vid1 = line.getString(0)
        val vid2 = line.getString(1)
        val weight = if(line.isNullAt(2) || line.getString(2).isEmpty || line.getString(2) == "") {
          line.getDouble(3) * 0.1   // sim / 10
        } else {
          line.getDouble(4)
        }
        val weight_type = if(line.isNullAt(2)|| line.getString(2).isEmpty || line.getString(2) == "") {
          "sim"
        } else {
          "ctr"
        }
        (vid1, vid2, weight, weight_type)
      }).toDF("vid1", "vid2", "weight", "weight_type")

    ret_data.write.mode("overwrite").parquet(output_path)
    println(s"--------------[write cosSimilarity done. output: $output_path]--------------")

    output_path
  }

  val tuple2_udf: UserDefinedFunction = udf((v: String, sim: Double) => {
    (v, sim)
  })

  def put_vid_vid_to_redis(spark: SparkSession, path : String, flags: Seq[String]): Unit = {
    if(flags.contains("delete") || flags.contains("put")) {
      import spark.implicits._


      val ip = "100.107.17.216"
      val port = 9020
      //val limit_num = 1000
      val bzid = "sengine"
      val prefix = "v1_sv_nr"
      val tag_type: Int = -1
      val data = spark.read.parquet(path).select($"vid1", $"vid2", $"weight")
        .groupBy($"vid1").agg(collect_list(tuple2_udf($"vid2", $"weight")))
        .map(line => {
        val vid1 = line.getString(0)
        val vid2_sim = line.getSeq[Row](1).map(vid_sim_tp => (vid_sim_tp.getString(0), vid_sim_tp.getDouble(1)))
          .sortBy(_._2)(Ordering[Double].reverse).take(40).toArray
        //      val similarity = line.getDouble(2)
        //      val value_weight = Array((vid2, similarity))
        KeyValueWeight(vid1, vid2_sim)
      })
        //  .filter(d => Tools.boss_guid.contains(d.key))
        .cache

      data.show()
      val test_redis_pool = new TestRedisPool(ip, port, 40000)
      val broadcast_redis_pool = spark.sparkContext.broadcast(test_redis_pool)
      if (flags.contains("delete")) {
        Tools.delete_to_redis(data, broadcast_redis_pool, bzid, prefix, tag_type /*, limit_num = 1000 */)
      }
      if (flags.contains("put")) {
        Tools.put_to_redis(data, broadcast_redis_pool, bzid, prefix, tag_type /*, limit_num = 1000 */)
      }
      Tools.stat(spark, data, "V2") // 统计数据
      println("-----------------[put_vid_vid_to_redis] to redis done, number: " + data.count)
    }
  }


  def main(args: Array[String]) {

    /**
      * step1. create SparkSession object
      * 封装了spark sql的执行环境，是spark SQL程序的唯一入口
      */


    val spark = SparkSession
      .builder
      .appName(this.getClass.getName.split("\\$").last)

      .getOrCreate()


    val input_path = args(0)
    val ctr_path = args(1)
    val filter_path = args(2)
    val output_path = args(3)
    val tag_df_length = args(4).toInt
    val date = args(5)
    val vid_recomm_num = args(6).toInt
    val control_flag = args(7).split(Defines.FLAGS_SPLIT_STR, -1).toSet
    println("------------------[begin]-----------------")
    println("control flags is: " + control_flag.mkString("""||"""))
    val filter_output_path = output_path + "/vid_filter/" + date
    val vid_recomm_output_path = output_path + "/vid_vid_recomm_v2/" + date
    if(control_flag.contains("create")) {
      vid_filter(spark, video_idf_path = input_path, filter_vid_path = filter_path, filter_output_path)

      vid_vid_recomm_v2(spark, tag_df_length,
        clean_input_path = filter_output_path,
        ctr_path = ctr_path,
        output_path = vid_recomm_output_path,
        vid_recomm_num = vid_recomm_num)
    }

    // control_flags是在里面有控制
    put_vid_vid_to_redis(spark, path = vid_recomm_output_path, flags = control_flag.toSeq)



    println("------------------[done]-----------------")
  }
}
