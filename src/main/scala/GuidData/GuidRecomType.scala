package GuidData

import Utils.Tools.KeyValueWeight
import Utils.{Defines, TestRedisPool, Tools}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

/**
  * Created by baronfeng on 2017/10/24.
  */
object GuidRecomType {
  val REPARTITION_NUM = 400

  /**
    * 从cid_info和vid_info那里取得各项数据，并联立成表
    * @param vid_info_path /data/stage/outface/omg/tdw/export_video_t_video_info_extend_hour/ds=YYYYMMDDHH
    * @param output_path 目前暂定GuidData/jmtags/cid_vid_list
    *
    */
  def get_vid_list(spark: SparkSession, vid_info_path: String, output_path: String) : Unit = {
    import spark.implicits._
    println("begin to process vid_info, source path: " + vid_info_path)
    val vid_info = spark.sparkContext.textFile(vid_info_path)
      .map(_.split("\t", -1))
      .filter(_.length >107)
      .filter(_(59) == "4")
//      .filter(_(3).contains("正片"))
        .filter(arr=>{arr(73).nonEmpty && !arr(73).contains("#") && arr(74).nonEmpty && !arr(74).contains("#")})

      .map(line => {
        val vid = line(1)

        val first_recom_id = line(73).toInt
        val second_recom_id = line(74).toInt
        (vid, first_recom_id, second_recom_id)
      }).toDF("vid", "first_recom_id", "second_recom_id").cache()

    vid_info.repartition(REPARTITION_NUM).write.mode("overwrite").parquet(output_path)
    println("write vid_recom_id done, vid num: " + vid_info.count())
    vid_info.unpersist()
  }


  def get_guid_recomm_daily(spark: SparkSession, vid_info_path: String, guid_path_daily: String, output_path: String) : Unit = {
    import spark.implicits._
    println("begin to process guid_vid_cid_data_daily, source path: ")
    println("vid_info: " + vid_info_path)
    println("guid_path_daily: " + guid_path_daily)


    // cid, title_cid, vid, title_vid, current_update_num, vid_num, weight
    val vid_data = spark.read.parquet(vid_info_path).select($"vid", $"first_recom_id",  $"second_recom_id")
    // guid, vid, playduration, duration, sqrtx
    val guid_data = spark.read.parquet(guid_path_daily)
      .select($"guid", $"vid", $"ts", $"percent")
      .filter($"percent" > 0.1).coalesce(400)

    val join_data = guid_data.join(vid_data, "vid")
      .filter($"vid".isNotNull).repartition(REPARTITION_NUM, $"guid")

    join_data.createOrReplaceTempView("temp_db")

    val vid_sql_str = "select guid, vid, first_recom_id, second_recom_id,  percent from temp_db"
    val guid_vid_data = spark.sql(vid_sql_str)
      .groupBy($"guid", $"first_recom_id", $"second_recom_id").agg(sum($"percent") as "percent")
      .cache()

    println("begin to write guid_recom_daily: " + output_path)
    guid_vid_data.write.mode("overwrite").parquet(output_path)
    println("done, guid_recom_daily result path: " + output_path)
  }

  def get_guid_recomm_monthly(spark: SparkSession, guid_vid_path: String, output_path: String): Unit = {
    import spark.implicits._

    val vid_subpaths = Tools.get_last_month_date_str()
      .map(date=>(date, guid_vid_path + "/" + date))
      .filter(kv => {Tools.is_path_exist(spark, kv._2) && Tools.is_flag_exist(spark, kv._2)})

    val today = Tools.get_n_day(0)
    if(vid_subpaths.length == 0) {
      println("error, we have no vid data here!")
      return
    } else {
      printf("[monthly] we have %d days vid data to process.\n", vid_subpaths.length)
    }

    var vid_data_total: DataFrame = null
    for(vid_path_kv <- vid_subpaths) {
      val diff_day = Tools.diff_date(vid_path_kv._1, today)
      val time_weight = if(diff_day == 0) 1 else Math.pow(diff_day.toDouble, -0.35)

      val time_weight_res = {
        if (time_weight > 1) {
          println("warning!!, time_weight upper than 1! value:" + time_weight)
          1
        } else {
          println(s"date [${vid_path_kv._1}] time weight: " + time_weight.formatted("%.4f"))
          time_weight
        }
      }

      val vid_data_daily = spark.read.parquet(vid_path_kv._2)
        .select($"guid", $"first_recom_id", $"second_recom_id",  ($"percent" * time_weight_res) as "percent")

      if(vid_data_total == null) {
        vid_data_total = vid_data_daily
      } else {
        vid_data_total = vid_data_total.union(vid_data_daily)
      }
    }

    val vid_data_res = vid_data_total
      .groupBy($"guid", $"first_recom_id", $"second_recom_id").agg(sum($"percent") as "percent")


    vid_data_res.write.mode("overwrite").parquet(output_path)
    println(s"write vid_data_monthly to path $output_path done.")
  }

  def put_guid_cat_to_redis(spark: SparkSession, path : String, flags: Set[String]): Unit = {
    if(flags.contains("put") || flags.contains("delete")) {
      import spark.implicits._
      val ip = "100.107.17.228" //1159
      val port = 9100
      //val limit_num = 1000
      val bzid = "uc"
      val prefix = "cat"
      val tag_type: Int = -1
      val data = spark.read.parquet(path)
        .groupBy($"guid").agg(collect_list(struct($"first_recom_id", $"second_recom_id", $"percent")) as "cat_info")
        .map(line => {
        val guid = line.getString(0)
        val value_weight = line.getAs[Seq[Row]](1).map(v => (s"${v.getInt(0)}.${v.getInt(1)}", v.getDouble(2)))
          .sortBy(_._2)(Ordering[Double].reverse).take(30)
        KeyValueWeight(guid, value_weight)
      })
        //  .filter(d => Tools.boss_guid.contains(d.key))
        .cache


      val test_redis_pool = new TestRedisPool(ip, port, 40000)
      val broadcast_redis_pool = spark.sparkContext.broadcast(test_redis_pool)
      if (flags.contains("delete")) {
        Tools.delete_to_redis(data, broadcast_redis_pool, bzid, prefix, tag_type /*, limit_num = 1000 */)
      }
      if (flags.contains("put")) {
        Tools.put_to_redis(data, broadcast_redis_pool, bzid, prefix, tag_type /*, limit_num = 1000 */)
      }
      Tools.stat(spark, data, "CAT") // 统计数据
      println("-----------------[put_guid_vid_to_redis] to redis done, number: " + data.count)
    }
  }



  def main(args: Array[String]) {

    val spark = SparkSession
      .builder
      .appName(this.getClass.getName.split("\\$").last)
      .getOrCreate()



    val vid_info_path = args(0)
    val play_percent_path = args(1)
    val vid_recomm_path = args(2)
    val vid_output_path_daily = args(3)
    val vid_output_monthly = args(4)
    val date = args(5)
    val control_flags = args(6).split(Defines.FLAGS_SPLIT_STR, -1).toSet
    println("------------------[begin]-----------------")
    println("control flags is: " + control_flags.mkString("""||"""))

    if(control_flags.contains("vid_recomm_list")) {
      get_vid_list(
        spark,
        vid_info_path = vid_info_path,
        output_path = vid_recomm_path + "/" + date)
    }

    if(control_flags.contains("vid_recomm_daily")) {
      get_guid_recomm_daily(
        spark,
        vid_info_path = vid_recomm_path + "/" + date,
        guid_path_daily = play_percent_path + "/" + date,
        output_path = vid_output_path_daily + "/" + date
      )
    }

    if(control_flags.contains("vid_recomm_monthly")) {
      get_guid_recomm_monthly(
        spark,
        guid_vid_path = vid_output_path_daily,
        output_path = vid_output_monthly
      )
    }

    put_guid_cat_to_redis(spark, vid_output_monthly, control_flags)



    println("------------------[done]-----------------")
  }
}
