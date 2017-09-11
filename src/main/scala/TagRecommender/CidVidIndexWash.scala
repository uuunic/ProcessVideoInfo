package TagRecommender

import java.text.SimpleDateFormat
import java.util.Date

import org.apache.commons.net.ntp.TimeStamp
import org.apache.spark.sql.{Row, SparkSession}

import scala.collection.mutable.ArrayBuffer

/**
  * Created by baronfeng on 2017/9/8.
  */
object CidVidIndexWash {
  def get_cid_vid_list(spark: SparkSession, cid_info_path: String, vid_info_path: String, output_path: String) : String = {
    import spark.implicits._
    val ret_path = output_path + "/cid_vid_list"
    spark.sqlContext.udf.register("weight", (vid_num: Long, current_update: Long) => {
      if(vid_num == 0 || current_update == 0 || (vid_num > current_update)) {
        0.2
      } else {
        vid_num.toDouble / current_update.toDouble
      }
    })


    println("begin to process cid_info, source path: " + cid_info_path)

    val digit_pattern = """\d+""".r
    val cid_info = spark.sparkContext.textFile(cid_info_path)
      .map(line => line.split("\t", -1))
      .filter(_.length >= 136)

      .filter(arr => arr(61) == "4") //_(61)是b_cover_checkup_grade 要未上架的和在线的
      .filter(arr => arr(84) != "")  //119为碎视频列表，84为长视频列表，我这里一并做一个过滤
        .filter(arr=>arr(3).contains("正片"))  // 只找正片cid
      .flatMap(arr => {
      val cid = arr(1)
      val video_ids = arr(84).split("#", -1)
      val title = arr(53)
      val episode_all = digit_pattern.findFirstIn(arr(10))  // 总集数
      val episode_update = digit_pattern.findFirstIn(arr(109))  // 更新集数
      video_ids.map(vid => (cid, title, episode_all, episode_update, vid))

    }).toDF("cid", "title_cid", "episode_all", "episode_update", "vid")

    println("begin to process vid_info, source path: " + vid_info_path)

    val episode_regex = """_\d+$""".r
    val vid_info = spark.sparkContext.textFile(vid_info_path)
      .map(_.split("\t", -1))
      .filter(_.length >107)
      .filter(_(59) == "4")
        .filter(_(3).contains("正片"))
      .map(line => {
        val vid = line(1)
        val title = line(49)
        val vid_num =  episode_regex.findFirstIn(line(49)).getOrElse("_0").replace("_", "").toLong
        val drm_pay = if(line(55) != "") line(55).toInt else 0
        val pay_1080 = if(line(56) != "") line(56).toInt else 0
        val duration = if(line(57) != "") line(57).toInt else 0
        val create_time = line(61)
        val checkup_time = line(60)
        val covers = line(72)
        (vid, title, vid_num, drm_pay, pay_1080, duration, create_time, checkup_time, covers)
      }).toDF("vid_t", "title_vid", "vid_num", "drm_pay", "pay_1080", "duration", "create_time", "checkup_time", "covers")

    println("begin to write to output_path: " + output_path)
    val cid_vid_data = cid_info.join(vid_info, $"vid" === vid_info("vid_t"), "left").filter($"vid_t".isNotNull).drop($"vid_t")

    cid_vid_data.createOrReplaceTempView("temp_db")
    val cid_vid_current_num = spark.sql("select cid, max(vid_num) as current_update_num from temp_db group by cid").toDF("cid_temp", "current_update_num")
    val result_data = cid_vid_data.join(cid_vid_current_num, cid_vid_data("cid") === cid_vid_current_num("cid_temp"), "left")
    result_data.createOrReplaceTempView("result_db")

    val ret_data = spark.sql("select cid, title_cid, vid, title_vid, current_update_num, vid_num, weight(vid_num, current_update_num) as weight, unix_timestamp(checkup_time) as update_time from result_db")
    ret_data.write.mode("overwrite").parquet(ret_path)
    ret_path
  }


  val sdf:SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
  def date_format(time:Long):String={
    sdf.format(new Date(time * 1000L))
  }

  def get_guid_vid_daily(spark: SparkSession, cid_vid_path: String, guid_path_daily: String, output_path: String) : String = {
    import spark.implicits._
    spark.sqlContext.udf.register("vid_data", (vid: String, vid_title: String, vid_weight: Double,  update_time: Long) => {
      (vid, vid_title, vid_weight, update_time)
    })

    spark.sqlContext.udf.register("vid_data_sort", (data: Seq[Row]) => {
      val d = data.map(line => (line.getString(0), line.getString(1), line.getDouble(2), line.getLong(3)))
      d.sortWith(_._4>_._4)
    })

    spark.sqlContext.udf.register("cid_data_sort", (data: Seq[(String, String, Double, Long)]) => {
      data.sortWith(_._4>_._4)
    })



    // cid, title_cid, vid, title_vid, current_update_num, vid_num, weight
    val cid_vid_data = spark.read.parquet(cid_vid_path)
    // guid, vid, playduration, duration, sqrtx
    val guid_data = spark.read.parquet(guid_path_daily).toDF("guid", "vid_t", "playduration", "duration", "sqrtx")
    val join_data = guid_data.join(cid_vid_data, $"vid_t" === cid_vid_data("vid"), "left")
      .filter($"vid".isNotNull)
    join_data.createOrReplaceTempView("temp_db")

    val vid_sql_str = "select guid, vid, title_vid, cid, title_cid, vid_num, weight, sqrtx as play_percent, update_time from temp_db where sqrtx > 0.1"
    val guid_vid_cid_data = spark.sql(vid_sql_str)
      .map(line => {
        val guid = line.getString(0)
        val vid = line.getString(1)
        val title_vid = line.getString(2)
        val cid = line.getString(3)
        val title_cid = line.getString(4)
        val vid_num = line.getLong(5)
        val vid_weight = line.getDouble(6)
        val sqrtx = line.getDouble(7)
        val update_time = line.getLong(8)
        (guid, vid, title_vid, cid, title_cid, vid_weight.toDouble * sqrtx, update_time)
      }).toDF("guid", "vid", "title_vid", "cid", "title_cid", "vid_weight", "update_time")

    guid_vid_cid_data.createOrReplaceTempView("guid_vid_cid_db")
    val guid_vid_data = spark.sql("select guid, vid_data_sort(collect_list(vid_data(vid, title_vid, vid_weight, update_time))) as vid_info from guid_vid_cid_db group by guid")
    val guid_cid_data = spark.sql("select guid, cid, title_cid, sum(vid_weight) as cid_weight from guid_vid_cid_db group by guid, cid, title_cid order by sum(vid_weight)")

    val vid_result_path = output_path + "/vid_output"
    val cid_result_path = output_path + "/cid_output"
    guid_vid_data.write.mode("overwrite").parquet(vid_result_path)
    guid_cid_data.write.mode("overwrite").parquet(cid_result_path)
    println("done, result_path_vid: " + vid_result_path)
    println("done, result_path_cid: " + cid_result_path)
    cid_result_path
  }
  def main(args: Array[String]) {

    val spark = SparkSession
      .builder
      .appName("cid_vid_index_wash")
      .getOrCreate()


    val cid_info_path = args(0)
    val vid_info_path = args(1)
    val guid_daily_path = args(2)
    val output_path = args(3)
    println("------------------[begin]-----------------")

    val ret_path = output_path + "/cid_vid_list"
    //val ret_path = get_cid_vid_list(spark, cid_info_path, vid_info_path, output_path)
    get_guid_vid_daily(spark, ret_path, guid_daily_path, output_path)


    println("------------------[done]-----------------")
  }
}
