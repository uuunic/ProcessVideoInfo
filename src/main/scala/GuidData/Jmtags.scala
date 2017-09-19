package GuidData

import Utils.{TestRedisPool, Tools}
import Utils.Tools.KeyValueWeight
import org.apache.spark.sql.{Row, SparkSession}

/**
  * Created by baronfeng on 2017/9/18.
  */
object Jmtags {
  /**
    * 从cid_info和vid_info那里取得各项数据，并联立成表
    *
    * @param cid_info_path /data/stage/outface/omg/export_video_t_cover_info_extend_hour/ds=YYYYMMDD23  注意只有23点的时候落地了
    * @param vid_info_path /data/stage/outface/omg/tdw/export_video_t_video_info_extend_hour/ds=YYYYMMDDHH
    * @param output_path 目前暂定GuidData/jmtags/cid_vid_list
    * @return DataFrame, 应该包含guid, vid, ts, playduration, duration, percent  duration如果为0，percent默认为20%，也就是0.2
    */
  def get_cid_vid_list(spark: SparkSession, cid_info_path: String, vid_info_path: String, output_path: String) : Unit = {
    import spark.implicits._
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
      .filter(arr => arr(3).contains("正片"))  // 只找正片cid
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

    println("begin to write cid_vid_list to output_path: " + output_path)
    val cid_vid_data = cid_info.join(vid_info, $"vid" === vid_info("vid_t"), "left").filter($"vid_t".isNotNull).drop($"vid_t")

    cid_vid_data.createOrReplaceTempView("temp_db")
    val cid_vid_current_num = spark.sql("select cid, max(vid_num) as current_update_num from temp_db group by cid").toDF("cid_temp", "current_update_num")
    val result_data = cid_vid_data.join(cid_vid_current_num, cid_vid_data("cid") === cid_vid_current_num("cid_temp"), "left")
    result_data.createOrReplaceTempView("result_db")

    // vid的weight在这里计算
    val ret_data = spark.sql("select cid, title_cid, vid, title_vid, current_update_num, vid_num, weight(vid_num, current_update_num) as weight, unix_timestamp(checkup_time) as update_time from result_db")
    ret_data.write.mode("overwrite").parquet(output_path)
    println("write cid_vid_list done.")
  }


  def get_guid_vid_cid_data(spark: SparkSession, cid_vid_path: String, guid_path_daily: String, output_path: String) : String = {
    import spark.implicits._

    spark.sqlContext.udf.register("vid_data", (vid: String, vid_weight: Double) => {
      (vid, vid_weight)
    })

    spark.sqlContext.udf.register("vid_data_sort", (data: Seq[Row]) => {
      val d = data.map(line => (line.getString(0), line.getDouble(1)))
      d.sortBy(_._2)(Ordering[Double].reverse).take(200)
    })


    spark.sqlContext.udf.register("cid_tuple2", (cid: String, cid_weight: Double) => {
      (cid, cid_weight)
    })


    // cid, title_cid, vid, title_vid, current_update_num, vid_num, weight
    val cid_vid_data = spark.read.parquet(cid_vid_path).select($"cid", $"vid",  $"weight")
    // guid, vid, playduration, duration, sqrtx
    val guid_data = spark.read.parquet(guid_path_daily).select($"guid", $"vid" as "vid_t", $"ts", $"percent").coalesce(400)
    val join_data = guid_data.join(cid_vid_data, $"vid_t" === cid_vid_data("vid"), "left")
      .filter($"vid".isNotNull).repartition(400)

    join_data.createOrReplaceTempView("temp_db")

    val now = spark.sparkContext.broadcast(System.currentTimeMillis / 1000)
    val vid_sql_str = "select guid, ts, vid, cid,  weight, percent from temp_db where percent > 0.1"
    val guid_vid_cid_data = spark.sql(vid_sql_str)
      .map(line => {
        val guid = line.getString(0)
        val ts = line.getLong(1)
        val vid = line.getString(2)
        val cid = line.getString(3)
        val vid_weight = line.getDouble(4)
        val percent = line.getDouble(5)

        val now_time = now.value
        val time_weight = Math.pow((now_time - ts).toDouble / (24*60*60), -0.35)
        val time_weight_res = if(time_weight>1) 1 else time_weight

        (guid,  vid, cid, time_weight_res * vid_weight * percent)
      }).toDF("guid", "vid",  "cid", "vid_weight")

    guid_vid_cid_data.createOrReplaceTempView("guid_vid_cid_db")
 //   val guid_vid_data = spark.sql("select guid, vid_data_sort(collect_list(vid_data(vid, vid_weight))) as vid_info from guid_vid_cid_db group by guid")

    val cid_sql_inner_str = "select guid, cid, sum(vid_weight) as cid_weight from guid_vid_cid_db group by guid, cid "
    val cid_sql_outer_str = "select guid, collect_list(cid_tuple2(cid, cid_weight)) as cid_data from ( " + cid_sql_inner_str + " ) t group by guid"
    val guid_cid_data = spark.sql(cid_sql_outer_str).map(line=>{
      val guid = line.getString(0)
      val cid_tuple2 = line.getAs[Seq[Row]](1).map(line => {
        (line.getString(0), line.getDouble(1))
      }).sortBy(_._2)(Ordering[Double].reverse).take(200)
      (guid, cid_tuple2)
    }).toDF

    val vid_result_path = output_path + "/vid_output"
    val cid_result_path = output_path + "/cid_output"
//    println("begin to write vid info to: " + vid_result_path)
//    guid_vid_data.write.mode("overwrite").parquet(vid_result_path)
    println("write vid info done, begin to write cid data to: "+ cid_result_path)
    guid_cid_data.write.mode("overwrite").parquet(cid_result_path)
    println("done, result_path_cid: " + cid_result_path)
    cid_result_path
  }

  def put_guid_vid_to_redis(spark: SparkSession, path : String): Unit = {
    import spark.implicits._
    val ip = "100.107.17.215"
    val port = 9039
    //val limit_num = 1000
    val bzid = "uc"
    val prefix = "G3"
    val tag_type: Int = 2513
    val data = spark.read.parquet(path).map(line=>{
      val guid = line.getString(0)
      val value_weight = line.getAs[Seq[Row]](1).take(100).map(v=>(v.getString(0), v.getDouble(1)))
      KeyValueWeight(guid, value_weight)
    })
    //  .filter(d => Tools.boss_guid.contains(d.key))
      .cache


    val test_redis_pool = new TestRedisPool(ip, port, 40000)
    val broadcast_redis_pool = spark.sparkContext.broadcast(test_redis_pool)
    Tools.put_to_redis(data, broadcast_redis_pool, bzid, prefix, tag_type /*, limit_num = 1000 */)
    println("-----------------[put_guid_vid_to_redis] to redis done, number: " + data.count)

  }

  def put_guid_cid_to_redis(spark: SparkSession, path : String): Unit = {
    import spark.implicits._
    val ip = "100.107.17.215"
    val port = 9039
    //val limit_num = 1000
    val bzid = "uc"
    val prefix = "G3"
    val tag_type: Int = 2512
    val data = spark.read.parquet(path).map(line=>{
      val guid = line.getString(0)
      val value_weight = line.getAs[Seq[Row]](1).take(100).map(v=>(v.getString(0), v.getDouble(1)))
      KeyValueWeight(guid, value_weight)
    })
    //  .filter(d => Tools.boss_guid.contains(d.key))
      .cache()


//    data.collect().foreach(println)
    val test_redis_pool = new TestRedisPool(ip, port, 40000)
    val broadcast_redis_pool = spark.sparkContext.broadcast(test_redis_pool)
    Tools.put_to_redis(data, broadcast_redis_pool, bzid, prefix, tag_type /*, limit_num = 1000 */)
    println("-------------[put_guid_cid_to_redis] to redis done, number: " + data.count)

  }


  def main(args: Array[String]) {

    val spark = SparkSession
      .builder
      .appName("cid_vid_index_wash")
      .getOrCreate()


    val cid_info_path = args(0)
    val vid_info_path = args(1)
    val guid_path = args(2)
    val output_path = args(3)
    println("------------------[begin]-----------------")

    val cid_vid_path = output_path + "/cid_vid_list"
//    get_cid_vid_list(spark, cid_info_path, vid_info_path, cid_vid_path)

    val result_path = output_path
//    get_guid_vid_cid_data(spark, cid_vid_path, guid_path, result_path)
    put_guid_vid_to_redis(spark, output_path + "/vid_output")
 //   put_guid_cid_to_redis(spark, output_path + "/cid_output")

    println("------------------[done]-----------------")
  }
}
