package GuidData

import Utils.Tools.KeyValueWeight
import Utils.{TestRedisPool, Tools}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

/**
  * Created by baronfeng on 2017/9/18.
  */
object Jmtags {
  val REPARTITION_NUM = 400

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


  def get_guid_vid_cid_data_daily(spark: SparkSession, cid_vid_path: String, guid_path_daily: String, vid_result_path: String, cid_result_path: String) : Unit = {
    import spark.implicits._
    println("begin to process guid_vid_cid_data_daily, source path: ")
    println("cid_vid_path: " + cid_vid_path)
    println("guid_path_daily: " + guid_path_daily)
    println("vid_result_path" + vid_result_path)
    println("cid_result_path" + cid_result_path)

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
      .filter($"vid".isNotNull).coalesce(400)

    join_data.createOrReplaceTempView("temp_db")

   // val now = spark.sparkContext.broadcast(System.currentTimeMillis / 1000)
    val vid_sql_str = "select guid, ts, vid, cid,  weight, percent from temp_db where percent > 0.1"
    val guid_vid_cid_data = spark.sql(vid_sql_str)
      .map(line => {
        val guid = line.getString(0)
        val ts = line.getLong(1)
        val vid = line.getString(2)
        val cid = line.getString(3)
        val vid_weight = line.getDouble(4)
        val percent = line.getDouble(5)

     //   val now_time = now.value
     //   val time_weight = Math.pow((now_time - ts).toDouble / (24*60*60), -0.35)
     //   val time_weight_res = if(time_weight>1) 1 else time_weight

        (guid,  vid, cid, vid_weight * percent)
      }).toDF("guid", "vid",  "cid", "vid_weight").cache()

    guid_vid_cid_data.createOrReplaceTempView("guid_vid_cid_db")
    val vid_core_sql_str = "select guid, vid, vid_weight from guid_vid_cid_db DISTRIBUTE BY guid, vid SORT BY guid, vid"
    val guid_vid_data = spark.sql("select guid, vid_data_sort(collect_list(vid_data(vid, vid_weight))) as vid_info from ( " + vid_core_sql_str + " ) group by guid")

    val cid_core_sql_str = "select guid, cid, vid_weight from guid_vid_cid_db DISTRIBUTE BY guid, cid SORT BY guid, cid"
    val cid_sql_inner_str = "select guid, cid, sum(vid_weight) as cid_weight from ( " + cid_core_sql_str +" )guid_vid_cid_db group by guid, cid "
    val cid_sql_outer_str = "select guid, collect_list(cid_tuple2(cid, cid_weight)) as cid_data from ( " + cid_sql_inner_str + " ) t group by guid"
    val guid_cid_data = spark.sql(cid_sql_outer_str).map(line=>{
      val guid = line.getString(0)
      val cid_tuple2 = line.getAs[Seq[Row]](1).map(line => {
        (line.getString(0), line.getDouble(1))
      }).sortBy(_._2)(Ordering[Double].reverse).take(200)
      (guid, cid_tuple2)
    }).toDF



    println("begin to write vid_result_path: " + vid_result_path)
    guid_vid_data.write.mode("overwrite").parquet(vid_result_path)
    println("write vid info done, begin to write cid data to: "+ cid_result_path)
    guid_cid_data.write.mode("overwrite").parquet(cid_result_path)
    println("done, cid_result_path: " + cid_result_path)

  }

  def get_guid_cid_monthly(spark: SparkSession, guid_cid_path: String, output_path: String): Unit = {
    import spark.implicits._
    spark.sqlContext.udf.register("flatten", (xs: Seq[Seq[Row]]) => {
      val data = xs.map(line=>
        line.map(arr=>(arr.getString(0), arr.getDouble(1)))
      )
      val ret = data.flatten.groupBy(_._1).map(line=>{
        val vid = line._1
        val weight = line._2.map(_._2).sum
        (vid, weight)
      })
      ret.toSeq.sortBy(_._2)(Ordering[Double].reverse).take(200)
    })

    val cid_subpaths = Tools.get_last_month_date_str()
      .map(date=>(date, guid_cid_path + "/" + date))
      .filter(kv => {Tools.is_path_exist(spark, kv._2) && Tools.is_flag_exist(spark, kv._2)})

    val today = Tools.get_n_day(0)
    if(cid_subpaths.length == 0) {
      println("error, we have no cid data here!")
      return
    } else {
      printf("we have %d days cid data to process.\n", cid_subpaths.length)
    }

    // String, Seq[(String, Double)]
    var cid_data_total: DataFrame = null
    for(cid_path_kv <- cid_subpaths) {
      val diff_day = Tools.diff_date(cid_path_kv._1, today)
      val time_weight = if(diff_day == 0) 1 else Math.pow(diff_day.toDouble, -0.35)

      val time_weight_res = {
        if (time_weight > 1) {
          println("warning!!, time_weight upper than 1! value:" + time_weight)
          1
        } else {
          time_weight
        }
      }

      val cid_data_daily = spark.read.parquet(cid_path_kv._2)
        .map(line=>{
          val guid = line.getString(0)
          val cid_weight = line.getSeq[Row](1).map(kv=>(kv.getString(0), kv.getDouble(1) * time_weight_res))
          (guid, cid_weight)
        }).toDF("guid", "cid_weight")

      if(cid_data_total == null) {
        cid_data_total = cid_data_daily
      } else {
        cid_data_total = cid_data_total.union(cid_data_daily).toDF("guid", "cid_weight")
      }
    }

    cid_data_total.createOrReplaceTempView("cid_data_db")
    val cid_data_sql_inner = "select guid, collect_list(cid_weight) as cid_weight from cid_data_db group by guid"
    val cid_data_sql_outer = "select guid, flatten(cid_weight) as cid_weight from ( " + cid_data_sql_inner + " ) t"
    val cid_data_res = spark.sql(cid_data_sql_outer).coalesce(REPARTITION_NUM)
    cid_data_res.write.mode("overwrite").parquet(output_path)

  }

  def get_guid_vid_monthly(spark: SparkSession, guid_vid_path: String, output_path: String): Unit = {
    import spark.implicits._
    spark.sqlContext.udf.register("flatten", (xs: Seq[Seq[Row]]) => {
      val data = xs.map(line=>
        line.map(arr=>(arr.getString(0), arr.getDouble(1)))
      )
      val ret = data.flatten.groupBy(_._1).map(line=>{
        val vid = line._1
        val weight = line._2.map(_._2).sum
        (vid, weight)
      })
      ret.toSeq.sortBy(_._2)(Ordering[Double].reverse).take(200)
    })

    val vid_subpaths = Tools.get_last_month_date_str()
      .map(date=>(date, guid_vid_path + "/" + date))
      .filter(kv => {Tools.is_path_exist(spark, kv._2) && Tools.is_flag_exist(spark, kv._2)})

    val today = Tools.get_n_day(0)
    if(vid_subpaths.length == 0) {
      println("error, we have no vid data here!")
      return
    } else {
      printf("we have %d days vid data to process.\n", vid_subpaths.length)
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
          time_weight
        }
      }

      val vid_data_daily = spark.read.parquet(vid_path_kv._2)
        .map(line=>{
          val guid = line.getString(0)
          val vid_weight = line.getSeq[Row](1).map(kv=>(kv.getString(0), kv.getDouble(1) * time_weight_res))
          (guid, vid_weight)
        }).toDF("guid", "vid_weight")

      if(vid_data_total == null) {
        vid_data_total = vid_data_daily
      } else {
        vid_data_total = vid_data_total.union(vid_data_daily).toDF("guid", "vid_weight")
      }
    }

    vid_data_total.createOrReplaceTempView("vid_data_db")
    val vid_data_sql_inner = "select guid, collect_list(vid_weight) as vid_weight from vid_data_db group by guid"
    val vid_data_sql_outer = "select guid, flatten(vid_weight) as vid_weight from ( " + vid_data_sql_inner + " ) t"
    val vid_data_res = spark.sql(vid_data_sql_outer).coalesce(REPARTITION_NUM)
    vid_data_res.write.mode("overwrite").parquet(output_path)

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
      .appName("guid-jmtags")
      .getOrCreate()


    val cid_info_path = args(0)
    val vid_info_path = args(1)
    val play_percent_path = args(2)
    val cid_vid_path = args(3)
    val vid_output_path = args(4)
    val cid_output_path = args(5)
    val vid_output_monthly = args(6)
    val cid_output_monthly = args(7)
    val date = args(8)
    println("------------------[begin]-----------------")

//    get_cid_vid_list(
//      spark,
//      cid_info_path = cid_info_path,
//      vid_info_path = vid_info_path,
//      output_path = cid_vid_path + "/" + date)
//
//
//    get_guid_vid_cid_data_daily(
//      spark,
//      cid_vid_path = cid_vid_path + "/" + date,
//      guid_path_daily = play_percent_path + "/" + date,
//      vid_result_path = vid_output_path + "/" + date,
//      cid_result_path = cid_output_path + "/" + date
//    )
/**
    get_guid_vid_monthly(
      spark,
      guid_vid_path = vid_output_path,
      output_path = vid_output_monthly
    )

    get_guid_cid_monthly(
      spark,
      guid_cid_path = cid_output_path,
      output_path = cid_output_monthly
    )
**/
 //      put_guid_vid_to_redis(spark, path = vid_output_monthly)
    put_guid_cid_to_redis(spark, path = cid_output_monthly)

    println("------------------[done]-----------------")
  }
}
