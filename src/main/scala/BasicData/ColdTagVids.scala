package BasicData

import Utils.Tools.KeyValueWeight
import Utils.{Defines, TestRedisPool, Tools}
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import org.apache.spark.sql.functions._

import scala.util.{Success, Try}

/**
  * Created by baronfeng on 2017/11/22.
  * 小时级别落cold数据，数据来源为倒排tag-vids
  */
object ColdTagVids {
  case class vid_ztid_click_exposure(vid: String, ztid: String, click: Int, exposure: Int)
  //判断数据类型，去掉脏数据
  private[this] def verify(str: String, dtype: String):Boolean = {
    var c:Try[Any] = null
    if("double".equals(dtype)) {
      c = Try(str.toDouble)
    } else if("int".equals(dtype)) {
      c = Try(str.toInt)
    }
    val result = c match {
      case Success(_) => true;
      case _ =>  false;
    }
    result
  }

  def get_hot_vids(spark: SparkSession, ctr_source_subpaths: Seq[String], exposure_max: Int) : Dataset[vid_ztid_click_exposure] = {
    import spark.implicits._
    println(s"begin to process ctr data, \n input_path: ${ctr_source_subpaths.mkString(";")}")
    val input_all = spark.read.textFile(ctr_source_subpaths: _*)
      .map(arr => arr.split("\t", -1))
      .filter(_.length >= 6)
      .filter(arr => arr(1) != "" && verify(arr(4), "int") && verify(arr(5), "int"))

      .map(arr => vid_ztid_click_exposure(arr(1), arr(3), arr(5).toInt, arr(4).toInt))
      .filter(_.exposure > exposure_max)
//      .filter(d => d.ztid == "120121" || d.ztid == "110540")
    input_all
  }

  def get_checkup_vids(spark: SparkSession, video_info_path: String, expire_day: Long) : DataFrame = {
    import spark.implicits._
    println(s"begin to get video info filter with checkup time, path: $video_info_path")
    val vid_time = spark.read.textFile(video_info_path)
      .map(line => line.split("\t", -1))
      .filter(_.length >= 130)
      .filter(line => line(59) == "4") // 在线上
      .filter(line => line(1).length == 11) //vid长度为11
      .map(line => (line(1), line(60)))
      .toDF("vid", "time")
      .select($"vid", unix_timestamp($"time") as "ts")


    val now: Long = System.currentTimeMillis() / 1000
    val expire_sec = expire_day * 24 * 60 * 60

    val fresh_vid_time = vid_time.filter($"ts" > now - expire_sec).cache
    println(s"get fresh vids, expire_day: $expire_day,  number: ${fresh_vid_time.count()}")
    fresh_vid_time
  }

  def get_tag_vids_cold(spark: SparkSession,
                        tag_vids_path: String,
                        hot_vids: Dataset[vid_ztid_click_exposure],  // delete them
                        checkup_vids: DataFrame,   // vid, timestamp   delete them
                        tag_vid_cold_output_path: String) : Unit = {
    import spark.implicits._
    val vids = hot_vids.map(_.vid).toDF("vid_t").cache()
    println(s"get tag_vid_source from path $tag_vids_path")
    val tag_vids_source = spark.read.parquet(tag_vids_path)
      .flatMap(line => {
        val tag = line.getAs[String]("tag")
        val tagid = line.getAs[Long]("tagid")
        val tag_type = line.getAs[String]("tag_type")
        val vid_weight = line.getAs[Seq[Row]]("vid_weight").map(r => (r.getString(0), r.getDouble(1)))

        vid_weight.map(vw => (tag, tagid, tag_type, vw._1, vw._2))

      }).toDF("tag", "tagid", "tag_type", "vid", "weight").cache

    val tag_vid_cold = tag_vids_source.join(vids, $"vid" === vids("vid_t"), "leftanti")
      .join(checkup_vids, "vid")
      .cache()
    println(s"total cold vid number: ${tag_vid_cold.select($"vid").distinct().count}")
    tag_vid_cold.printSchema()
    tag_vid_cold.show

    val tag_vids_cold = tag_vid_cold.groupBy($"tag", $"tagid", $"tag_type").agg(collect_list(struct($"vid", $"weight")) as "vid_weight")
      .map(line => {
        val tag = line.getAs[String]("tag")
        val tagid = line.getAs[Long]("tagid")
        val tag_type = line.getAs[String]("tag_type")
        val vid_weight = line.getAs[Seq[Row]]("vid_weight").map(r => (r.getString(0), r.getDouble(1)))
          .sortBy(_._2)(Ordering[Double].reverse)
        (tag, tagid, tag_type, vid_weight)
      }).filter(_._4.nonEmpty).toDF("tag", "tagid", "tag_type", "vid_weight").cache
    tag_vids_cold.printSchema()
    tag_vids_cold.show
    println(s"cold tag_vid data write to path: $tag_vid_cold_output_path, number: ${tag_vids_cold.count()}")
    tag_vids_cold.write.mode("overwrite").parquet(tag_vid_cold_output_path)

  }

  def put_tag_vid_to_redis(spark: SparkSession,
                           path : String,
                           control_flags:Set[String],
                           ip: String = "100.107.18.32", // zkname sz1163.short_video_tg2vd.redis.com
                           port: Int = 9000,
                           data_type: String = "v0"): Unit = {
    if(control_flags.contains("delete") || control_flags.contains("put")) {
      println("put cold_tag_vid to redis")
      import spark.implicits._

      //val limit_num = 1000
      val bzid = "sengine"
      val prefix = s"${data_type}_sv_tg2vd_cold"
      val tag_type: Int = -1 // 不需要字段中加入tag_type,就设置成-1

      //"tag": String, "tagid": Long, "tag_type": String, "vid_weight": Seq[(String, Doumble)]
      val data = spark.read.parquet(path)
          .map(line => {
            val tagid = line.getAs[Long]("tagid").toString
            val vid_weight = line.getAs[Seq[Row]]("vid_weight")
              .map(r => (r.getString(0), r.getDouble(1)))
            KeyValueWeight(tagid, value_weight = vid_weight)
          })
        .cache()
      println(s"put tag_vid to redis, data_type: $data_type, number： ${data.count()}")

      val test_redis_pool = new TestRedisPool(ip, port, 40000)
      val broadcast_redis_pool = spark.sparkContext.broadcast(test_redis_pool)
      if (control_flags.contains("delete") && control_flags.contains("put")){
        Tools.delete_and_put_to_redis(data, broadcast_redis_pool, bzid, prefix, tag_type, expire_time = 60 * 60 * 24 * 1)
      } else if (control_flags.contains("delete")) {
        Tools.delete_to_redis(data, broadcast_redis_pool, bzid, prefix, tag_type /*, limit_num = 1000 */)
      } else if (control_flags.contains("put")) {
        Tools.put_to_redis(data, broadcast_redis_pool, bzid, prefix, tag_type /*, limit_num = 1000 */)
      }
      println(s"put tag_vid_cold to redis done, number: ${data.count}")
      Tools.stat(spark, data, s"Cold_Tag_$data_type")
    }
  }


  def main(args: Array[String]) {
    val spark = SparkSession
      .builder
      .appName(this.getClass.getName.split("\\$").last)
      .getOrCreate()


    val tag_vids_path = Tools.get_latest_subpath(spark, args(0))
    val ctr_source_path = args(1)
    val video_info_path = Tools.get_latest_subpath(spark, args(2))
    val time = args(3)
    val exposure_max = args(4).toInt
    val expire_day = args(5).toLong
    val tag_vid_output_path = args(6) + "/" + time
    val control_flags = args(7).split(Defines.FLAGS_SPLIT_STR, -1).toSet
    val ip = args(8)
    val port = args(9).toInt

    println("------------------[begin]-----------------")
    println(s"control flags: ${control_flags.mkString("||")}")

    // 精确的取小时级别数据
    val ctr_source_subpaths = Tools.get_last_days(8, with_today = true)
      .flatMap(day => {
        (0 until 24).map(i => day + i.formatted("%02d"))
      }) // 找到小时级别的数据
      .filter(time_hour => Tools.is_path_exist(spark, ctr_source_path + "/" + time_hour))
      .sortBy(_.toInt)(Ordering[Int].reverse)
      .take(24 * 7)
      .map(time_hour => ctr_source_path + "/" + time_hour)
    // hot vids to filter
    val hot_vid_dataset = get_hot_vids(spark, ctr_source_subpaths = ctr_source_subpaths, exposure_max = exposure_max)

    // new vids to filter
    val checkup_vids = get_checkup_vids(spark, video_info_path = video_info_path, expire_day = expire_day)
    get_tag_vids_cold(spark,
      tag_vids_path = tag_vids_path,
      hot_vids = hot_vid_dataset,
      checkup_vids = checkup_vids,
      tag_vid_cold_output_path = tag_vid_output_path)

    println("------------------begin to write to redis-----------------")
    println(s"write to redis ip = $ip, port = $port")
    put_tag_vid_to_redis(spark, tag_vid_output_path, control_flags, ip = ip, port = port)

    println("------------------[done]-----------------")
  }

}
