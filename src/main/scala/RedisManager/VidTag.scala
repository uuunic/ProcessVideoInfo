package RedisManager


import java.text.SimpleDateFormat
import java.util.Date

import Utils.TestRedisPool
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.{Dataset, SparkSession}


/**
  * Created by mancoszhang on 2017/9/14.
  */

object VidTag {

  case class KeyValueWeight(key: String, value_weight: Seq[(String, Double)])

  def put_to_redis(input:Dataset[KeyValueWeight],
                   broadcast_redis_pool: Broadcast[TestRedisPool],
                   bzid:String,
                   prefix: String,
                   tag_type: Int,
                   weight_format: String = "%.4f",
                   expire_time: Int = 400000,
                   limit_num: Int = -1) : Unit = {
    val output = if(limit_num == -1) input else input.limit(limit_num)

    //要写入redis的数据，RDD[Map[String,String]]
    output.repartition(20).foreachPartition { iter =>
      //val redis = new Jedis(ip, port, expire_time)
      val redis = broadcast_redis_pool.value.getRedisPool.getResource   // lazy 加载 应该可以用
    val ppl = redis.pipelined() //使用pipeline 更高效的批处理
    var count = 0

      iter.foreach(f => {
        val key = bzid + "_" + prefix + "_" + f.key
        val values_data = f.value_weight.sortWith(_._2>_._2).map(line=>{
          line._1 +  ":" + tag_type.toString + ":" + line._2.formatted(weight_format)
        }).take(100)

//        val keys = Array(key)
        ppl.del(key)
        ppl.rpush(key, values_data: _*)
        ppl.expire(key, 60*60*24*7)   // 这里设置expire_time

        count += 1
        if(count % 30 == 0) {
          ppl.sync()

        }
      })

      ppl.sync()
      redis.close()

    }
  }



  // output_path + "/guid_total_result_30days"
  def put_vid_tags_to_redis(spark: SparkSession, path : String): Unit = {

    println("put vid tags to redis")
    import spark.implicits._
    val ip = "100.107.18.17"   //sz1162.short_video_vd2tg.redis.com
    val port = 9101
    //val limit_num = 1000
    val bzid = "sengine"
    val prefix = "v0_sv_vd2tg"
    val tag_type: Int = 2502
    val data = spark.read.textFile(path).map(line=>{

      val line_data = line.split("\t", -1)
      val guid = line_data(0)
      val cps = line_data(1)
      val value_weight = cps.split(";", -1).map(line=> {

        val line_data = line.split(",", -1)
        val cp = line_data(0)
        val weight = if(line_data.length == 2) line_data(1).toDouble else -1
        (cp, weight)
      }).filter(_._2 != -1)

      KeyValueWeight(guid, value_weight)
    })
    //data.collect().foreach(line=>println(line.key))
    //println("\n\n\n\n")
    val test_redis_pool = new TestRedisPool(ip, port, 40000)
    val broadcast_redis_pool = spark.sparkContext.broadcast(test_redis_pool)

    //统计
    var filepath = "/tmp/short_video_data/mancoszhang.log"
    val inner_type = "V1"
    var now:Date = new Date()
    var dateFormat:SimpleDateFormat = new SimpleDateFormat("yyyyMMddHH")
    var cur_date = dateFormat.format(now)

    put_to_redis(data, broadcast_redis_pool, bzid, prefix, tag_type /*, limit_num = 1000 */)
    println("put to redis done, number: " + data.count)

  }

  def put_tag_vids_to_redis(spark: SparkSession, path : String): Unit = {

    println("put tag vids to redis")
    import spark.implicits._
    val ip = "100.107.18.31"  // sz1163.short_video_tg2vd.redis.com
    val port = 9000
    //val limit_num = 1000
    val bzid = "sengine"
    val prefix = "v0_sv_tg2vd"
    val tag_type: Int = 2511
    val data = spark.read.textFile(path).map(line=>{

      val line_data = line.split("\t", -1)
      val guid = line_data(0)
      val cps = line_data(1)
      val value_weight = cps.split(";", -1).map(line=> {

        val line_data = line.split(",", -1)
        val cp = line_data(0)
        val weight = if(line_data.length == 2) line_data(1).toDouble else -1
        (cp, weight)
      }).filter(_._2 != -1)

      KeyValueWeight(guid, value_weight)
    })
    //data.collect().foreach(line=>println(line.key))
    //println("\n\n\n\n")
    val test_redis_pool = new TestRedisPool(ip, port, 40000)
    val broadcast_redis_pool = spark.sparkContext.broadcast(test_redis_pool)

    put_to_redis(data, broadcast_redis_pool, bzid, prefix, tag_type /*, limit_num = 1000 */)
    println("put to redis done, number: " + data.count)

  }


  def main(args: Array[String]) {

    val spark = SparkSession
      .builder
      .appName("vid_tag_recomm")
      .getOrCreate()


    val vid_tag_path = args(0)
    val tag_vid_path = args(1)
    println("------------------[begin]-----------------")


    put_vid_tags_to_redis(spark, vid_tag_path)
//    put_tag_vids_to_redis(spark, tag_vid_path)


    println("------------------[done]-----------------")
  }

}
