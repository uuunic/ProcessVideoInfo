package Test

import Utils.TestRedisPool
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.{Dataset, Row, SparkSession}

object Test {

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
          line._1 + ":" + tag_type.toString + ":" + line._2.formatted(weight_format)

        })
        val keys = Array(key)
        ppl.del(keys: _*)
        ppl.rpush(key, values_data: _*)
        ppl.expire(key, 60*60*24*2)   // 这里设置expire_time


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
  def put_guid_tag_to_redis(spark: SparkSession, path : String): Unit = {
    println("put guid_tag to redis")
    import spark.implicits._
    val ip = "100.107.17.202"   //sz1159.show_video_hb_online.redis.com
    val port = 9039
    //val limit_num = 1000
    val bzid = "uc"
    val prefix = "G1"
    val tag_type: Int = 2501
    val data = spark.read.parquet(path).map(line=>{
      val guid = line.getString(0)
      val value_weight = line.getAs[Seq[Row]](2)
      val value_weight_res = {if(value_weight.length>100) value_weight.take(100) else value_weight}
        .map(v=>(v.getLong(1).toString, v.getDouble(2))).distinct  //_1 tag _2 tag_id _3 weight
      KeyValueWeight(guid, value_weight_res)
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
      .appName("cid_vid_index_wash")
      .getOrCreate()


    val path = args(0)

    println("------------------[begin]-----------------")


    put_guid_tag_to_redis(spark, path)

    println("------------------[done]-----------------")
  }

}