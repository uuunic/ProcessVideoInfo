package Test

import Utils.TestRedisPool
import org.apache.spark.sql.{Dataset, SparkSession}

/**
  * Created by baronfeng on 2017/10/19.
  */
object RedisDemo {
  case class KeyValueWeight(key: String, value_weight: Seq[(String, Double)])

  def put_to_redis(input:Dataset[KeyValueWeight],
                   spark: SparkSession,

                   ip: String,
                   port: Int,

                   weight_format: String = "%.4f",
                   redis_expire_time: Int = 400000) : Unit = {
    println("put to redis.")

    val test_redis_pool = new TestRedisPool(ip, port, redis_expire_time)
    val output = input
    val broadcast_redis_pool = spark.sparkContext.broadcast(test_redis_pool)
    //要写入redis的数据，RDD[Map[String,String]]
    output.repartition(30).foreachPartition { iter =>
      val redis = broadcast_redis_pool.value.getRedisPool.getResource // lazy 加载 应该可以用
    val ppl = redis.pipelined() //使用pipeline 更高效的批处理

      var count = 0
      iter.foreach(f => {
        val key = f.key
        val values_data = f.value_weight.sortWith(_._2 > _._2).map(line => {
          line._1 + ":" + line._2.formatted(weight_format)

        })
        val keys = Array(key)
        //ppl.del(keys: _*)
        ppl.rpush(key, values_data: _*)
        ppl.expire(key, 60 * 60 * 24 * 7)

        count += 1
        if (count % 20 == 0) { // 每写20条同步一次
          ppl.sync()
        }
      })
      ppl.sync()
      redis.close()

    }
  }
}
