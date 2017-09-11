package Test

import org.apache.spark.SparkException
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.unsafe.hash.Murmur3_x86_32.{hashInt, hashLong, hashUnsafeBytes}
import org.apache.spark.unsafe.types.UTF8String
import redis.clients.jedis.Jedis

/**
  * Created by baronfeng on 2017/9/4.
  */
object WriteVidTagsRedis {
  def VID_TAG_DIM = Math.pow(2, 24).toInt  // tf-idf dim

  case class vid_idf_line(vid: String, tags:Array[Long], features_index: Array[Int], features_data: Array[Double], features_size: Int)

  def hash_tf_idf_no_abs(term: Any, mod_num: Int = Math.pow(2, 24).toInt, seed: Int = 42) : Int = {
    term match {

      case b: Boolean => hashInt(if (b) 1 else 0, seed)
      case b: Byte => hashInt(b, seed)
      case s: Short => hashInt(s, seed)
      case i: Int => hashInt(i, seed)
      case l: Long => hashLong(l, seed)
      case f: Float => hashInt(java.lang.Float.floatToIntBits(f), seed)
      case d: Double => hashLong(java.lang.Double.doubleToLongBits(d), seed)
      case s: String => {
        val utf8 = UTF8String.fromString(s)
        hashUnsafeBytes(utf8.getBaseObject, utf8.getBaseOffset, utf8.numBytes(), seed)
      }
      case _ => throw new SparkException("HashingTF with murmur3 algorithm does not " +
        s"support type ${term.getClass.getCanonicalName} of input data.")
    }
  }



  // spark tf-idf hash, use as base hash function
  // seed: 42 is the spark default seed
  // mod_num: Math.pow(2, 24).toInt is my feature space
  // return: hash value as Int (32bit)
  def hash(term: Any, mod_num: Int = Math.pow(2, 24).toInt, seed: Int = 42) : Int = {
    val hashval = hash_tf_idf_no_abs(term, seed)
    ((hashval % mod_num) + mod_num) % mod_num
  }

  // input_path: /user/baronfeng/baronfeng_video/output/tag_recom_ver_2/vid_idf
  // ip: user_define
  // port :user_define
  def put_to_redis(spark: SparkSession, input_path:String, ip: String, port:Int, redis_expire:Int = 40000 /*ms*/) : Long = {
    import spark.implicits._
    val rdd = spark.read.parquet(input_path).as[vid_idf_line] //要写入redis的数据，RDD[Map[String,String]]

    var db_num: Long = 0L
    rdd.repartition(1).foreachPartition { iter =>
      val redis = new Jedis(ip, port, redis_expire)

      val ppl = redis.pipelined() //使用pipeline 更高效的批处理

      iter.foreach(line => {
        val ret_str = new StringBuffer()
        val key = line.vid

        //找出tag对应的weight，并一一搞出来。tag未id化
        for(tag<-line.tags.distinct){
          val hash_value = hash(tag)
          val index = line.features_index.indexOf(hash_value)
          if(index == -1) { // illegal value, drop it.
            //println("tag: " + tag + ", hash_value: " + hash_value + ", index: " + index)
          } else {
            val weight = line.features_data(index)    // 得到对应的weight
            ret_str.append(tag + "#" + weight.formatted("%.3f"))  // 截断下double
          }
        }
        val ret = ppl.set(key, ret_str.toString)
        ppl.sync()
      })
      db_num = if(db_num > redis.dbSize()) db_num else redis.dbSize()
      redis.close()
    }
    db_num
  }


  def main(args: Array[String]) {
    val spark = SparkSession
      .builder
      .appName("vid-tag-redis-put")
      .getOrCreate()
    put_to_redis(spark, "/user/baronfeng/baronfeng_video/output/tag_recom_ver_2/vid_idf", "IP", 1024)
  }

}
