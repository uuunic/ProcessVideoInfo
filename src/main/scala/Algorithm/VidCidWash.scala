package Algorithm

import org.apache.spark.sql.{Row, SparkSession}

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

import org.apache.spark.{SparkContext, SparkConf}
import redis.clients.jedis.Jedis

/**
  * Created by baronfeng on 2017/7/31.
  */
object VidCidWash {

  //把vid对应的cid_list转换为vid_cid的输出
  def wash_vid_cid(spark: SparkSession, input_path: String, output_path: String) : Int = {
    import spark.implicits._
    val input = spark.read.parquet(input_path)
    input.printSchema()
    input.show(100)

    val output = input.as[(String, Seq[(String, Double)])].map(line => {
      val vid = line._1
      val cids = line._2
      var cid = ""
      if(cids.nonEmpty)
        cid = cids.head._1
      (line._1, cid)
    })
    output.show()
    output.write.mode("overwrite").parquet(output_path)
    0
  }

  def put_to_redis(spark: SparkSession, input_path:String) : Unit = {
    import spark.implicits._
    val rdd = spark.read.parquet(input_path).as[(String, String)] //要写入redis的数据，RDD[Map[String,String]]

    rdd.foreachPartition{iter =>
      val redis = new Jedis("10.49.99.138", 9027, 4000)
      val prefix: String = "113_"
      val hashkey = "cover_id"
      val ppl = redis.pipelined()//使用pipeline 更高效的批处理

      iter.foreach{f=>
        ppl.hset(prefix + f._1, hashkey, f._2 )
      }
      ppl.sync()

      redis.close()
    }
  }



  def main(args: Array[String]) {

    /**
      * step1. create SparkSession object
      * 封装了spark sql的执行环境，是spark SQL程序的唯一入口
      */
    //System.setProperty("hadoop.home.dir", "C:\\winutils")

    val spark = SparkSession
      .builder
      .appName("spark-vid-tag")
      //     .master("local")
      .getOrCreate()


    val input_path = args(0)
    val output_path = args(1)
    println("------------------[begin]-----------------")

    //wash_vid_cid(spark, input_path, output_path)
    put_to_redis(spark, output_path)
    val redis = new Jedis("10.49.99.138", 9027, 4000)
    println("the redis size is: " + redis.dbSize())
    redis.close()

    println("------------------[done]-----------------")
  }
}
