package Algorithm


import org.apache.spark.sql.SparkSession
import redis.clients.jedis.Jedis

/**
  * Created by baronfeng on 2017/8/1.
  */
object vid_append {
  case class
  def append_vid_title(spark: SparkSession, input_path: String, output_path: String) : Int = {
    import spark.implicits._


    0
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




    println("------------------[done]-----------------")
  }
}
