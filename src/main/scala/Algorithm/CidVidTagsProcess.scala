package Algorithm


import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

import scala.collection.mutable.ArrayBuffer

/**
  * Created by baronfeng on 2017/7/21.
  */


case class line_cid_info_usage(cid:String,       //1
                               map_name:String,  //3
                               b_cover_checkup_grade:Int,//61
                               c_vclips:String,  //119
                               video_ids:String, //84
                               b_title:String,   //53
                               b_tag:String)     //49

object CidVidTagsProcess {
  def process_cid_info(spark: SparkSession, input_path: String, output_path: String): Int = {
    val db_table_name = "table_cid_info"
    import spark.implicits._
    val rdd= spark.sparkContext.textFile(input_path)
      .map(line => line.split("\t", -1))
      .filter(_.length >= 136)
      .filter(arr => arr(61) == "0" || arr(61) == "4") //_(61)是b_cover_checkup_grade 要未上架的和在线的
      .filter(arr => arr(119) != "" && arr(84) != "")

      .flatMap(arr => {
        val long_vids: Array[String] = arr(84).split("#")
        long_vids.map(vid =>
          line_cid_info_usage(
            arr(1),
            arr(3),
            arr(61).toInt,
            arr(119),
            vid,
            arr(53),
            arr(49)
          )
        )

      })
      .sortBy(_.cid)

    val df = rdd.toDF
    df.show(100)
    rdd.take(100).foreach(println)
    return 0
  }

  def main(args: Array[String]) {

    /**
      * step1. create SparkSession object
      * 封装了spark sql的执行环境，是spark SQL程序的唯一入口
      */


    val spark = SparkSession
      .builder
      .appName("spark-vid-tag")
           .master("local")
      .getOrCreate()


    //val inputPath = args(0)
    //val outputPath = args(1)
    val inputPath = "data/cover_info.txt"
    val outputPath = "data/output"
    System.setProperty("hadoop.home.dir", "C:\\winutils")

    println("------------------[begin]-----------------")

    process_cid_info(spark, inputPath, outputPath)
    println("------------------[done]-----------------")
  }

}

