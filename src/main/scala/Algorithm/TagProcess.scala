package Algorithm

import Algorithm.dataProcess.process_video_info
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

/**
  * Created by baronfeng on 2017/7/14.
  */
case class video_tag_infos(vid : String,
                           map_name : String,
          //                 b_tag: Array[String], // split #  45
                           b_title: String,
                           b_duration: Int,
                           first_recommand: String,
                           second_recommand: String,
         //                  pioneer_tag: Array[String], //split: +
                          tag: Array[String],
                           c_qq: String

                          )

object TagProcess {
  def process_video_info(spark: SparkSession, inputPath:String, outputPath:String) : Int = {
    val db_table_name = "table_video_info"
    val rdd : RDD[video_tag_infos] = spark.sparkContext.textFile(inputPath)
      .map(line => line.split("\t", -1)).filter(_.length != 116)
      // 45:b_tag, 57:b_duration
      .map(arr => video_tag_infos(
                    arr(1),
                    arr(3),
    //                arr(45).split("#", -1).filter(_ != ""),
                    arr(49),
                    arr(57).toInt,
                    arr(75),
                    arr(76),
     //               arr(88).split("\\+", -1).filter(_!=""),
      ((arr(45).split("#", -1).filter(_ != "")) ++ (arr(88).split("\\+", -1).filter(_!=""))).distinct,
                    arr(107)
          )
      )
      .filter(_.b_duration <600)
        .filter(_.tag.length != 0)


    import spark.implicits._
    val video_info = rdd.toDF
    video_info.createTempView(db_table_name)

    video_info.show(1000)

    return 0
  }
  def main(args: Array[String]) {

    /**
      * step1. create SparkSession object
      * 封装了spark sql的执行环境，是spark SQL程序的唯一入口
      */
    //System.setProperty("hadoop.home.dir", "C:\\winutils")
    println("------------------[begin]-----------------")
    val spark = SparkSession
      .builder
      .appName("spark-vid-tag")
      //     .master("local")
      .getOrCreate()


    val inputPath = args(0)
    val outputPath = args(1)

    process_video_info(spark, inputPath, outputPath)
    println("------------------[done]-----------------")
  }
}
