package Algorithm


import com.huaban.analysis.jieba.SegToken
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.linalg.distributed.{CoordinateMatrix, RowMatrix}
import org.apache.spark.mllib.stat.MultivariateStatisticalSummary
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

case class cid_clip(cid:String, clip:String)
case class cid_vid(cid:String, vid:String)

case class video_tag_infos(vid:String, tags: Array[String], tag_size:Int)
object CidVidTagsProcess {
  def process_cid_info(spark: SparkSession, input_path: String, output_path: String, vid_input_path: String): Int = {
    val db_table_name = "table_cid_info"
    import spark.implicits._
    val rdd_usage : RDD[line_cid_info_usage] = spark.sparkContext.textFile(input_path)
      .map(line => line.split("\t", -1))
      .filter(_.length >= 136)
      .filter(arr => arr(61) == "0" || arr(61) == "4") //_(61)是b_cover_checkup_grade 要未上架的和在线的
      .filter(arr => arr(119) != "" && arr(84) != "")  //119为碎视频列表，84为长视频列表，我这里一并做一个过滤
      .map(arr => {
        line_cid_info_usage(
          arr(1),
          arr(3),
          arr(61).toInt,
          arr(119),
          arr(84),
          arr(53),
          arr(49)
        )
      })
    val df_usage = rdd_usage.toDF
    df_usage.createOrReplaceTempView("temp_db")
    println("show cid info")
    df_usage.show(10)
    df_usage.sqlContext.sql("select cid, c_vclips from temp_db").write.mode("overwrite").json(output_path + "/output_json")
    val cid_clips = df_usage.select("cid", "c_vclips").flatMap(line => {
      val cid = line.get(0).asInstanceOf[String]
      val clips = line.get(1).asInstanceOf[String].split("#").filter(_ != "")
      clips.map(clip => cid_clip(cid, clip))
    })

    val cid_vids = df_usage.select($"cid", $"video_ids").flatMap(line =>{
      val cid = line.get(0).asInstanceOf[String]
      val videos = line.get(1).asInstanceOf[String].split("#").filter(_ != "")
      videos.map(video=> cid_vid(cid, video))
    })
    println("cid number: " + df_usage.count())
    println("cid clips: " + cid_clips.count())
    cid_clips.show(10)
    println("cid vids: " + cid_vids.count())
    println("show cid vids data")
    cid_vids.show(10)

    val cid_video_union = cid_vids.union(cid_clips.map(line => cid_vid(line.cid, line.clip)))
    println("show cid video union data, number: " + cid_video_union.count())
    cid_video_union.show(10)


    val rdd_video_info : RDD[video_tag_infos] = spark.sparkContext.textFile(vid_input_path)
      .map(line => line.split("\t", -1)).filter(_.length >107).filter(_(59) == "4")
      //.filter(_(57).toInt < 600)
      // 45:b_tag, 57:b_duration
      .map(arr => {
      val c_qq = arr(107)
      val tags: ArrayBuffer[String] = new ArrayBuffer[String]
      tags ++= arr(45).split("#", -1)
      tags ++= arr(88).split("\\+", -1)
      tags += arr(107)
      tags += arr(75)
      tags += arr(76)
      val tags_array = tags.distinct.toArray.sorted.filter(!_.equals(("")))
      video_tag_infos(
        arr(1),

        tags_array,
        tags_array.length

      )
    })
      .filter(line =>
        line.tag_size > 1
      )
    val df_video_info = rdd_video_info.toDF
    println("show video_info data")
    df_video_info.show(10)

    val join_data = cid_video_union.join(df_video_info, cid_video_union("vid") === df_video_info("vid"), "left_outer")
    println("show cid|vid|tags join data")
    join_data.show(10)

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


//    val inputPath = args(0)
//    val outputPath = args(1)
 //   val vid_input_path = args(2)
    val inputPath = "data/cover_info.txt"
    val outputPath = "data/output"
    System.setProperty("hadoop.home.dir", "C:\\winutils")

    println("------------------[begin]-----------------")
  //  val vid_idf_path: String = TagProcess.get_tf_idf(spark, vid_input_path, outputPath)
    val vid_input_path = "data/test/txt"
  //  process_cid_info(spark, inputPath, outputPath, vid_input_path)
    import com.huaban.analysis.jieba.JiebaSegmenter
    import com.huaban.analysis.jieba.JiebaSegmenter.SegMode
    val segmenter = new JiebaSegmenter

    val sentences = Array[String]("我的.。；‘，《前半生》_》38", "极速之巅(英语版)", "九州天空城_01",
      "我们的少年时代_37","鬼吹灯之黄皮子坟")
    for (sentence <- sentences) {
      val regex_str = """_\d+$""".r
      if (regex_str.findFirstMatchIn(sentence) != None)
        println("match: " + sentence.split("_", -1)(0))
 //     System.out.println(segmenter.process(sentence, SegMode.INDEX).toString)
      val data = segmenter.process(sentence, SegMode.SEARCH).toArray.map(line => line.asInstanceOf[SegToken].word).filter(!_.matches("\\pP")).filter(!_.equals(""))
      data.foreach(line => println("line:" + line + " size:" + line.size))
      
    }

    println("------------------[done]-----------------")
  }

}

