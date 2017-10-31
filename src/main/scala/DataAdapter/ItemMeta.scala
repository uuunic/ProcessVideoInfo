package DataAdapter

import Utils.Tools
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Row, SparkSession}
/**
  * Created by baronfeng on 2017/10/26.
  */
object ItemMeta {
  val REPARTITION_NUM = 400
  val spark: SparkSession = SparkSession.builder()
    .appName {
      this.getClass.getName.split("\\$").last
    }
    //      .master("local")
    .getOrCreate()
  import spark.implicits._


  val cols_pos: Array[(Int, String)] = Array(
    1 -> "vid",
    10 -> "duration",
    12 -> "vtime",
    39 -> "omFirstCat",
    40 -> "omSecondCat",
    16 -> "tagIds",
    60 -> "omMediaId"
  ).sortBy(_._1)(Ordering[Int])



  /**
  *   useful col:
                1 vid str
                10 duration 视频时长 (sec)
                12 vtime 上架时间 (ts sec)
                39 omFirstCat om一级类目
                40 omSecondCat om二级类目
                16 tagIds 先锋标签 英文半角逗号分割
                60 omMediaId om账号id
                61 Qtags  新添加的qtags
    目前需要做的是还原它，并通过过滤池 from @chenxuexu
    omMediaId需要另join一份数据 video_bee_application::t_dw_article_details

    尽量做成小时级别的数据
    TODO: Qtags还没有加
    * */
  val long_to_string = udf((l: Long)=>l.toString)
  case class useful_cols_without_tagid(vid: String,
                                       duration: String,
                                       vtime: String,
                                       omFirstCat: String,
                                       omSecondCat: String,
                                       omMediaId: String)
  def get_item_meta(video_info_path: String,
                    filter_vids_path: String,
                    idf_info_path: String,
                    output_path: String,
                    col_format_num: Int) : Unit = {

    // guid vtags数据格式化

    println("-------------begin to get item meta---------------")

    val real_video_info_path = Tools.get_latest_subpath(spark, video_info_path)
    println("real video_info path: " + real_video_info_path)

    val real_video_info_time = """\d+""".r.findFirstIn(real_video_info_path.replace(video_info_path + "/", "")).getOrElse("")
    if(real_video_info_time == "") {
      println("real_video_info_time is Null, please check real_video_info_path: " + real_video_info_path)
      return
    }

    println(s"get lastest vid_info from  [$video_info_path] time [$real_video_info_time]")
    val video_info = spark.sparkContext.textFile(real_video_info_path)
      .map(line => line.split("\t", -1))
      .filter(_.length > 116)
      .filter(line => line(59) == "4") // 在线上
      .filter(line => line(1).length == 11) //vid长度为11
      .map(line => {
      val vid = line(1)
      val duration = line(57).trim.replaceAll("\t", " ")
      val vtime = line(60).trim
      val omFirstCat = line(73).replaceAll("\t", " ")
      val omSecondCat = line(74).trim.replaceAll("\t", " ")
//      val tagIds = line(89).trim.replaceAll("\t", " ").replaceAll("\\+", ",")
      val omMediaId = "-1"
      useful_cols_without_tagid(vid, duration, vtime, omFirstCat, omSecondCat, /*tagIds,*/ omMediaId)
    }).toDS

      .repartition(REPARTITION_NUM).cache()
    println("-------------begin to get filter vids---------------")
    val real_filter_vid_path = Tools.get_latest_subpath(spark, filter_vids_path)
    println("filter lastest vid pool from  [" + real_filter_vid_path + "]")
    val filter_vid = spark.read.json(real_filter_vid_path).coalesce(REPARTITION_NUM).toDF("vid")

    println("-------------begin to get vtags---------------")
    val real_idf_path = Tools.get_latest_subpath(spark, idf_info_path)
    println(s"get last idf path from [$real_idf_path]")
    val vid_vtags = spark.read.parquet(real_idf_path)
      .map(line => {
        val vid = line.getString(0)
        val tags_id = line.getAs[Seq[Row]](2)
          .map(_.getLong(1)) //_1 tag _2 tag_id _3 weight
          .sortBy(line=>line)(Ordering[Long].reverse)
          .take(30).distinct
          .map(_.toString).mkString(",")
        (vid, tags_id)
      }).toDF("vid", "tagIds")   // 字符串形式的ids



    // 这里的思路是算出每个key之间\t的个数，然后循环拼接出需要的字符串
    val col_indice: Array[Int] = cols_pos.map(_._1)


    val col_indice_diff = new Array[Int](col_indice.length)
    for(i <- col_indice_diff.indices) {
      if (i == 0)
        col_indice_diff(i) = col_indice(i)
      else
        col_indice_diff(i) = col_indice(i) - col_indice(i - 1)
    }
//    println(s"col_indice_diff is ${col_indice_diff.toString}")
    val cols_pos_broadcast = spark.sparkContext.broadcast(cols_pos)
    val col_indice_diff_broadcast = spark.sparkContext.broadcast(col_indice_diff)


    println("-------------begin to join filter vids and vtags---------------")
    val data_res = video_info.join(filter_vid, "vid")   // vtime必须转换成时间戳
        .join(vid_vtags, "vid")
      .select($"vid", $"duration", long_to_string(unix_timestamp($"vtime")) as "vtime", $"omFirstCat", $"omSecondCat", $"tagIds", $"omMediaId")

      .map(line=>{
        var s: String = ""
        for(i <- cols_pos_broadcast.value.indices) {
          s += "\t" * col_indice_diff_broadcast.value(i)

          val field_name = cols_pos_broadcast.value(i)._2
          val field_value = line.getAs[String](field_name)
          s += field_value
        }
        s += "\t" * (col_format_num - cols_pos_broadcast.value.last._1 - 1)  // 假如有61行，最后一个是line(60)，这时候后面是不用\t的
        "-1" + s   // 要求-1开头 否则会被trim掉
      }).cache

    val real_output_path = output_path
    println(s"s-------------begin to write interests to path: $real_output_path---------------")
    data_res.toDF("value").write.mode("overwrite").text(real_output_path)
    println(s"-------------write interests done, number: ${data_res.count()}---------------")
  }

  def main(args: Array[String]): Unit = {


    val video_info_path = args(0)   // 父目录
    val filter_vids_path = args(1)  // 父目录
    val idf_path = args(2)
    val output_path = args(3)
    val col_format_num = args(4).toInt
    val date = args(5)

    get_item_meta(video_info_path = video_info_path,
      filter_vids_path = filter_vids_path,
      idf_info_path = idf_path,
      output_path = output_path + "/" + date,
      col_format_num = col_format_num
    )
    println("############################### is Done #########################################")
  }
}
