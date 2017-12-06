package DataAdapter

import BasicData.VidTagsWeightV3.TagWeightInfo
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
    16 -> "vtagids",
    60 -> "omMediaId",
    61 -> "qtagids",
    62 -> "vtagids_weight",
    63 -> "qtagids_weight"
  ).sortBy(_._1)(Ordering[Int])



  /**
  *   useful col: 所有标签(vtag和qtag) 均依权重从高到低排序
                1 vid str
                10 duration 视频时长 (sec)
                12 vtime 上架时间 (ts sec)
                39 omFirstCat om一级类目
                40 omSecondCat om二级类目
                16 vtagIds vtag标签 英文半角逗号分割
                60 omMediaId om账号id
                61 Qtags  新添加的qtags
                62 vtagids_weight  带权重的vtag
                63 qtagids_weight  带权重的qtag

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
                    vid_vtag_path: String,  // vtags
                    vid_qtag_path: String,
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
    println("filter lastest vid vtag pool from  [" + real_filter_vid_path + "]")
    val filter_vid = spark.read.json(real_filter_vid_path).coalesce(REPARTITION_NUM).toDF("vid")

    println("-------------begin to get vtags---------------")
    val vid_vtag_subpath = Tools.get_latest_subpath(spark, vid_vtag_path)
    println(s"get last idf path from [$vid_vtag_subpath]")

    // 要一列vtag不带weight,要一列vtag带weight
    val vid_vtags_info = spark.read.parquet(vid_vtag_subpath).select("vid", "duration", "tag_info")
      .map(row => {
        val vid = row.getAs[String]("vid")
        val duration = row.getAs[Int]("duration")
        //tag tagid weight, tag_type
        val tag_info = row.getAs[Seq[Row]]("tag_info").map(tg => TagWeightInfo(tg.getString(0), tg.getLong(1), tg.getDouble(2), tg.getString(3)))
        (vid, duration, tag_info)
      }).cache()
      // _1:vid, _2: duration, _3: TagInfo
    val vid_vtags_weight = vid_vtags_info
      .map(line => {
        val vid = line._1
        val vtagids_weight = line._3
            .sortBy(_.weight)(Ordering[Double].reverse)
          .map(tag_weight => tag_weight.tagid.toString + ":" + tag_weight.weight.formatted("%.4f")) //_1 tag _2 tag_id _3 weight
          .take(30).distinct
          .mkString(",")
        (vid, vtagids_weight)
      }).toDF("vid_v_weight", "vtagids_weight").cache()   // 字符串形式的ids

    val vid_vtags = vid_vtags_info
      .map(line => {
        val vid = line._1
        val vtagids = line._3
          .sortBy(_.weight)(Ordering[Double].reverse)
          .map(_.tagid.toString) //_1 tag _2 tag_id _3 weight
          .take(30).distinct
          .mkString(",")
        (vid, vtagids)
      }).toDF("vid", "vtagids").cache()   // 字符串形式的ids

    println("-------------begin to get qtags---------------")
    val real_vid_qtag_path = Tools.get_latest_subpath(spark, vid_qtag_path)
    println("filter lastest vid qtag pool from  [" + real_vid_qtag_path + "]")
    // qtag和vtag一样，要一列带weight的，要一列不带weight的
    val vid_qtags_info = spark.read.parquet(real_vid_qtag_path)
      .map(line => {
        val vid = line.getAs[String]("vid")
        val qtags_info = line.getAs[Seq[Row]]("tag_weight")
            .map(row => TagWeightInfo(line.getString(0), row.getLong(1), row.getDouble(2), row.getString(3)))
            .sortBy(_.weight)(Ordering[Double].reverse)
            .take(30)
        (vid, qtags_info)
      }).cache()   // 字符串形式的ids

    val vid_qtags_weight = vid_qtags_info.map(line => {
      val vid = line._1
      val qtagids_weight = line._2.map(row => row.tagid.toString + ":" + row.weight.formatted("%.4f"))
        .mkString(",")
      (vid, qtagids_weight)
    }).toDF("vid_q_weight", "qtagids_weight").cache()

    val vid_qtags = vid_qtags_info.map(line => {
      val vid = line._1
      val qtagids = line._2.map(_.tagid.toString).mkString(",")
      (vid, qtagids)
    }).toDF("vid_q", "qtagids").cache()
    println(s"vid_qtag number: ${vid_qtags.count}, show:")
    vid_qtags.show()


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
        .join(vid_qtags, $"vid" === vid_qtags("vid_q"), "left")
        .join(vid_vtags_weight, $"vid" === vid_vtags_weight("vid_v_weight"), "left")
        .join(vid_qtags_weight, $"vid" === vid_qtags_weight("vid_q_weight"), "left")

      .select($"vid", $"duration", long_to_string(unix_timestamp($"vtime")) as "vtime", $"omFirstCat", $"omSecondCat", $"vtagids", $"omMediaId", $"qtagids", $"vtagids_weight", $"qtagids_weight")

      .map(line=>{
        var s: String = ""
        for(i <- cols_pos_broadcast.value.indices) {
          s += "\t" * col_indice_diff_broadcast.value(i)

          val field_name = cols_pos_broadcast.value(i)._2
          val field_value = line.getAs[String](field_name)
          if(field_value != null && !field_value.isEmpty)
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
    val vid_vtag_path = args(2)
    val vid_qtag_path = args(3)
    val output_path = args(4)
    val col_format_num = args(5).toInt
    val date = args(6)

    get_item_meta(video_info_path = video_info_path,
      filter_vids_path = filter_vids_path,
      vid_vtag_path = vid_vtag_path,
      vid_qtag_path = vid_qtag_path,
      output_path = output_path + "/" + date,
      col_format_num = col_format_num
    )
    println("############################### is Done #########################################")
  }
}
