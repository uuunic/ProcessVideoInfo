package Pm

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.functions._


/**
  * Created by baronfeng on 2017/9/27.
  */
object VidTagsRank {
  val url = udf((vid: String) =>{
    "https://v.qq.com/x/page/" + vid + ".html"
  })
  def getData(spark: SparkSession, pm_data_path: String, vid_tags_path: String, vid_info_path: String, tag_vids_path: String, real_filter_vid_path: String) : Unit = {
    val pm_data = spark.read.option("header", "true").csv(pm_data_path).select("vid").toDF("vid_source").distinct().cache
    println("pm_data show")
    pm_data.show()



    import spark.implicits._
    val filter_vid = spark.read.json(real_filter_vid_path).toDF("vid_filter").cache
    val vid_tags = spark.read.parquet(vid_tags_path).select("vid", "duration", "tag_info")
      .join(filter_vid, $"vid" === filter_vid("vid_filter"), "inner").drop("vid_filter")
      .cache()
    val vid_info = spark.sparkContext.textFile(vid_info_path)
      .map(line => line.split("\t", -1))
      .filter(_.length > 116)
      .map(arr=>{
        val vid = arr(1)
        val map_name = arr(3)
        val title = arr(49)
        val checkup_time = arr(60)
        (vid, map_name, title, checkup_time)
      }).toDF("vid_info", "map_name", "title", "checkup_time")

      .cache
    val ret_data = pm_data.join(vid_tags, $"vid_source" === vid_tags("vid"), "left").join(vid_info, $"vid_source" === vid_info("vid_info"))
      .select("vid_source", "title", "map_name", "duration", "tag_info", "checkup_time").filter($"title".isNotNull).filter($"duration".isNotNull)
      .map(line=>{
        val vid = line.getString(0)
        val title = line.getString(1)
        val map_name = line.getString(2)
        val duration = line.getInt(3)
        val tag_info = line.getAs[Seq[Row]](4).map(row => {
          (row.getString(0), row.getLong(1), row.getDouble(2))
        }).sortBy(_._3)(Ordering[Double].reverse)
          .map(row => {
           row._1 + "###" + row._2 + "###" + row._3.formatted("%.04f")
        }).mkString(";")
        val checkup_time = line.getString(5)
        val url = "http://v.qq.com/x/page/" + vid +".html"
        (vid, title, map_name, tag_info, checkup_time, duration, url)
      })

    ret_data.toDF("vid", "title", "map_name", "tag_info", "checkup_time", "duration", "url")
      .repartition(1)
      .write.mode("overwrite").option("header", "true").csv("pm_data/VidTagsRank/top_vv_normal_tag")

    val ret_data_2 = vid_tags.sample(false, 0.0001).join(vid_info, $"vid" === vid_info("vid_info"), "left").filter($"vid_info".isNotNull)
      .select("vid", "title", "map_name", "duration", "tag_info", "checkup_time")
      .map(line=>{
        val vid = line.getString(0)
        val title = line.getString(1)
        val map_name = line.getString(2)
        val duration = line.getInt(3)
        val tag_info = line.getSeq[Row](4).map(arr => (arr.getString(0), arr.getLong(1), arr.getDouble(2))).sortBy(_._3)(Ordering[Double].reverse)
          .map(arr => arr._1 + "###" + arr._2 + "###" + arr._3.formatted("%.4f")).mkString(";")
        val checkup_time = line.getString(5)
        (vid, title, map_name, duration, tag_info, checkup_time)
      })
      .toDF("vid", "title", "map_name", "duration", "tag_info", "checkup_time")

    ret_data_2.repartition(1).write.mode("overwrite").option("header", "true").csv("pm_data/VidTagsRank/random_vv_normal_tag")

    val vids_count = vid_tags.map(line =>{
      val vid = line.getString(0)
      val tag_info = line.getAs[Seq[Row]](2).map(row => {
        (row.getString(0), row.getLong(1), row.getDouble(2))
      })
      val poineer_count = tag_info.count(info => {
        0 < info._2 && info._2 < 100000000
      })
      val inner_count = tag_info.length - poineer_count
      (poineer_count, inner_count)
    }).cache()

    for(i <- 0 to 10) {
      printf("[count: %d], poineer_tag: %d, inner_tag: %d\n", i, vids_count.filter(_._1 == i).count(), vids_count.filter(_._2 == i).count())
    }
    for(i <- 1 until 10) {
      val i_mul = i * 10
      val i_mul_plus = i * 10 + 10
      val po_count = vids_count.filter(_._1 > i_mul).filter(_._1 <= i_mul_plus).count()
      val inner_count = vids_count.filter(_._2 > i_mul).filter(_._2 <= i_mul_plus).count()
      if(po_count != 0 && inner_count != 0)
        printf("[count: (%d-%d]], poineer_tag: %d, inner_tag: %d\n", i_mul, i_mul_plus, po_count, inner_count)
    }

    printf("[count>100], poineer_tag: %d, inner_tag: %d\n", vids_count.filter(_._1 > 100).count(), vids_count.filter(_._2 > 100).count())

    val tags_info = ret_data.flatMap(_._4.split(""";""", -1).take(30)).map(line=>{

      val data = line.split("""###""", -1)
      (data(0), data(1))
    }).toDF("tag", "tagid").cache

    println("print tags_info")
    tags_info.show



    val vid_title_map = vid_info.map(line=>(line.getString(0), line.getString(2))).toDF("vid_2", "vid_title")

    val tag_vids = spark.read.parquet(tag_vids_path).cache()



    val tags_info_ret = tags_info.join(tag_vids, $"tagid" === tag_vids("key"), "inner").select("tag", "tagid", "value_weight")
      .flatMap(line=> {
        val tag = line.getString(0)
        val tagid = line.getString(1)
        val vid_weight = line.getSeq[Row](2).map(arr=>{
          val vid = arr.getString(0)

          val weight = arr.getDouble(1)
          (tag, tagid, vid, weight)
        }).sortBy(_._4)(Ordering[Double].reverse).take(30)
        vid_weight

      }).toDF("tag", "tagid", "vid", "weight")
      .join(filter_vid, $"vid" === filter_vid("vid_filter"), "inner").drop("vid_filter")
      .cache
    println("print tags_info_ret")
    tags_info_ret.show()

    val tags_info_ret2 = tags_info_ret.join(vid_title_map, $"vid" === vid_title_map("vid_2"), "inner")
      .select("tag", "tagid", "vid", "vid_title", "weight")
      .map(line=>{
        val tag = line.getString(0)
        val tagid = line.getString(1)
        val vid = line.getString(2)
        val title = line.getString(3)
        val weight = line.getDouble(4)
        (tag, tagid, vid+"###" + title+"###" + weight.formatted("%.4f"))
      }).toDF("tag", "tagid", "vid_info")
      tags_info_ret2.createOrReplaceTempView("temp_db")

    val tag_data = spark.sql("select tag, tagid, collect_list(vid_info) as info from temp_db group by tag, tagid")
      .flatMap(line=>{
        val tag = line.getString(0)
        val tagid = line.getString(1)
        val info = line.getSeq[String](2).map(s=>{
          val d = s.split("###", -1)
          (d(0), d(1), d(d.length-1).replace("#", "").toDouble)
        }).sortBy(_._3)(Ordering[Double].reverse).take(10)

        info.map(tp=>{
          (tag, tagid, tp._1, tp._2, tp._3)
        })
      }).toDF("tag", "tagid", "vid", "title_1", "weight")
        .join(vid_info, $"vid" === vid_info("vid_info"), "inner")
      .drop("vid_info")
        .withColumn("url", url($"vid"))

      .sort($"tagid", $"weight".desc)


    tag_data.repartition(1).write.mode("overwrite").option("header", "true").csv("pm_data/VidTagsRank/top_tags_normal_vid")


    val tags_count = tag_vids.map(line =>{
      val key = line.getString(0)
      val tag_info = line.getAs[Seq[Row]](1).map(row => {
        (row.getString(0), row.getDouble(1))
      })
      val count = tag_info.length
      (key, count)
    }).cache()

    val tags_count_poin = tags_count.filter(line=> line._1.toLong > 0 && line._1.toLong < 100000000).cache()
    val tags_count_inner = tags_count.filter(line=> !(line._1.toLong > 0 && line._1.toLong < 100000000)).cache()

    for(i <- 0 to 10) {
      printf("[count: %d], poineer_tag: %d, inner_tag: %d\n",
        i,
        tags_count_poin.filter(_._2 == i).count(),
        tags_count_inner.filter(_._2 == i).count()
      )
    }
    for(i <- 1 until 10) {
      val i_mul = i * 10
      val i_mul_plus = i * 10 + 10
      val po_count = tags_count_poin.filter(_._2 > i_mul).filter(_._2 <= i_mul_plus).count()
      val inner_count = tags_count_inner.filter(_._2 > i_mul).filter(_._2 <= i_mul_plus).count()
      if(po_count != 0 && inner_count != 0)
        printf("[count: (%d-%d]], poineer_tag: %d, inner_tag: %d\n", i_mul, i_mul_plus, po_count, inner_count)
    }

    for(i <- 1 until 10) {
      val i_mul = i * 100
      val i_mul_plus = i * 100 + 100
      val po_count = tags_count_poin.filter(_._2 > i_mul).filter(_._2 <= i_mul_plus).count()
      val inner_count = tags_count_inner.filter(_._2 > i_mul).filter(_._2 <= i_mul_plus).count()
      if(po_count != 0 && inner_count != 0)
        printf("[count: (%d-%d]], poineer_tag: %d, inner_tag: %d\n", i_mul, i_mul_plus, po_count, inner_count)
    }

    printf("[count>1000], poineer_tag: %d, inner_tag: %d\n", tags_count_poin.filter(_._2 > 1000).count(), tags_count_inner.filter(_._2 > 1000).count())


  }

  def main(args: Array[String]) {


    val spark = SparkSession
      .builder
      .appName("VidTagsRank")
      .getOrCreate()


    val pm_data_path = args(0)  //"pm_data/data_v2.csv"
    val vid_tags_path = args(1)   // BasicData/VidTagsWeight/cleaned_data/20170926
    val vid_info_path = args(2)
    val tag_vids_path = args(3)
    val filter_path = args(4)
    println("------------------[begin]-----------------")

    getData(spark = spark,
      pm_data_path: String, vid_tags_path = vid_tags_path, vid_info_path = vid_info_path, tag_vids_path= tag_vids_path, real_filter_vid_path = filter_path)
    println("------------------[done]-----------------")
  }
}
