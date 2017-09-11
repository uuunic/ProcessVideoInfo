package Pm

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import Utils.Hash._

/**
  * Created by baronfeng on 2017/9/5.
  */
object TagVids {
  def get_vid_info(spark: SparkSession, input_path_pm: String, input_path_useful: String) : String = {
    import spark.implicits._
    val output_path = "pm_results/TagVids"
    val tag_want = spark.sparkContext.textFile(input_path_pm)
      .map(_.trim)
      .filter(_.trim != "")
        .map(line=> {
          val data = line.split(" ", -1)
          (data(0), data(1))
        })
      .toDF("tag_id", "tag_name").cache

    val vid_data = spark.read.parquet(input_path_useful).cache
    vid_data.createOrReplaceTempView("vid_data")
    tag_want.createOrReplaceTempView("tag_want")

    val sql_str_source = "select vid_data.vid, vid_data.tags, vid_data.features_index, vid_data.features_data, vid_data.features_size, tag_want.tag_id, tag_want.tag_name " +
      "  from vid_data right join tag_want " +
      "  on array_contains(vid_data.tags, tag_want.tag_id) = true"

    val result_source = spark.sqlContext.sql(sql_str_source)
    result_source.printSchema()
    //result_source.show

    val result = result_source.filter(!_.isNullAt(2))
      .map(line => { // vid tags features_index features_data features_size tag_id tag_name
        val vid = line.getAs[String](0)
        val tag_name = line.getAs[String](6)

        val tag_id = line.getAs[String](5).toLong
        val hashval = hash_tf_idf(tag_id)

        val indice = line.getAs[Seq[Int]](2)
        val tag_index =indice.indexOf(hashval)

        val weights = line.getAs[Seq[Double]](3)
        val weight = weights(tag_index)
        (tag_id, tag_name, vid, weight)
      }).toDF("tag_id", "tag_name", "vid", "weight")

    val vid_source_path = "/data/stage/outface/omg/tdw/export_video_t_video_info_extend_hour/ds=2017090606"
    val vid_total = spark.sparkContext.textFile(vid_source_path)
      .map(line => line.split("\t", -1)).filter(_.length >107).filter(line => line(59) == "4" || line(59) == "0")
      .map(line => {
        val vid = line(1)
        val time = line(60)
        val title = line(49)
        (vid, title, time)
      }).toDF("vid_source", "title", "time")

    result.repartition(1).createOrReplaceTempView("temp_db")

    val sql_str = "select t.no as number, t.tag_id as tag_id, t.tag_name as tag_name, t.vid as vid, t.weight as weight" +
      " from  (select  row_number() over (partition by tag_id order by weight desc) as no, tag_id, tag_name, vid, weight from temp_db) t " +
      " where t.no< 50 order by t.tag_id asc, t.weight desc"

    val ret_without_title = spark.sqlContext.sql(sql_str).cache

    val ret = ret_without_title.join(vid_total, $"vid" === vid_total("vid_source"), "left")
      .select("number", "tag_id", "tag_name", "vid", "weight", "title", "time").orderBy($"tag_id".asc, $"weight".desc)

    println("useful vid count: " + ret.count)
    ret.repartition(1).write.mode("overwrite").csv(output_path)
    output_path

  }
  def main(args: Array[String]): Unit = {


    val spark = SparkSession.builder()
      .appName("pm")
      .getOrCreate()

    val vid_pm_path = args(0)
    val vids_path = args(1)

    get_vid_info(spark, vid_pm_path, vids_path)




    println("############################### is Done #########################################")
  }
}
