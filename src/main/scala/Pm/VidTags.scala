package Pm

import TagRecommender.TagRecommend.vid_idf_line
import Utils.Hash
import org.apache.spark.sql.SparkSession

import scala.collection.mutable.ArrayBuffer

/**
  * Created by baronfeng on 2017/8/31.
  */
object VidTags {


  def get_vid_info(spark: SparkSession, input_path_pm: String, input_path_useful: String, output_path: String) : String = {
    import spark.implicits._
    spark.udf.register("tag_recognize",
      (tags_id: Seq[Long], tags_source: Seq[String], indice: Seq[Int], datas: Seq[Double]) => {
        if(tags_id == null || tags_id.isEmpty)
          null
        else {
          val ret_tags = new ArrayBuffer[(String, Long, Double)]
          for(tag<-tags_source.distinct){
            if(tag.contains("_inner")){
              val tag_pure = tag.replace("_inner", "")
              val hash_value = Hash.hash_own(tag_pure)
              val hash_tf = Hash.hash_tf_idf(hash_value)
              val index = indice.indexOf(hash_tf)
              val weight = datas(index)
              ret_tags append ((tag_pure, hash_value, weight))
            } else {  //先锋id
              val tag_data = tag.split("##", -1)
              val tag_pure = tag_data(0)
              val hash_value = if(tag_data.length == 1) tag_data(0).toLong else tag_data(1).toLong
              val hash_tf = Hash.hash_tf_idf(hash_value)
              val index = indice.indexOf(hash_tf)
              val weight = datas(index)
              ret_tags append ((tag_pure, hash_value, weight))
            }
          }
          ret_tags.sortWith(_._3> _._3).map(line=>line._1 + "#" + line._2 + "#" + line._3.formatted("%.4f")).mkString(";")
        }
      })


    val vid_want = spark.sparkContext.textFile(input_path_pm).toDF("vid_want")
    vid_want.show(10)
    val vids = spark.read.parquet(input_path_useful).as[vid_idf_line]
    //vid_want vid tags features_index features_data features_size
    vids.show()
    val vid_join = vid_want.join(vids, vid_want("vid_want") === vids("vid"), "left").filter($"vid".isNotNull)
        .select("vid_want", "tags", "features_index", "features_data", "tags_source")
    println("vid_join number: " + vid_join.count)
    vid_join.createOrReplaceTempView("temp_db")
    val ret = spark.sqlContext.sql("select vid_want, tag_recognize(tags, tags_source, features_index, features_data) from temp_db")
    ret.repartition(1).write.mode("overwrite").csv(output_path)

    println ("write to " + output_path)
    output_path
  }

  def main(args: Array[String]): Unit = {


    val spark = SparkSession.builder()
      .appName("pm")
      .getOrCreate()

    val vid_pm_path = args(0)
    val vids_path = args(1)
    val output_path = args(2)
    get_vid_info(spark, vid_pm_path, vids_path, output_path)




    println("############################### is Done #########################################")
  }
}
