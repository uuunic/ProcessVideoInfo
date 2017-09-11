package Pm

import TagRecommender.TagRecommend.vid_idf_line
import org.apache.spark.sql.SparkSession
import org.apache.spark.unsafe.hash.Murmur3_x86_32.hashUnsafeBytes
import org.apache.spark.unsafe.types.UTF8String

import scala.collection.mutable.ArrayBuffer

/**
  * Created by baronfeng on 2017/8/31.
  */
object VidTags {
  def hash(s: String, mod_num: Int = Math.pow(2, 24).toInt, seed: Int = 42) : Int = {
    val utf8 = UTF8String.fromString(s)

    val hashval = hashUnsafeBytes(utf8.getBaseObject, utf8.getBaseOffset, utf8.numBytes(), seed)
    ((hashval % mod_num) + mod_num) % mod_num
  }

  def get_vid_info(spark: SparkSession, input_path_pm: String, input_path_useful: String, output_path: String) : String = {
    import spark.implicits._
    spark.udf.register("tag_recognize",
      (tags: Seq[String], indice: Seq[Int], datas: Seq[Double]) => {
        if(tags == null || tags.isEmpty)
          null
        else {
          val ret_tags = new ArrayBuffer[(String, Double)]
          for(tag<-tags.distinct){
            val hash_value = hash(tag)
            val index = indice.indexOf(hash_value)
            val weight = datas(index)
            ret_tags += ((tag, weight))
          }
          ret_tags.sortWith(_._2> _._2).map(line=>line._1 + "#" + line._2.toString.substring(0,6)).mkString(";")
        }
      })


    val vid_want = spark.sparkContext.textFile(input_path_pm).toDF("vid_want")
    vid_want.show(10)
    val vids = spark.read.parquet(input_path_useful).as[vid_idf_line]
    //vid_want vid tags features_index features_data features_size
    vids.show()
    val vid_join = vid_want.join(vids, vid_want("vid_want") === vids("vid"), "left")
        .select("vid_want", "tags", "features_index", "features_data", "features_size")
    println("vid_join number: " + vid_join.count)
    vid_join.createOrReplaceTempView("temp_db")
    val ret = spark.sqlContext.sql("select vid_want, tag_recognize(tags, features_index, features_data) from temp_db")
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
