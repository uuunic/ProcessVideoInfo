package TagRecommender

import TagRecommender.TagRecommend.vid_idf_line
import breeze.linalg.{SparseVector, norm}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.udf

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer



/**
  * Created by baronfeng on 2017/8/30.
  */
object VidVidRecommander {
  // input_path: baronfeng/output/tag_recom_ver_1
  // sub_path: vid_idf
  // output_path: baronfeng/output/tag_recom_ver_1/vid_vid_recomm
  // filter_vid_path: /user/chenxuexu/vid_filter_v1/valid_vid_0830
  // return value: output_path
  val TAG_HASH_LENGTH = Math.pow(2, 24).toInt  // 目前暂定2^24为feature空间

  def vid_filter(spark: SparkSession, video_info_path: String, filter_vid_path: String, output_path: String) : String = {
    println("--------------[begin to process vid filter]--------------")
    import spark.implicits._
    val  ret_path = output_path + "/vid_cleaned"
    val video_info = spark.read.parquet(video_info_path).as[vid_idf_line]
    println("print video_info schema.")
    video_info.printSchema()
    val filter_vid = spark.read.parquet(filter_vid_path).withColumnRenamed("vid", "vid_filter")
    println("print vid_filter schema.")
    filter_vid.printSchema()
    val video_info_clean = video_info.join(filter_vid, $"vid" === filter_vid("vid_filter"), "inner")
      .select("vid", "duration", "tags", "tags_source", "features_index", "features_data")
    println("begin to write to path: " + ret_path)
    video_info_clean.repartition(80).write.mode("overwrite").parquet(ret_path)
    ret_path
  }

  def vid_vid_recomm_v2(spark: SparkSession, tag_df_length: Int, output_path: String): String = {
    import spark.implicits._
    val clean_input_path = output_path + "/vid_cleaned"
    val ret_path = output_path + "/vid_vid_recomm"

    spark.sqlContext.udf.register("vid_weight", (index: String, weight: Double) => {
      (index, weight)
    })


    println("--------------[begin recommend, source: " + clean_input_path + "]--------------")
    val clean_vid_data = spark.read.parquet(clean_input_path).as[vid_idf_line]
      .flatMap(line => {
        val vid = line.vid
        val index = line.features_index
        val data = line.features_data
        val ret_arr = new ArrayBuffer[(String, Int, Double)]
        for(i <- index.indices) {
           ret_arr.append((vid, index(i), data(i)))
        }
        ret_arr
      }).toDF("vid", "index", "weight")
    clean_vid_data.createOrReplaceTempView("temp_db")

    val sql_str = "select index, collect_list(vid_weight(vid, weight)) as vid_weight from temp_db group by index"
    val index_data = spark.sql(sql_str).as[(Int, Seq[(String, Double)])]
      .flatMap(line => {
        val ret_arr = new ArrayBuffer[(Int, String, String, Double)]


        val index = line._1
        val vid_weight_seq = if (line._2.length > tag_df_length) line._2.sortWith(_._2 > _._2).take(tag_df_length) else line._2
        for (i <- vid_weight_seq.indices) {
          val vid1 = vid_weight_seq(i)._1
          val weight1 = vid_weight_seq(i)._2
          for (j <- vid_weight_seq.indices if j != i) {
            val vid2 = vid_weight_seq(j)._1
            val weight2 = vid_weight_seq(j)._2
            ret_arr.append((index, vid1, vid2, weight1 * weight2))
          }
        }
        ret_arr
      }).toDF("index", "vid1", "vid2", "similarity")
    index_data.createOrReplaceTempView("index_db")


    val sql_str2_inner = "select vid1, vid2,  sum(similarity) as sim, collect_set(index) as indice from index_db group by vid1, vid2"
    val sql_str2_outer = "select row_number() over (partition by vid1 order by sim desc) as no, vid1, vid2, sim, indice from (" + sql_str2_inner + ") t "
    val sql_str2_outer2 = "select no as number, vid1, vid2, sim, indice from (" + sql_str2_outer + ") where no < 10  order by vid1, sim desc"
    val ret_data = spark.sql(sql_str2_outer2)

    ret_data.write.mode("overwrite").parquet(ret_path)

    println("--------------[write cosSimilarity done. output: " + ret_path +"]--------------")

    ret_path
  }
/**
  def vid_vid_recomm(spark: SparkSession, input_path: String, sub_path: String, filter_vid_path: String): String = {
    import spark.implicits._
    spark.udf.register("array_intersect",
      (xs: Seq[Int], ys: Seq[Int]) => xs.intersect(ys).nonEmpty)

    spark.udf.register("cos_similar",
      (l_index:Seq[Int], l_data:Seq[Double],  r_index: Seq[Int], r_data: Seq[Double]) => {
        val l = new SparseVector[Double](l_index.toArray, l_data.toArray, TAG_HASH_LENGTH)
        val r = new SparseVector[Double](r_index.toArray, r_data.toArray, TAG_HASH_LENGTH)

        l.dot(r) / (norm(l) * norm(r))

      })


    val output_path = input_path+ "/vid_vid_recomm"
    val whole_input_path = (input_path + "/" + sub_path).replace("//", "/")
    val vid_sv_data = spark.read.parquet(whole_input_path).as[vid_idf_line]

    val vid_useful = spark.read.parquet(filter_vid_path).toDF("vid_useful").as[String]

    val useful_vid_sv = vid_sv_data
      .join(vid_useful, vid_sv_data("vid") === vid_useful("vid_useful"), "left")
      .filter($"vid_useful".isNotNull)
      .select($"vid", $"tags", $"features_index", $"features_data", $"features_size").repartition(80).cache()
    println("recomm vid num is: " + useful_vid_sv.count())
    val useful_vid_sv2 = useful_vid_sv.toDF("vid2", "tags2", "features_index2", "features_data2", "features_size2").repartition(80).cache()

    useful_vid_sv.createOrReplaceTempView("vid_db1")
    useful_vid_sv2.createOrReplaceTempView("vid_db2")


    val sql_inner = "select db1.vid as vid1, db1.tags as tags1, db1.features_index as features_index1, db1.features_data as features_data1, db2.vid2 as vid2, db2.tags2 as tags2, db2.features_index2 as features_index2, db2.features_data2 as features_data2" +
      " from vid_db1 db1 full outer join vid_db2 db2 on (array_intersect(db1.features_index, db2.features_index2)=TRUE and db1.vid != db2.vid2)"

    val sql_outer = "select vid1, vid2, tags1, tags2, cos_similar(features_index1, features_data1, features_index2, features_data2) as cosx from (" +
                     sql_inner + ") where cos_similar(features_index1, features_data1, features_index2, features_data2) > 0.2"

    println("get vid matches data")
    spark.sqlContext.sql(sql_inner).show

    val ret_data = spark.sqlContext.sql(sql_outer)

    println("begin to write to " + output_path)
    ret_data.write.mode("overwrite").parquet(output_path)
    println("write cosSimilarity done")

    output_path

  }


**/


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

    // input_path: baronfeng/output/tag_recom_ver_1
    // sub_path: vid_idf
    // output_path: baronfeng/output/tag_recom_ver_1/vid_vid_recomm
    // filter_vid_path: /user/chenxuexu/vid_filter_v1/valid_vid_0830
    val input_path = args(0)
    val filter_path = args(1)
    val output_path = args(2)
    val tag_df_length = args(3).toInt
    val filter_data_path = output_path + "/vid_cleaned"
    //val filter_data_path = vid_filter(spark: SparkSession, video_info_path = input_path, filter_vid_path = filter_path, output_path)
    vid_vid_recomm_v2(spark, tag_df_length, output_path)


    println("------------------[done]-----------------")
  }
}
