package BasicData

import TagRecommender.TagRecommend.vid_idf_line
import Utils.{Defines, TestRedisPool, Tools}
import Utils.Tools.KeyValueWeight
import org.apache.spark.sql.SparkSession

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
  val TAG_HASH_LENGTH = Defines.TAG_HASH_LENGTH  // 目前暂定2^24为feature空间
  val REPARTITION_NUM = 400

  def vid_filter(spark: SparkSession, video_idf_path: String, filter_vid_path: String, output_path: String) : Unit = {
    println("--------------[begin to process vid filter]--------------")
    println("filter video idf  from  [" + video_idf_path + "]")
    // vid_filter path 先找到
    val real_filter_vid_path = Tools.get_latest_subpath(spark, filter_vid_path)
    println("filter use vids from  [" + real_filter_vid_path + "]")
    println("filter output path is [" + output_path + "]")
    import spark.implicits._

    val video_info = spark.read.parquet(video_idf_path).coalesce(REPARTITION_NUM).as[vid_idf_line]
    println("print video_info schema.")
    video_info.printSchema()
    // json形式的过滤列表
    val filter_vid = spark.read.json(real_filter_vid_path).coalesce(REPARTITION_NUM).toDF("vid_filter")
    println("print vid_filter schema.")
    filter_vid.printSchema()
    val video_info_clean = video_info.join(filter_vid, $"vid" === filter_vid("vid_filter"), "inner")
      .select("vid", "duration", "tags", "tags_source", "features_index", "features_data")
    println("begin to write to path: " + output_path)
    video_info_clean.repartition(200).write.mode("overwrite").parquet(output_path)

    println("--------------[vid filter done.]--------------")
  }

  def vid_vid_recomm_v2(spark: SparkSession, tag_df_length: Int, vid_recomm_num: Int, clean_input_path: String, output_path: String): String = {
    import spark.implicits._
    spark.sqlContext.udf.register("vid_weight", (index: String, weight: Double) => {
      (index, weight)
    })
    println("--------------[begin recommend, source: " + clean_input_path + "]--------------")
    val clean_vid_data = spark.read.parquet(clean_input_path).as[vid_idf_line]
      .flatMap(line => {
        val vid = line.vid
        val index = line.features_index
        val data = line.features_data
        //        val ret_arr = new ArrayBuffer[(String, Int, Double)]
        //        for(i <- index.indices) {
        //           ret_arr.append((vid, index(i), data(i)))
        //        }
        val ret_arr = index.zip(data).map(line=>{(vid, line._1, line._2)})
        ret_arr
      }).toDF("vid", "index", "weight").cache()
    printf("clean vid number: [%d]", clean_vid_data.count())

    clean_vid_data.createOrReplaceTempView("temp_db")

    val sql_str = "select index, collect_list(vid_weight(vid, weight)) as vid_weight from temp_db group by index"
    val index_data = spark.sql(sql_str).as[(Int, Seq[(String, Double)])].repartition(REPARTITION_NUM)

      .flatMap(line => {
        val ret_arr = new ArrayBuffer[(Int, String, String, Double)]

        val index = line._1
        val vid_weight_seq = line._2.sortBy(_._2)(Ordering[Double].reverse).take(tag_df_length)
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
      }).toDF("index", "vid1", "vid2", "similarity").repartition(REPARTITION_NUM, $"vid1")
    index_data.createOrReplaceTempView("index_db")


    val sql_str2_inner = "select vid1, vid2,  sum(similarity) as sim, collect_set(index) as indice from index_db group by vid1, vid2"
    val sql_str2_outer = "select row_number() over (partition by vid1 sort by sim desc) as no, vid1, vid2, sim, indice from (" + sql_str2_inner + ") t "
    val sql_str2_outer2 = "select no as number, vid1, vid2, sim, indice from (" + sql_str2_outer + s") where no <= $vid_recomm_num  distribute by vid1 sort by vid1, sim desc"
    val ret_data = spark.sql(sql_str2_outer2)

    ret_data.write.mode("overwrite").parquet(output_path)

    println("--------------[write cosSimilarity done. output: " + output_path +"]--------------")

    output_path
  }

  def put_vid_vid_to_redis(spark: SparkSession, path : String, flags: Seq[String]): Unit = {
    import spark.implicits._
    val ip = "100.107.17.216"
    val port = 9020
    //val limit_num = 1000
    val bzid = "sengine"
    val prefix = "v0_sv_nr"
    val tag_type: Int = -1
    val data = spark.read.parquet(path).filter($"number" === 1).select($"vid1", $"vid2", $"sim").map(line=>{
      val vid1 = line.getString(0)
      val vid2 = line.getString(1)
      val similarity = line.getDouble(2)
      val value_weight = Array((vid2, similarity))
      KeyValueWeight(vid1, value_weight)
    })
      //  .filter(d => Tools.boss_guid.contains(d.key))
      .cache

    data.show()
    val test_redis_pool = new TestRedisPool(ip, port, 40000)
    val broadcast_redis_pool = spark.sparkContext.broadcast(test_redis_pool)
    if(flags.contains("delete")) {
      Tools.delete_to_redis(data, broadcast_redis_pool, bzid, prefix, tag_type /*, limit_num = 1000 */)
    }
    if(flags.contains("put")) {
      Tools.put_to_redis(data, broadcast_redis_pool, bzid, prefix, tag_type /*, limit_num = 1000 */)
    }
    println("-----------------[put_vid_vid_to_redis] to redis done, number: " + data.count)

  }


  def main(args: Array[String]) {

    /**
      * step1. create SparkSession object
      * 封装了spark sql的执行环境，是spark SQL程序的唯一入口
      */


    val spark = SparkSession
      .builder
      .appName(this.getClass.getName.split("\\$").last)

      .getOrCreate()


    val input_path = args(0)
    val filter_path = args(1)
    val output_path = args(2)
    val tag_df_length = args(3).toInt
    val date = args(4)
    val vid_recomm_num = args(5).toInt
    val control_flag = args(6).split(Defines.FLAGS_SPLIT_STR, -1).toSet
    println("------------------[begin]-----------------")
    println("control flags is: " + control_flag.mkString("""||"""))
    val filter_output_path = output_path + "/vid_filter/" + date
    val vid_recomm_output_path = output_path + "/vid_vid_recomm_v2/" + date
    if(control_flag.contains("create")) {
      vid_filter(spark, video_idf_path = input_path, filter_vid_path = filter_path, filter_output_path)

      vid_vid_recomm_v2(spark,
        tag_df_length,
        clean_input_path = filter_output_path,
        vid_recomm_num = vid_recomm_num,
        output_path = vid_recomm_output_path)
    }


    put_vid_vid_to_redis(spark, path = vid_recomm_output_path, flags = control_flag.toSeq)



    println("------------------[done]-----------------")
  }
}