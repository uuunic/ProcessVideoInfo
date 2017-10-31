package DataAdapter

import Utils.Tools
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.functions._

/**
  * Created by baronfeng on 2017/10/26.
  */
object UserInterest {
  val REPARTITION_NUM = 400
  val spark: SparkSession = SparkSession.builder()
    .appName {
      this.getClass.getName.split("\\$").last
    }
    //      .master("local")
    .getOrCreate()

  // eg. CAT_S2:1494448.1494491:15.6|1494456.1494560:14.9$TAG:821535:29.8|9567889:15.1|764667:14.7
  def get_interests(guid_tags_monthly_path: String, guid_cat_monthly_path: String, output_path: String) : Unit = {
    import spark.implicits._
    // guid vtags数据格式化
    println("-------------begin to get vtags interests---------------")
    val guid_tags_monthly = spark.read.parquet(guid_tags_monthly_path)
      .map(line=>{
        val guid = line.getString(0)
        val value_weight = line.getAs[Seq[Row]](1)
          .map(v=>(v.getLong(0).toString, v.getDouble(1))).sortBy(_._2)(Ordering[Double].reverse).take(30).distinct
          .map(tp=>(tp._1, Tools.normalize(tp._2)))
          .map(tp=>s"${tp._1}:${tp._2.formatted("%.4f")}")
          .mkString(raw"|")

        //_1 tag _2 tag_id _3 weight
        (guid, "TAG:" + value_weight)
      }).toDF("guid", "tag_weight").repartition(REPARTITION_NUM, $"guid")
    println("-------------begin to get cat interests---------------")
    // guid cat 数据格式化  cat数据没有做guid的聚合，先做一下
    val guid_cat_monthly = spark.read.parquet(guid_cat_monthly_path)
      .map(line=>{
        val guid = line.getString(0)
        val first = line.getInt(1)
        val second = line.getInt(2)
        val weight = line.getDouble(3)
        val cat_weight = (first, second, weight)
        (guid, cat_weight)
      }).toDF("guid", "cat_weight")
      .groupBy($"guid").agg(collect_list($"cat_weight") as "cat_weight")
      .map(line=>{
        val guid = line.getString(0)
        val cat_weight = line.getSeq[Row](1)
          .map(tp=>{(tp.getInt(0), tp.getInt(1), tp.getDouble(2))})
          .sortBy(_._3)(Ordering[Double].reverse).take(30).distinct
          .map(tp=>{
            s"${tp._1}.${tp._2}:${tp._3.formatted("%.4f")}"
          })
          .mkString(raw"|")
        (guid, "CAT_S2:" + cat_weight)
      }).toDF("guid_tmp", "cat_weight").repartition(REPARTITION_NUM, $"guid_tmp")
    println("-------------begin to join interests---------------")
    val guid_data_res = guid_tags_monthly.join(guid_cat_monthly, $"guid" === $"guid_tmp", "outer")
      .select($"guid", $"guid_tmp", $"tag_weight", $"cat_weight")  // String, String, String  后两个有可能为空
      .map(line=>{
      val guid = if(line.isNullAt(0)) line.getString(1) else line.getString(0)
      val tag_weight = if (!line.isNullAt(2)) line.getString(2) else null
      val cat_weight = if (!line.isNullAt(3)) line.getString(3) else null
      val res_str = {
        if(tag_weight == null) cat_weight
        else if (cat_weight == null) tag_weight
        else {cat_weight + s"$$" + tag_weight}
      }
      "guid=" + guid + "\t" + res_str
    })

    println(s"s-------------begin to write interests to path: $output_path---------------")
    guid_data_res.toDF("value").write.mode("overwrite").text(output_path)
    println("-------------write interests done---------------")
  }

  def main(args: Array[String]): Unit = {


    val guid_vtags_path = args(0)
    val guid_cat_path = args(1)
    val output_path = args(2)
    val date = args(3)

    get_interests(guid_tags_monthly_path = guid_vtags_path,
      guid_cat_monthly_path = guid_cat_path,
      output_path = output_path + "/" + date)
    println("############################### is Done #########################################")
  }
}
