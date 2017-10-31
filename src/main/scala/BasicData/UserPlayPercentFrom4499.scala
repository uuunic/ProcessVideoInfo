package BasicData

import org.apache.commons.codec.binary.Base64
import org.apache.spark.sql.SparkSession

/**
  * Created by baronfeng on 2017/9/12.
  * 以天级别的落4499的播放数据
  */
object UserPlayPercentFrom4499 {
  /**
    * 从4499里获取有用的信息，目前只需要 guid, vid, ts, playduration, duration, percent
    * 4499的播放数据，默认从mini_session走，如果不给力的话，就走4499
    * duration里会出现等于0的情况，目前的调查是发现和用户有关，和vid无关（在vid维度上随机出现），所以可以过滤掉
    *
    * @param inputPath 4499的原始数据位置, 默认为 /data/gather/etl/4499/{YYYYMMDD}/{HH} 延迟2小时
    *                  如果没有找到当天的mini_session,就从这里拿
    * @param output_path BasicData/PlayPercent
    *
    * @return DataFrame, 应该包含guid, vid, ts, playduration, duration, percent  duration如果为0，percent默认为20%，也就是0.2
    */

  def process_4499(spark :SparkSession, inputPath:String, output_path: String): String ={
    //base64解析 + json解析
    val path_4499 = inputPath
    val res_path = output_path
    println("----------[process 4499 data, source: " + path_4499 + "]----------")
    if (inputPath == null || inputPath.equals("") || inputPath.equals("\t"))
      return null
    val parquet_4499 = spark.read.parquet(inputPath + "/*")
    parquet_4499.createOrReplaceTempView("parquet_4499")
    spark.sqlContext.udf.register("get_base64", (s: Array[Byte]) => new String(Base64.decodeBase64(s)))
    val sql_text_inner =
      "select " +
        "guid, " +
        "cast(ts as long) as ts, " +
        "vid, cid, " +
        "kv.playduration as playduration, " +
        "duration " +
        " from parquet_4499 lateral view json_tuple(get_base64(`data`), \"playduration\") kv " +
        "as playduration " +
        "where step = 50 and  guid != '' and ts != '' and length(vid)=11 and playduration != 0.0"
    val sql_text_outer = "select guid, ts, vid, playduration, duration, case when duration < 1.0 then 0.2 else playduration/duration end as percent from (" + sql_text_inner + ")"
    val parquet_4499_res = spark.sqlContext.sql(sql_text_outer)
    println("----------[process 4499 data done, write to: " + res_path + "]----------")
    parquet_4499_res.coalesce(400).write.mode("overwrite").parquet(res_path)
    res_path
  }

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName {
        this.getClass.getName.split("\\$").last
      }
  //      .master("local")
      .getOrCreate()

    val input_path = args(0)
    val output_path = args(1)

    process_4499(spark, input_path, output_path)
    println("############################### is Done #########################################")
  }
}
