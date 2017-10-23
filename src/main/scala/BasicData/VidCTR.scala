package BasicData
import Utils.Defines
import org.apache.spark.sql.SparkSession

import scala.util.{Success, Try}


/**
  * Created by baronfeng on 2017/10/17.
  */
object VidCTR {
  val REPARTITION_NUM: Int = 400
  case class vid_ztid_click_exposure(vid: String, ztid: String, click: Int, exposure: Int)

  //判断数据类型，去掉脏数据
  private[this] def verify(str: String, dtype: String):Boolean = {
    var c:Try[Any] = null
    if("double".equals(dtype)) {
      c = Try(str.toDouble)
    } else if("int".equals(dtype)) {
      c = Try(str.toInt)
    }
    val result = c match {
      case Success(_) => true;
      case _ =>  false;
    }
    result
  }

  def process(spark: SparkSession,
              input_path: String,
              output_path: String,
              click_min: Int = 100,
              exposure_min: Int = 100,
              ztid_filter_set: Set[String]) : Unit = {
    import spark.implicits._
    println(s"begin to process\n input_path: $input_path, output_path: $output_path")
    val input = spark.sparkContext.textFile(input_path)
      .map(arr=> arr.split("\t",-1))
      .filter(_.length >= 4 )
      .filter(arr => arr(0) != ""&& verify(arr(2),"int") && verify(arr(3),"int"))

      .map(arr => vid_ztid_click_exposure(arr(0),arr(1),arr(2).toInt,arr(3).toInt))
      .filter(data =>{ztid_filter_set.contains(data.ztid)})
      .toDS
      .repartition(REPARTITION_NUM, $"vid")
    input.createOrReplaceTempView("ctr_data")
    val sql_str = "SELECT vid, ztid, SUM(click) as click, SUM(exposure) as exposure, SUM(click)/SUM(exposure) AS ctr FROM ctr_data GROUP BY vid, ztid"
    val ctr_data = spark.sql(sql_str).repartition(REPARTITION_NUM / 20)
      .filter($"click">=click_min || $"exposure" >= exposure_min)
      .filter($"ctr" <= 1.0).cache()
    println("ctr data count: " + ctr_data.count())
    ctr_data.write.mode("overwrite").parquet(output_path)
  }

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName {
        this.getClass.getName.split("\\$").last
      }
      //      .master("local")
      .getOrCreate()

    val input_path_root = args(0)
    val output_path_root = args(1)
    val date = args(2)
    val click_min = args(3).toInt
    val exposure_min = args(4).toInt

    val ztid_filter_set = args(5).split(Defines.FLAGS_SPLIT_STR, -1).toSet

    val input_path = input_path_root + "/ds=" + date + "*"
    val output_path = output_path_root + "/" + date
    process(spark,
      input_path = input_path,
      output_path = output_path,
      click_min = click_min,
      exposure_min = exposure_min,
      ztid_filter_set = ztid_filter_set)
    println("############################### is Done #########################################")
  }
}
