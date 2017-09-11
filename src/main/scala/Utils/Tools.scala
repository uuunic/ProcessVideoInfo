package Utils

import java.text.SimpleDateFormat
import java.util.{Calendar, Date}

import scala.collection.mutable.ArrayBuffer

/**
  * Created by baronfeng on 2017/9/10.
  */
object Tools {
  def get_n_day(N: Int, format: String = "yyyyMMdd"): String= {
    var dateFormat: SimpleDateFormat = new SimpleDateFormat(format)
    var cal: Calendar = Calendar.getInstance()
    cal.add(Calendar.DATE, N)
    dateFormat.format(cal.getTime)
  }
  def get_last_month_date_str(format: String= "yyyyMMdd") : Array[String] = {
    val ret = new ArrayBuffer[String]
    for(i <- 1 to 30) {
      ret += get_n_day(0 - i)
    }
    ret.toArray
  }

  def diff_date(date1: String, date2: String): Int = {
    val dateFormat1 = new SimpleDateFormat("yyyyMMdd")
    val d1 = dateFormat1.parse(date1).getTime
    val d2 = dateFormat1.parse(date2).getTime
    val days = ((d2 - d1) / (1000 * 3600 * 24)).toInt
    days
  }

  def main(args: Array[String]): Unit = {
    //get_last_month_date_str().foreach(println)
    println(get_n_day(-1))
  }

}
