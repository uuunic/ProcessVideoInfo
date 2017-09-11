package TagRecommender

import org.apache.hadoop.fs.{FSDataOutputStream, FileSystem, Path}
import org.apache.spark.sql._
import org.apache.spark.sql.expressions.Aggregator
import org.apache.spark.sql.functions._

import scala.io.Source
import scala.collection.mutable.ArrayBuffer
import java.text.SimpleDateFormat
import java.util.Date
import java.util.Calendar

import org.apache.spark.SparkContext
import org.apache.hadoop.io.{LongWritable, Text}
import org.apache.hadoop.mapred.TextInputFormat
import scala.util.{Success, Try}

/**
  * Created by chenxuexu on 2017/8/25.
  * 内容池过滤：1）先根据pm给出的分类时效性过滤
  *             2）封面图逻辑：1.封面图逻辑：1）能否访问，2）图像分辨率；3）访问时长
  *             3）加入近30天播放次数大于5次
  * /data/stage/outface/omg/tdw/export_video_t_video_info_extend_hour/ds=2017071723/attempt_*.gz vid的数据源。这个产生的idf中间变量，为output+"temp_file"
  */

object VidFilter {
  case class vid(vid:String)
  case class id_duration(id:String, duration:Int)
  case class vid_play_times(vid:String, play_times:Long)
  case class vid_info(vid:String, vid_info:String)
  
  def addtime(date:String,num:Int):String ={
    val myformat = new SimpleDateFormat("yyyyMMdd")
    var dnow = new Date()
    if(date !=""){
      dnow=myformat.parse(date)}
    var cal = Calendar.getInstance()
    cal.setTime(dnow)
    cal.add(Calendar.DAY_OF_MONTH,num)
    val newday= cal.getTime()
    myformat.format(newday)
  }

  /**
    * date2比date1多的天数
    */
  def differentDays(date1: Date, date2: Date): Int = {
    val cal1 = Calendar.getInstance
    cal1.setTime(date1)
    val cal2 = Calendar.getInstance
    cal2.setTime(date2)
    val day1 = cal1.get(Calendar.DAY_OF_YEAR)
    val day2 = cal2.get(Calendar.DAY_OF_YEAR)
    val year1 = cal1.get(Calendar.YEAR)
    val year2 = cal2.get(Calendar.YEAR)
    if (year1 != year2) { //同一年
      var timeDistance = 0
      var i = year1
      while ( {
        i < year2
      }) {
        if (i % 4 == 0 && i % 100 != 0 || i % 400 == 0) { //闰年
          timeDistance += 366
        }
        else { //不是闰年
          timeDistance += 365
        }
        {
          i += 1; i - 1
        }
      }
      timeDistance + (day2 - day1)
    }
    else { //不同年
      System.out.println("判断day2 - day1 : " + (day2 - day1))
      day2 - day1
    }
  }
  //判断时效性
  def verify_active_duration(arr: Array[String], now:String,MAP:scala.collection.mutable.Map[String, Int]):Boolean = {
    val format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    if (arr(83)=="1335033")//长期   99999待定
      return true
    else if(arr(83)=="1335035") {//短期
      /*if (dateformat2.parse(now).getTime() - dateformat2.parse(arr(60)).getTime() >= 24*60*60*1000)
        return false
      else return true*/
      if(differentDays(format.parse(arr(60)),format.parse(now)) > 1)
        return false
      else return true
    } else if (arr(83)=="1335034"){  //一般
      var duration = 0
      if (MAP.contains(arr(74).split("#",-1)(0)))//二级
      {  duration = MAP(arr(74).split("#",-1)(0))
        var valid_time = duration
        if (differentDays(format.parse(arr(60)),format.parse(now)) > valid_time)
          return false
        else if (MAP.contains(arr(73).split("#",-1)(0)))//一级
        {
          duration = MAP(arr(73).split("#",-1)(0))
          valid_time = duration * 24*60*60*1000
          if (differentDays(format.parse(arr(60)),format.parse(now)) > valid_time)
            return false
          else return true
        }
      }
    }
    return false
  }

  //读取4499清洗过的数据
  def filter_4499(spark:SparkSession, nowPath:String,valid_vids:Dataset[vid],threshold:Int): Dataset[vid] ={
    val parquetFile = spark.read.parquet(nowPath)
    parquetFile.createOrReplaceTempView("vid_count")
    val Sql1 = "SELECT vid, SUM(count) AS play_times FROM vid_count GROUP BY vid"
    import spark.implicits._
    val vid_count = parquetFile.sqlContext.sql(Sql1).toDF.as[vid_play_times]//get: vid-play_times
    //println("-------------Function: filter_4499: vid_count.printSchema------------")
    //vid_count.printSchema()
    //vid_count
    val vid_count2 = vid_count.filter($"play_times" >= threshold)
    val filtered_vid = valid_vids.join(vid_count2,"vid").select("vid").as[vid]
    filtered_vid
  }

  def main(args: Array[String]) {
    val inputPath0 = args(0)//vid_extend_hour_data path,最后一个是/
    val inputValue = args(1)//5点的好了 time interval
    val active_duration_file = args(2)
    val threshold = args(3)//5，0
    val first_recommand_vid_file = args(4)
    val outputPath =args(5)
    val spark = SparkSession
      .builder
      //.master("local")
      .appName("vid_filter")
      .getOrCreate()
    //计算读取媒资库的路径
    // /data/stage/outface/omg/tdw/export_video_t_video_info_extend_hour/ds=2017071723/attempt_*.gz
    val dateformat = new SimpleDateFormat("yyyyMMddHH")
    val now = dateformat.format(new Date())
    //dataformat2，now2是判断时效性的时间
    val dateformat2 = new SimpleDateFormat("yyyy-MM-dd HH:MM:SS")
    val now2 = dateformat2.format(new Date())

    val deltaT_in_millisec = inputValue.toInt * 3600000//60分*60秒*1000毫秒
    val begin_time = dateformat.parse(now).getTime - deltaT_in_millisec
    val begin_time_str = dateformat.format(new Date(begin_time))
    val vid_path = inputPath0 + begin_time_str + "/attempt_*.gz"
    println("----------now---------"+inputPath0 + now+"--------------")
    println("----------begin_time---------"+vid_path+"--------------")

    import spark.implicits._
    //读取分类时效性file
    val Map = scala.collection.mutable.Map[String, Int]()
    //active_duration得到rdd内容为1494446=9999
    val active_duration  = spark.sparkContext.textFile(active_duration_file, 200).filter(arr => arr.contains("#")).map(line => line.split(" ",-1)(0)).filter(line => line.split("=",-1).length == 2).collect()
    var countLines = 0
    for ( countLines <- active_duration.indices) {
      Map(active_duration(countLines).split("=",-1)(0)) = active_duration(countLines).split("=",-1)(1).toInt
    }
    println("Map.size => "+ Map.size)

    //读取媒资库数据,过滤
    val valid_vid  = spark.sparkContext.textFile(vid_path, 200)//vid信息
      .map(line => line.split("\t", -1))
      .filter(_.length >= 127 ).filter(arr => arr(1) != "" && arr(73) != "" && arr(74) != "")//vid长度，vid不为空，有一级（73）二级（74）分类id
      .filter(arr => verify_active_duration(arr, now2, Map))//判断时效性
      .filter(arr => arr(59) == "4" && arr(55) == "0")//59, b_state :上架状态，值为4; 55.b_drm：非付费视频，值为0
      .filter(arr => !arr(115).contains("已拒绝"))//115  is_normalized 是否已标准化 剩1216575 条
      .filter(arr => arr(126).contains("3") && arr(126).contains("5"))//平台可播状态。ios and android
      .filter(arr => !arr(98).contains("1505275") && !arr(99).contains("1505276") && !arr(87).contains("1487114"))//播放控制，标题尺度低俗，标题图片尺度低俗，内容低俗，剩1198276
      .filter(arr => arr(49).length > 5 && !arr(49).matches("\\d+") )//49，中文标题字数显示 其他标题字数限制（标题小于5个字）、纯数字标题、、全品类纯其它外文标题 1193569
      .filter(arr => !(arr(73) !="1494470" && arr(49).matches("[a-zA-Z]+")))//非音乐类纯英文标题  1193451
      .filter(arr => arr(57).toInt >= 7 && arr(57).toInt <= 600)//时长大于7 小于600
      .map(arr=>arr(1)).toDF.withColumnRenamed("value","vid").as[vid]//选vid
    //暂时关闭指纹判重
    //val valid_vids = valid_vid.join(first_vid,"vid").as[vid]
    val valid_vids = valid_vid

    //使用播放数据，去掉一个月内没有播放过的vid
    val data4499 = "/user/chenxuexu/new_hot_recomm/total_20170821/read_Data4499_ByDay/time="
    var nowPath = data4499 + "{"//20170817
    val i = 1
    for(i <- 1 to 30 ){
      println("--------i = " + i)//time={20170817,20170818}通配符写法
      val current_read_date = addtime(now.substring(0,8),-1 * (i-1))//20170717
      nowPath =nowPath + current_read_date
      if(i < 30)
        nowPath = nowPath +","
    }
    nowPath = nowPath + "}/*"
    println(nowPath)
    val valid_vids_4499_filtered = filter_4499(spark, nowPath, valid_vids, threshold.toInt)
    valid_vids_4499_filtered.printSchema()
    println(valid_vids_4499_filtered.count)
    valid_vids_4499_filtered.write.mode("overwrite").parquet(outputPath + "/vid_filter")
  }
}
