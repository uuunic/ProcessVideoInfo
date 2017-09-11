package Algorithm

import org.apache.hadoop.fs.{FSDataOutputStream, FileSystem, Path}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import org.apache.spark.sql.expressions.Aggregator

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

/**
  * Created by merlinnshi on 2017/7/29.
  * Task1 :（1）我们的数据和媒资人工数据的共同部分，即我们有的媒资库也有的VID-CID列表
  *       （2）我们没有识别出来但是媒资库填写的数据部分，即我们没有媒资库有的VID-CID列(vid , !cid)
  * Task2 : 头部剧的CID列表中，根据这个热搜榜单里的电影，电视剧，综艺动漫分别排名前10个的CID，每个CID随机抽取20个对应VID。进行人工看内容评估准确性。
  * Task3 : 全量VID库中随机抽取200个VID，以及部分给出VID对应的CID，就是能识别出那一部分。进行长尾的人工数据评测。
  * cover_url : "https://v.qq.com/x/cover/" + cid + ".html"
  * video_url : "https://v.qq.com/x/page/" + vid + ".html"
  *  /data/stage/outface/omg/export_video_t_cover_info_extend_hour/ds=2017071723 （每天23点落一份，一份就是全量）  cid的数据源，格式为ProcessVideoInfo的line_cover_info
  * /data/stage/outface/omg/tdw/export_video_t_video_info_extend_hour/ds=2017071723/attempt_*.gz vid的数据源。这个产生的idf中间变量，为output+"temp_file"
  */
object pm {

  case class vid_cid(vid:String, cid:String)
  case class vid_title(vid:String, vid_title:String)
  case class vid_title_cid(vid:String, title:String, cid:String)
  case class cid_vid(cid:String, vid:String)
  case class cid_title(cid:String, cid_title:String)
  case class vid_cidOurs(vid:String, cid_ours:String)

  case class vid_url_cid_url(top_vid:String, vid_url:String, top_cid:String, cid_url:String)
  case class cid_url_vid_url(top_cid:String, cid_url:String, top_vid:String, vid_url:String)
  case class vid_url_cid_url_cid_url_coclick_vidclick_rate(vid:String, vid_title: String, cid_dict:String, cid_ours:String, cid_ours_title: String, cosSim:Double, vid_tags:mutable.WrappedArray[String], cid_tags:mutable.WrappedArray[String])
  case class vid_cid_coclick_vidclick_rate(vid:String, cid:String, coclick:Long, vidclick:Long, rate:Double)
//$"vid", $"cid", $"co_click", $"vid_click"
  val reg_str = "#"

  def groupConcat(separator: String, columnToConcat: Array[Int]) = new Aggregator[Row, String, String] with Serializable {
    override def zero: String = ""
    override def reduce(b: String, a: Row) = b + separator + (a.get(columnToConcat(0)), a.get(columnToConcat(1)), a.get(columnToConcat(2)))
    override def merge(b1: String, b2: String) = b1 + b2
    override def finish(b: String) = b.substring(1)
    override def bufferEncoder: Encoder[String] = Encoders.STRING
    override def outputEncoder: Encoder[String] = Encoders.STRING
  }.toColumn

  def task1(spark:SparkSession, inputPath:String, df_usage:Dataset[pm.vid_cid], outputPath:String,  cid_title_df:Dataset[pm.cid_title]): DataFrame = {
    import spark.implicits._


    var res_str:String = ""
    val hadoopConf = spark.sparkContext.hadoopConfiguration
    val hdfs = org.apache.hadoop.fs.FileSystem.get(hadoopConf)
    val path = new Path(outputPath + "/" + "task1" + "/" + "data")
    if(hdfs.exists(path)){
      hdfs.delete(path)
    }

    cid_title_df.cache()

      res_str += "------------------------- data ----------------------------" + "\n"
      val ours = spark.read.parquet(inputPath)

//      val ours = parquet.map(line => vid_cid(line.get(0).asInstanceOf[String], line.get(1).asInstanceOf[String]))

      val count_dict = df_usage.count()  //vid_cid <-编辑推荐
      val count_ours = ours.count()
      //求交集 vid 和 cid相同
      val intersects = ours.join(df_usage, df_usage("vid") === ours("vid") && df_usage("cid") === ours("cid"))

      println( "our count = " + count_ours)
      ours.printSchema()
      println ( "dict count = " + count_dict)
    df_usage.printSchema()
      println ("(vid and cid equal)intersect count = " + intersects.count())

      //求差集 vid相同 cid不同
      val excepts = df_usage.join(ours, df_usage("vid") === ours("vid") && df_usage("cid") =!= ours("cid"))
      println("except count = " + excepts.count())

      val tmp = excepts.map(row => vid_url_cid_url_cid_url_coclick_vidclick_rate( row.get(0).asInstanceOf[String], row.getAs[String](5),
                                                            row.get(1).asInstanceOf[String],
                                                            row.get(3).asInstanceOf[String], row.getAs[String](6),
                                                            row.get(4).asInstanceOf[Double], row.get(7).asInstanceOf[mutable.WrappedArray[String]], row.get(8).asInstanceOf[mutable.WrappedArray[String]]))
      val result = tmp.select("vid_title", "vid", "cid_dict", "cid_ours", "cid_ours_title", "cosSim", "vid_tags", "cid_tags")
         .join(cid_title_df, tmp("cid_dict") === cid_title_df("cid"), "left")
         .select($"vid_title", $"vid", $"cid_dict", $"cid_title".as("cid_dict_title"), $"cid_ours", $"cid_ours_title", $"cosSim", $"vid_tags", $"cid_tags")

      println("show data1 result")
    result.show

        result.write.mode("overwrite").parquet(outputPath + "/" + "task1" + "/" + "excepts_results")

    //-------------------------------------
    val excepts2 = df_usage.join(ours, df_usage("vid") === ours("vid"))
    println("except count = " + excepts2.count())

    val tmp2 = excepts2.map(row => vid_url_cid_url_cid_url_coclick_vidclick_rate( row.get(0).asInstanceOf[String], row.getAs[String](5),
      row.get(1).asInstanceOf[String],
      row.get(3).asInstanceOf[String], row.getAs[String](6),
      row.get(4).asInstanceOf[Double], row.get(7).asInstanceOf[mutable.WrappedArray[String]], row.get(8).asInstanceOf[mutable.WrappedArray[String]]))
    val result2 = tmp2.select("vid_title", "vid", "cid_dict", "cid_ours", "cid_ours_title", "cosSim", "vid_tags", "cid_tags")
      .join(cid_title_df, tmp2("cid_dict") === cid_title_df("cid"), "left")
      .select($"vid_title", $"vid", $"cid_dict", $"cid_title".as("cid_dict_title"), $"cid_ours", $"cid_ours_title", $"cosSim", $"vid_tags", $"cid_tags")

    println("show data1 result")
    result2.show
    return result2

//    res.append(res_str)
//    spark.sparkContext.parallelize(res).repartition(1).saveAsTextFile(outputPath + "/" + "task1" + "/" + "data")
  }

  case class vid(vid_str:String)
  def task2(spark:SparkSession, input: String, cid_title_df:Dataset[pm.cid_title], outputPath:String) : Unit = {
    val top_vid = Seq(
      "q0024hajctr",
      "z0024z2ulr4",
      "w0024jwbcqa",
      "n0024gz6uy2",
      "u0024izm1wz",
      "h0024x1jb6k",
      "m0016nwmqws",
      "y00240ny2xe",
      "e0024h5qwun",
      "m0024qjk1k9",
      "q0024c7a6yu",
      "q00247fk747",
      "l0024dvsf5c",
      "u0024ecji4c",
      "l0024si3r7q",
      "e0024dxa4jv",
      "h0024g60p3b",
      "f0024sb4i9f",
      "m00246qxzaz",
      "w0024ze3q6k",
      "h0024mdgrnz",
      "j0024j1z506",
      "g00247ewkzx",
      "n0522bogz8s",
      "w00249vvus6",
      "a00241b0mbz",
      "b0017ejsi8c",
      "o002418w1iq",
      "h0024a92ted",
      "a0024ph198r",
      "z0024bbbcsb",
      "e0024b0wjjt",
      "k00240e220b",
      "i0024wtnmoi",
      "m0024h4jr0a",
      "g00241dnbi5",
      "n0024ffk0kc",
      "t0024fp58h9",
      "x0024firtbh",
      "m0024v5gxfp",
      "f0024apf5pd",
      "d0024qgaiyl",
      "c0024m5kzn8",
      "f0024f0kzvd",
      "c002458ouzt",
      "e002478e5oj",
      "q0024xvau05",
      "e002426leiu",
      "a0024qttc0m",
      "j00245ehx47",
      "s00240rn01g",
      "y00247q6ta9",
      "f00249yntef",
      "f0024eeqmsi",
      "x0024p0q44a",
      "c0024ef2z99",
      "h053372xbmf",
      "b0024m9he86",
      "n0024mvc1b8",
      "m0024gftrf1",
      "z0024tt7oox",
      "y00247653tp",
      "v002484dbke",
      "h002446mgco",
      "i0024j9egbz",
      "n0021kwlsu6",
      "h0024e993js",
      "l002403g45c",
      "p002486thkn",
      "c00248syj3f",
      "u0024oorwua",
      "w0024tn4ysc",
      "p0024g6gjmx",
      "u002499cn6v",
      "h0024rtc3q4",
      "t0024r5xm62",
      "r0024s9oeeg",
      "e0024pvzdqy",
      "s0024wb8po6",
      "m0024zkb5r8",
      "w0024xkm4tl",
      "w0024t0yp46",
      "u0024ndou46",
      "w0532r5sfmd",
      "e0024rd470f",
      "w0024xenxfh",
      "e00246qv3f0",
      "z00245tosno",
      "g0024dylnou",
      "l0024aorbzj",
      "l0024b1h01m",
      "o0024y1hafz",
      "w002426jx6f",
      "y0024413wbu",
      "q0024t41reg",
      "r0024kpy0tn",
      "p00245ifbls",
      "a002478hih0",
      "x0024aexxsc",
      "c0020ag9xjh",
      "j0018ui57c2",
      "m00240tn540",
      "c0024upve8l",
      "b0524vl5t63",
      "n0024k5tsf3",
      "k0024w49g21",
      "e0024e1ourk",
      "q0024qj9ql4",
      "w00244e29q5",
      "h0024bsyvhr",
      "i0015wdu8vl",
      "m0024jose0l",
      "h0024sudliy",
      "a0022lu2ibo",
      "u0024zhrz9z",
      "k0024r3vbpq",
      "o00242g502i",
      "g0024mnwup2",
      "j00240w6res",
      "t00220u8fu9",
      "f0024ujhxzr",
      "o0024qrofyw",
      "o00248t9zfn",
      "a0024m6ui0c",
      "k0024voawah",
      "v053011hs50",
      "l0024zhu0bt",
      "w00247n5djb",
      "x0024xiafo0",
      "o0024mh5lib",
      "m0024w68yf3",
      "h00249bqqtw",
      "y0024cl4myo",
      "l00246a64tc",
      "x0024dk5joj",
      "d0022fbmgl2",
      "m0024386kq0",
      "x0024j33cz5",
      "b0024ik5642",
      "r0024egja6u",
      "y00151da9v1",
      "n0024ujzwbz",
      "u0024medl5z",
      "d00241dhbta",
      "l0024dyrrxh",
      "i00248zf8p4",
      "o00249538f7",
      "m0024g5c5az",
      "d002472d70f",
      "u0015x13q20",
      "m00241dytmu",
      "m00242vsmzn",
      "w0024b99fh5",
      "w0357mcul38",
      "n0020yrnly7",
      "k0023nwsf2i",
      "8DqIMLyIjei",
      "z00242bsn86",
      "h0024jcp77g",
      "p05319yyebg",
      "j00248ccbxa",
      "c0024b4up0p",
      "w00202az26u",
      "f0024m0tupk",
      "k0024aqxyho",
      "a0024mjyms6",
      "q0024ic8jqc",
      "o0024wxlv37",
      "x00247c4sf8",
      "w0532ozcgdl",
      "f0024950mo1",
      "v0024yibg34",
      "y00249zqlus",
      "r0012pb710s",
      "i0023heopmf",
      "d0024qxbf1p",
      "y0024qkhz0y",
      "u00242t3zkb",
      "y0024zgpcq6",
      "e0023ppquxm",
      "m0024oship1",
      "p00240hlag5",
      "u0024kujwse",
      "s0024tbdyqy",
      "e0532fkvnc0",
      "l0534g24jqi",
      "j0024bnuy00",
      "m0024gmayan",
      "e002478baja",
      "p0024m49pyb",
      "k0389cmfzmr",
      "e0024d8adt2",
      "h00247wqyg4",
      "f0024kxha3n",
      "x0024uycu16",
      "w0533tabsu5",
      "h0024ba4f29",
      "u0024c2v6wi",
      "o00245wmn27",
      "b002426ezux"
    )
    val rdd : RDD[String] = spark.sparkContext.parallelize(top_vid)
    import spark.implicits._
    val vid_df = rdd.map(line => vid(line)).toDF()


    val ours = spark.read.parquet(input)
    println("show input data, length: " + ours.count())

    val join_res = vid_df.join(ours, vid_df("vid_str") === ours("vid"), "left")
    join_res.show(200)

    join_res.repartition(1).write.mode("overwrite").parquet(outputPath + "/" + "task2" + "/" + "data")


  }

  def get_click_data_from_3932(spark: SparkSession) : Unit = {
    import spark.implicits._
    val bucket_id_all = Array("2000", "50400", "50500", "50600", "50700")

    for (date <-   (20170724 to 20170731) ++ (20170801 to 20170818)) {
      val data_source_3932 = spark.read.parquet("/data/parquet/cron-3932/dt=" + date)
      val kv_data3932 = data_source_3932.select($"kv").filter(line=>line.getAs[String](0).contains("detail_personal_recmd_feed_item"))



      kv_data3932.createOrReplaceTempView("temp_db3932")
      val data_res_all = kv_data3932
        .sqlContext
        .sql("select t.guid, t.reportParams, t.reportKey, t.vid, t.plat_bucketid, t.cid from temp_db3932 a lateral view json_tuple(kv, \"guid\", \"reportParams\", \"reportKey\", \"vid\", \"plat_bucketid\", \"cid\") t as guid, reportParams, reportKey, vid, plat_bucketid, cid")
          .filter($"vid".isNotNull && $"vid" =!= "")
        .cache
      for (bucket_id <- bucket_id_all) {
        val data_res = data_res_all.filter($"plat_bucketid" === bucket_id)


        var algorithm_num = 0
//        if (bucket_id == "2000") {
//          algorithm_num = 0
//        } else {
//          algorithm_num = data_res
//            .filter($"reportParams".contains("algid=search_pull_long")).count.toInt
//        }
        val total_num = data_res
          .count.toInt

        val idx_num = data_res.filter($"reportParams".contains("&video_idx=0&")).count.toInt

        printf("at bucket: %s, at date: %d, total_num: %d, algorithm_num: %d, idx_0_num: %d\n", bucket_id, date, total_num, algorithm_num, idx_num)
      }
      printf("\n")
      data_res_all.unpersist()
    }
  }


  def main(args: Array[String]): Unit = {

    val inputPath = args(0)
    val inputPath_vid = args(1)
    val inputPath_cid = args(2)
    val outputPath = args(3)
    val spark = SparkSession.builder()
                            .appName("pm")
                            .getOrCreate()
    import spark.implicits._
    //create vid dict
    println("-------------------------------- create vid dict --------------------------------------------------")
    val rdd1 = spark.sparkContext.textFile(inputPath_vid, 200)
                                  .map(line => line.split("\t", -1))
                                  .filter(_.length >= 124)
                                  .map(arr => vid_title_cid(arr(1), arr(49), arr(69))) // relative_covers: String '相关剧ID', 70 对比推荐逻辑
                                  .toDF()

    //寻找第一个不为空的cid
    val df_usage = rdd1.filter(line => !(line.get(0).equals("") || line.get(2).equals(""))).map(line => {
      val vid = line.get(0).asInstanceOf[String]
      val cid = line.get(2).asInstanceOf[String]
      val arr = cid.split("\\+", -1).filter(!_.equals(""))
      if (arr.length == 0)
        vid_cid(vid, "")
      else
        vid_cid(vid, arr(0))
    }).filter(!$"cid".equalTo(""))
    //vid - title dict
    println("------------------> count = " + df_usage.count())

    println("------------------------------- create cid dict -------------------------------------------------------")
    val cid_title_df = spark.sparkContext.textFile(inputPath_cid, 200)
                                          .map(line => line.split("\t", -1))
                                          .filter(_.length >= 136)
                                          .map( arr => cid_title(arr(1),arr(53))) //1 : cid  53: b_title:标题
                                          .toDF().as[cid_title]
    println("----------------------------  case 1  -----------------------------------------------------")
      val result = task1(spark, inputPath, df_usage, outputPath,  cid_title_df)

    println("----------------------------- case 2 ------------------------------------------------------")
    task2(spark, inputPath, cid_title_df,outputPath)



    println("############################### is Done #########################################")
  }
}
