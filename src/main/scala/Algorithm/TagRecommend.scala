package Algorithm


import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import org.apache.spark.sql.Encoders

import scala.collection.mutable.ArrayBuffer
import org.apache.spark.ml.feature.{HashingTF, IDF, Normalizer}
import org.apache.spark.ml.linalg.{SparseVector => SV}
import breeze.linalg.{SparseVector, norm}
import breeze.linalg._
import com.huaban.analysis.jieba.{JiebaSegmenter, SegToken}
import com.huaban.analysis.jieba.JiebaSegmenter.SegMode
import org.apache.spark.sql.expressions.Aggregator

import scala.collection.mutable



/**
  * Created by baronfeng on 2017/8/14.
  */


case class xid_tags_new(xid:String, tags:Array[String], title:String, tag_weight:Double, data_type:Int, key_name:Int) // cid or vid


object TagRecommend {
  // for xid_tags.data_type
  val VID_TYPE: Int = 0
  val CID_TYPE: Int = 1
  // for xid_key_name
  val VID_TAGS:Int = 0

  val CID_TAGS: Int = 0
  val CID_LEADINGACTOR:Int = 1
  val CID_TITLE:Int = 2
  val CID_CIDTAGS:Int = 3

  //分词工具
  val segmenter = new JiebaSegmenter

  case class LineInfo(index:Int, weight:Double, split_str:String, is_distinct:Boolean)
  // cid col name : col_index, weight, split_str: if "": none, is_distinct
  val cid_useful_col : Map[String, LineInfo] = Map(
    "cid" -> LineInfo(1, 1.0, "", false),
    "leading_actor" -> LineInfo(34, 1.0, "#", false),
    "director" -> LineInfo(30, 1.0, "#", false),
    "title" -> LineInfo(53, 1.0, "", false),
    "tags" -> LineInfo(49, 1.0, "\\+", true),
    "keywords" -> LineInfo(50, 1.0, "\\+", true),
    "stars" -> LineInfo(85, 1.0, "#", false)
  )

  val vid_useful_col : Map[String, LineInfo] = Map(
    "pioneer_tag" -> LineInfo(88, 1.0, "\\+", true),
    "tags" -> LineInfo(45, 1.0, "#", true),
    "director" -> LineInfo(30, 1.0, "#", false),
    "writer" -> LineInfo(32, 1.0, "#", false),
    "guests" -> LineInfo(40, 1.0, "#", false),
    "keywords" -> LineInfo(46, 1.0, "\\+", true),
    "relative_stars" -> LineInfo(66, 1.0, "#", false),
    "stars_name" -> LineInfo(71, 1.0, "#", false),
    "sportsman" -> LineInfo(95, 1.0, "#", false),
    "first_recommand" -> LineInfo(75, 1.0, "", false),
    "sec_recommand" -> LineInfo(76, 1.0, "", false),
    "singer_name" -> LineInfo(67, 1.0, "", false),
    "game_name" -> LineInfo(114, 1.0, "", false),
    "title" -> LineInfo(49, 1.0, "", false)
  )

  def get_tag_from_data_line(vid_line: Array[String], useful_col:Map[String, LineInfo]) : Array[String] = {
    val tags_result = new ArrayBuffer[String]
    val tags_distinct = new ArrayBuffer[String]
    for((key, line_info) <- useful_col){
      if(line_info.split_str == ""){
        tags_result += vid_line(line_info.index)
      }else if (line_info.is_distinct){
        tags_distinct ++= vid_line(line_info.index).split(line_info.split_str, -1)
      }else{
        tags_result ++= vid_line(line_info.index).split(line_info.split_str, -1)
      }
    }
    tags_result ++= tags_result.filter(!_.equals("")).distinct
    tags_result.filter(!_.equals("")).toArray
  }


  /**
    * 从标题里获取tag，目前使用jieba分词
    * */
  def get_tag_from_title(title: String) : Array[String] = {
    if (title == null)
      return null
    val title_tags: ArrayBuffer[String] = new ArrayBuffer[String]
    val sig_data = segmenter.process(title, SegMode.SEARCH).toArray.map(line => line.asInstanceOf[SegToken].word).filter(!_.matches("\\pP")).filter(!_.equals(""))
    title_tags ++= sig_data.filter(_.size > 1)
    //为电视剧《楚乔传_01》这种切词
    val regex_str = """_\d+$""".r
    if (regex_str.findFirstMatchIn(title).isDefined)
      title_tags += title.split("_",-1)(0)
    else
      title_tags += title

    return title_tags.toArray

  }

  def vid_tag_collect(spark: SparkSession, inputPath: String) : Dataset[xid_tags_new] = {
    import spark.implicits._

    println("---------[begin to collect vid tags]----------")

    val rdd = spark.sparkContext.textFile(inputPath, 200)
      .map(line => line.split("\t", -1)).filter(_.length >107).filter(line => line(59) == "4")  // line(59) b_state
      //.filter(_(57).toInt < 600)
      // 45:b_tag, 57:b_duration
      .map(arr => {


      val tags: ArrayBuffer[String] = new ArrayBuffer[String]
      // b_tag + 先锋视频标签
      val dist_tag = (arr(88).split("\\+", -1) ++ arr(45).split("#", -1)).distinct
      tags ++= dist_tag

      tags ++= arr(30).split("#", -1).filter(_ != "")  // '导演名称', 30
      tags ++= arr(32).split("#", -1).filter(_ != "")  // '编剧', 32
      tags ++= arr(40).split("#", -1).filter(_ != "")  // '嘉宾', 40
      //     tags ++= (arr(45) + "#" + arr(88)).split("#", -1).filter(_ != "").distinct  // b_tag + 先锋视频标签
      tags ++= arr(46).split("\\+", -1).filter(_ != "") // '关键词', 46
      tags ++= arr(66).split("#", -1).filter(_ != "")  // '相关明星', 66
      tags ++= arr(71).split("#", -1).filter(_ != "") // '视频明星名字', 71
      //     tags ++= arr(88).split("\\+", -1).filter(_ != "") // 先锋视频标签
      tags ++= arr(95).split("#", -1).filter(_ != "") // '运动员', 95
      //     if (arr(107) !="") tags += arr(107)  // 107 c_qq
      if (arr(75) !="") tags += arr(75) // '一级推荐分类', 75
      if (arr(76) !="") tags += arr(76) // '二级推荐分类', 76
      if (arr(67) !="") tags += arr(67) // '歌手名', 67
      if (arr(114) !="") tags += arr(114) // '游戏名', 114


      // tags.distinct
      val title = arr(49)
      tags ++= get_tag_from_title(title)
      xid_tags_new(arr(1), tags.toArray.filter(_ != ""), title, 1.0, VID_TYPE, VID_TAGS)
    })
      .toDS
    println("vid total tags show:")
    rdd.show

    println("---------[collect vid tags done]----------")
    rdd
  }

  def tag_seqOp(a:Array[String], b:Array[String]) : Array[String] = {
    a ++ b
  }
  def tag_combOp(a:Array[String], b:Array[String]) : Array[String] = {
    a ++ b
  }

  def cid_tag_collect(spark:SparkSession, inputPath:String, vid_tags: Dataset[xid_tags_new]) : Dataset[xid_tags_new] = {
    import spark.implicits._
    println("---------[begin to collect cid tags]----------")
    println("cid_info_path：" + inputPath)

    val cid_source_data = spark.sparkContext.textFile(inputPath, 200)
      .map(line => line.split("\t", -1))
      .filter(_.length >= 136)
      .filter(arr => arr(61) == "4") //_(61)是b_cover_checkup_grade 要未上架的和在线的
      .filter(arr => (!arr(84).equals("")) || (!arr(119).equals(""))) //119为碎视频列表，84为长视频列表，我这里一并做一个过滤
      .filter(arr => arr(3).contains("正片"))



    val rdd_usage : RDD[cid_vidstr] = cid_source_data
      .map(arr => {
        val video_ids = arr(84)
        val clips = arr(119)
        if (video_ids.equals(""))
          cid_vidstr(arr(1), clips)
        else if (clips.equals(""))
          cid_vidstr(arr(1), video_ids)
        else
          cid_vidstr(arr(1), arr(84) + "#" + arr(119))
      })

    println("in cid_tag_collect: show rdd_usage, cid number: " + rdd_usage.map(line => line.cid).distinct().count())
    rdd_usage.toDF.show()


    val df_usage = rdd_usage.toDF.select("cid", "vidstr").explode("vidstr", "vid") {
      line:String => line.split("#", -1).filter(!_.equals(""))
    }
      .select("cid", "vid").map(line => cid_vid(line.get(0).asInstanceOf[String], line.get(1).asInstanceOf[String]))

    println("in cid_tag_collect: show df_usage, cid number: " + rdd_usage.count() + " cid_vid number: " + df_usage.count() )
    df_usage.show()

    val cid_vid_tags_union = df_usage.join(vid_tags, df_usage("vid") === vid_tags("xid"), "inner")  // 如果用left的话，有很多下架的vid也被推了
    println("in cid_tag_collect: show cid_vid_tags_union")
    //   cid_vid_tags_union.show(100)

    cid_vid_tags_union.printSchema()

    cid_vid_tags_union.createOrReplaceTempView("union_db")
    val cid_tags_tagsize = spark.sql("select cid, tags from union_db")
    println("in cid_tag_collect: show cid_tags_tagsize,  without distinct num: " + cid_tags_tagsize.count())
    //  cid_tags_tagsize.show(100)
    cid_tags_tagsize.printSchema()
    cid_tags_tagsize.show()

    // 至此为止， cid|tags已经获取全了,还需要获取其他部分的tag

    val cid_vidtags = cid_tags_tagsize.map(line=>
      (line.getAs[String](0),
        line.getAs[mutable.WrappedArray[String]](1).toArray)
    )
      .rdd
      .aggregateByKey(Array.empty[String])(tag_seqOp, tag_combOp)
      .map(line => {
        // TODO 暂时去掉distinct
        val distince_tags = line._2  //.distinct
        xid_tags_new(line._1, distince_tags, "",  1.0, CID_TYPE, CID_TAGS)
      }).toDS()

    cid_vidtags.show()

    println("cid tags collect from vids done, begin to get other useful infomations ")

    //获取其他tag，暂时不加权重


    //主演和导演
    val cid_leadingactor = cid_source_data.map(line => {
      val cid = line(1)

      val res = new ArrayBuffer[String]
      val leading_actor = (line(34) + "#" + line(30)).split("#").filter(!_.equals(""))
      val title = get_tag_from_title(line(53))
      val alias = line(54).split(";").filter(!_.equals(""))
      val tags = line(49).split("\\+").filter(!_.equals(""))
      val keywords = line(50).split("\\+").filter(!_.equals(""))
      val stars = line(85).split("#").filter(!_.equals(""))

      res ++= leading_actor
      res ++= title
      res ++= alias
      res ++= tags
      res ++= keywords
      res ++= stars


      xid_tags_new(cid, res.toArray, "", 1.0, CID_TYPE, CID_LEADINGACTOR)
    }).filter(_.tags.nonEmpty).toDS


    val cid_title = spark.sparkContext.textFile(inputPath)
      .map(line => line.split("\t", -1))
      .filter(_.length >= 136)
      .filter(arr => arr(61) == "0" || arr(61) == "4") //_(61)是b_cover_checkup_grade 要未上架的和在线的
      .filter(arr => (!arr(84).equals("")) || (!arr(119).equals(""))) //119为碎视频列表，84为长视频列表，我这里一并做一个过滤
      .filter(arr => arr(3).contains("正片"))
      .map(arr => (arr(1), arr(53))).toDF

    println("print cid_title:")
    cid_title.printSchema()
    cid_title.show()

    val cid_result = cid_leadingactor.join(cid_title, cid_leadingactor("xid")  === cid_title("_1"), "left")
      .select($"xid", $"tags", $"_2", $"tag_weight", $"data_type", $"key_name")
      .map(line => {
        xid_tags_new(line.getAs[String](0), line.getAs[mutable.WrappedArray[String]](1).toArray, line.getAs[String](2),
          line.getAs[Double](3), line.getAs[Int](4), line.getAs[Int](5)
        )
      })
    println("print cid_result")
    cid_result.show()
    cid_result


  }


  def xid_tf_idf(spark:SparkSession, cid_info: Dataset[xid_tags_new], vid_info:Dataset[xid_tags_new], output_path: String) : Unit = {
    import spark.implicits._



    println("---------[begin to process tf-idf]----------")
    val xid_info = vid_info.union(cid_info)
    // 到这里为止为 vid tags tag_size


    val hashingTF = new HashingTF().setInputCol("tags").setOutputCol("tag_raw_features").setNumFeatures(Math.pow(2, 22).toInt)
    val newSTF = hashingTF.transform(xid_info)

    println("tf done. begin idf")


    //下面计算IDF的值
    val idf = new IDF().setInputCol("tag_raw_features").setOutputCol("features")
    val idfModel = idf.fit(newSTF)

    // vid tags tag_size tag_raw_features features
    val rescaledData = idfModel.transform(newSTF)
    println("idf done. begin join all cid data")

    println("begin to write rescaled data to: " + output_path + "/idf_data")
    rescaledData.write.mode("overwrite").parquet(output_path + "/idf_data")

    val rescaledData_reread = spark.read.parquet(output_path + "/idf_data")
    println("write rescaled data and reread done. From: " + output_path + "/idf_data")

    //   val vid_data = rescaledData_reread.select($"xid", $"features").where("data_type=" + VID_TYPE)

    val cid_data = rescaledData_reread.select($"xid",$"tags", $"title", $"tag_weight", $"data_type", $"key_name", $"features").where("data_type=" + CID_TYPE)


    println("cid union done. cid_union schema:")
    cid_data.printSchema()
    cid_data.show()
    println("begin to write cid_union to: " + output_path + "/cid_union")
    cid_data.write.mode("overwrite").parquet(output_path + "/cid_union")



  }

  def xid_get_cos(spark: SparkSession, cid_union_input_path:String, vid_union_input_path: String, output_path: String) : Unit = {
    import spark.implicits._
    val cid_union_reread = spark.read.parquet(cid_union_input_path)
    println("write cid_union and reread done. From: " + cid_union_input_path)
    val cid_union = cid_union_reread.select($"xid", $"features", $"tags", $"title")
      .map(line=>{
        val xid = line.getAs[String](0)

        val features = line.getAs[SV](1)

        val tags = line.getAs[mutable.WrappedArray[String]](2)
        val title = line.getAs[String](3)
        if(tags == null)
          (xid, features, null, title)
        else
          (xid, features, tags.toArray, title)

      })
    cid_union.show()


    println("---------[begin to add cid vectors in title/tags/leaderactor/cidtags/...]----------")


    println("broadcast cid collect")
    val broadcast_cid_sv = spark.sparkContext.broadcast(cid_union.collect())
    println("broadcast cid collect done, num: " + cid_union.count())

    println("---------[begin to process cosine]----------")

    val rescaledData_reread = spark.read.parquet(vid_union_input_path)
    println("write rescaled data and reread done. From: " + vid_union_input_path)

    val vid_data = rescaledData_reread.select($"xid", $"features", $"tags", $"title").where("data_type=" + VID_TYPE)
      .filter(!_.isNullAt(2))

    val docSims = vid_data.repartition(200).cache().map( line => {

      val vid1 = line.getAs[String](0)
      val sv1 = line.getAs[SV](1)
      val vid_tags = line.getAs[mutable.WrappedArray[String]](2).toArray
      val vid_title = line.getAs[String](3)
      //构建向量1
      val bsv1 = new SparseVector[Double](sv1.indices, sv1.values, sv1.size)
      //取相似度最大的前n个

      val ret = broadcast_cid_sv.value
        .map(line2 => {
          val cid = line2._1
          val sv2 = line2._2
          val cid_tags = line2._3
          val cid_title = line2._4
          //构建向量2
          val bsv2 = new SparseVector[Double](sv2.indices, sv2.values, sv2.size)
          //计算两向量点乘除以两向量范数得到向量余弦值  这里可以优化
          val cosSim = bsv1.dot(bsv2) / (norm(bsv1) * norm(bsv2))

          (cid, cosSim, cid_tags, sv2, cid_title)
        }).maxBy(_._2)


      cos_result(vid1, ret._1, ret._2, vid_title, ret._5,  vid_tags, ret._3, sv1, ret._4)

    })
    println("---------[process cosine done]----------")
    println("print docsims schema")


    val simRdd_temp= docSims.toDF


    simRdd_temp.printSchema()
    simRdd_temp.take(30).foreach(println)
    //val simRdd = simRdd_temp.orderBy($"_1")
    println("---------[begin to write to outputfile]----------")
    simRdd_temp.write.mode("overwrite").parquet(output_path + "/output_data")
    println("---------[tf-idf done. All done.]----------")
  }
  case class cos_result(vid:String, cid:String, cosSim:Double, vid_title:String, cid_title:String, vid_tags: Array[String], cid_tags: Array[String], vid_feature: SV, cid_feature: SV)

  case class table(vid1: String, vid2 : String, value: Double)
  case class vid_sv(vid:String, sv:SV)


  case class index_vid_sv(index:Long, vid:String, sv:SV)
  case class vid_tags(vid :String, tags_vector:SV)



  case class cid_vid(cid:String, vid:String)
  case class vid_vector(vid:String, vec:SV)

  case class cid_sparsevector(cid:String, sparsevector:SparseVector[Double])
  def seqOp(a:Array[SV], b:Array[SV]) : Array[SV] = {
    a ++ b
  }
  def combOp(a:Array[SV], b:Array[SV]) : Array[SV] = {
    a ++ b
  }

  case class cid_vidstr(cid:String, vidstr:String)


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


    //    val vid_idf_path = args(1) + "/temp_file/"
    //    val cid_info_path = args(0)
    //    val outputPath = args(1)

    val vid_info_path = args(0)
    val cid_info_path = args(1)
    val output_path = args(2)
    val do_prefix = args(3)  // do_prefix|undo_prefix
    val do_cos = args(4)     // do_cos | undo_cos
    println("------------------[begin]-----------------")
    //process_vid_cid_recomm(spark, vid_idf_path, cid_info_path, outputPath)

    if (do_prefix.equals("do_prefix")) {
      val vid_tags = vid_tag_collect(spark, vid_info_path)
      val cid_tags = cid_tag_collect(spark, cid_info_path, vid_tags)
      xid_tf_idf(spark, cid_tags, vid_tags, output_path)
    }

    if (do_cos.equals("do_cos")) {
      xid_get_cos(spark, output_path + "/cid_union", output_path + "/idf_data", output_path)
    }

    println("------------------[done]-----------------")
  }
}
