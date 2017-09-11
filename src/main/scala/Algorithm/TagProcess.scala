package Algorithm


import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import org.apache.spark.sql.Encoders

import scala.collection.mutable.ArrayBuffer
import org.apache.spark.ml.feature.{HashingTF, IDF, Normalizer}
import org.apache.spark.ml.linalg.{SparseVector => SV}
import breeze.linalg.{SparseVector, norm}
import breeze.linalg._
import org.apache.spark.sql.expressions.Aggregator

import scala.collection.mutable



/**
  * Created by baronfeng on 2017/7/14.
  */

case class vid_vid_sim(vid1:String, vid2:String, cosSim:Double)
case class xid_tags(xid:String, tags:Array[String], tag_weight:Double, data_type:Int, key_name:Int) // cid or vid
case class xid_tag_sv(xid:String, sv:SV, weight: Double)
  case class xid_tag_sparsevector(xid:String, sparsevector:SparseVector[Double])


//object agg_vector extends Aggregator[xid_tag_sv, xid_tag_sparsevector, xid_tag_sparsevector] {
//  // A zero value for this aggregation. Should satisfy the property that any b + zero = b
//  def zero: xid_tag_sv = xid_tag_sv("", new SV(Math.pow(2,22).toInt, Array.empty[Int], Array.empty[Double]), 0.0)
//  // Combine two values to produce a new value. For performance, the function may modify `buffer`
//  // and return it instead of constructing a new object
//  def reduce(buffer: xid_tag_sparsevector, new_one: xid_tag_sv): xid_tag_sparsevector = {
//    val sv = new_one.sv
//    val sp_sv = new SparseVector[Double](sv.indices, sv.values, sv.size)
//    xid_tag_sparsevector(new_one.xid, buffer.sparsevector + sp_sv * new_one.weight)
//  }
//  // Merge two intermediate values
//  def merge(b1: xid_tag_sparsevector, b2: xid_tag_sparsevector): xid_tag_sparsevector = {
//    xid_tag_sparsevector(b1.xid, b1.sparsevector + b2.sparsevector)
//  }
//  // Transform the output of the reduction
//  def finish(reduction: xid_tag_sparsevector): xid_tag_sparsevector = reduction
//  // Specifies the Encoder for the intermediate value type
//  def bufferEncoder: Encoder[xid_tag_sv] = Encoders.product
//  // Specifies the Encoder for the final output value type
//  def outputEncoder: Encoder[xid_tag_sparsevector] = Encoders.product
//}


case class data_cid_sv(cid:String, sv:SparseVector[Double])
object TagProcess {
  // for xid_tags.data_type
  val VID_TYPE: Int = 0
  val CID_TYPE: Int = 1
  // for xid_key_name
  val VID_TAGS:Int = 0

  val CID_TAGS: Int = 0
  val CID_LEADINGACTOR:Int = 1
  val CID_TITLE:Int = 2
  val CID_CIDTAGS:Int = 3

  def vid_tag_collect(spark: SparkSession, inputPath: String) : Dataset[xid_tags] = {
    import spark.implicits._

    println("---------[begin to collect vid tags]----------")

    val rdd = spark.sparkContext.textFile(inputPath, 200)
      .map(line => line.split("\t", -1)).filter(_.length >107).filter(line => line(59) == "4" || line(59) == "0")
      //.filter(_(57).toInt < 600)
      // 45:b_tag, 57:b_duration
      .map(arr => {
      val c_qq = arr(107)
      val tags: ArrayBuffer[String] = new ArrayBuffer[String]
      tags ++= arr(30).split("#", -1).filter(_ != "")  // '导演名称', 30
      tags ++= arr(32).split("#", -1).filter(_ != "")  // '编剧', 32
      tags ++= arr(40).split("#", -1).filter(_ != "")  // '嘉宾', 40
      tags ++= arr(45).split("#", -1).filter(_ != "")  // b_tag
      tags ++= arr(46).split("\\+", -1).filter(_ != "") // '关键词', 46
      tags ++= arr(66).split("#", -1).filter(_ != "")  // '相关明星', 66
      tags ++= arr(71).split("#", -1).filter(_ != "") // '视频明星名字', 71
      tags ++= arr(88).split("\\+", -1).filter(_ != "") // 先锋视频标签
      tags ++= arr(95).split("#", -1).filter(_ != "") // '运动员', 95
      if (arr(107) !="") tags += arr(107)  // 107 c_qq
      if (arr(75) !="") tags += arr(75) // '一级推荐分类', 75
      if (arr(76) !="") tags += arr(76) // '二级推荐分类', 76
      if (arr(67) !="") tags += arr(67) // '歌手名', 67
      if (arr(114) !="") tags += arr(114) // '游戏名', 114


      tags.distinct
      xid_tags(arr(1), tags.toArray.sorted, 1.0, VID_TYPE, VID_TAGS)
    })
      .toDS

    println("---------[collect vid tags done]----------")
    rdd
  }

  def tag_seqOp(a:Array[String], b:Array[String]) : Array[String] = {
    a ++ b
  }
  def tag_combOp(a:Array[String], b:Array[String]) : Array[String] = {
    a ++ b
  }

  def cid_tag_collect(spark:SparkSession, inputPath:String, vid_tags: Dataset[xid_tags]) : Dataset[xid_tags] = {
    import spark.implicits._
    println("---------[begin to collect cid tags]----------")
    println("cid_info_path：" + inputPath)

    val cid_source_data = spark.sparkContext.textFile(inputPath, 200)
        .map(line => line.split("\t", -1))
        .filter(_.length >= 136)
        .filter(arr => arr(61) == "0" || arr(61) == "4") //_(61)是b_cover_checkup_grade 要未上架的和在线的
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
        val distince_tags = line._2.distinct
        xid_tags(line._1, distince_tags, 1.0, CID_TYPE, CID_TAGS)
      }).toDS()

    cid_vidtags.show()

    println("cid tags collect from vids done, begin to get other useful infomations ")

    //获取其他tag，暂时不加权重


    println("collect leading actors and directors ")
    //主演和导演
    val cid_leadingactor = cid_source_data.map(line => {
      val cid = line(1)
      val leading_actor = (line(34) + "#" + line(30)).split("#").filter(!_.equals(""))
      xid_tags(cid, leading_actor, 1.0, CID_TYPE, CID_LEADINGACTOR)
    }).filter(_.tags.nonEmpty).toDS

    // title和别名
    println("collect title and alias ")
    val cid_title = cid_source_data.map(line => {
      val cid = line(1)
      val title = line(53)
      val alias = line(54).split(";").filter(!_.equals(""))
      val names = new ArrayBuffer[String]
      names ++= alias
      names += title
      xid_tags(cid, names.toArray.distinct, 1.0, CID_TYPE, CID_TITLE)
    }).filter(_.tags.nonEmpty).toDS

    // cid本身的tags
    println("collect cid own tags ")
    val cid_cidtags = cid_source_data.map(line => {
      val cid = line(1)
      val tags = line(49).split("\\+").filter(!_.equals(""))
      xid_tags(cid, tags, 1.0, CID_TYPE, CID_CIDTAGS)
    }).filter(_.tags.nonEmpty).toDS

    println("union all tags")
    println("---------[collect cid tags done]----------")
    cid_vidtags.union(cid_leadingactor).union(cid_title).union(cid_cidtags)

  }


  def xid_tf_idf(spark:SparkSession, cid_info: Dataset[xid_tags], vid_info:Dataset[xid_tags], output_path: String) : Unit = {
    import spark.implicits._



    println("---------[begin to process tf-idf]----------")
    val xid_info = vid_info.union(cid_info)
    // 到这里为止为 vid tags tag_size


    val hashingTF = new HashingTF().setInputCol("tags").setOutputCol("tag_raw_features").setNumFeatures(Math.pow(2, 22).toInt)
    val newSTF = hashingTF.transform(xid_info)

    println("tf done. begin idf")

    //    val newSTF = rdd.map {
    //      video_tag_infos =>
    //        val tf = hashingTF.transform(video_tag_infos.tags)
    //        val vid1 = video_tag_infos.vid
    //        (vid1, tf)
    //    }

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

    val cid_data = rescaledData_reread.select($"xid",$"tags",$"tag_weight", $"data_type", $"key_name", $"features").where("data_type=" + CID_TYPE)

    val cid_title = cid_data.select($"xid".alias("title_xid"), $"features" alias "title_features", $"tags" alias "title_tags").where("key_name=" + CID_TITLE)
    val cid_tags = cid_data.select($"xid".alias("tags_xid"), $"features" alias "tag_features", $"tags" alias "tag_tags").where("key_name=" + CID_TAGS)
    val cid_leaderactor = cid_data.select($"xid".alias("leaderactor_xid"), $"features" alias "leaderactor_features", $"tags" alias "leaderactor_tags").where("key_name=" + CID_LEADINGACTOR)
    val cid_cidtags = cid_data.select($"xid".alias("cidtags_xid"), $"features" as "cidtags_features", $"tags" alias "cidtags_tags").where("key_name=" + CID_CIDTAGS)

    val cid_union_res1 = cid_title.join(cid_tags, cid_title("title_xid") === cid_tags("tags_xid"), "left")
    cid_union_res1.show()
    val cid_union_res2 = cid_union_res1.join(cid_leaderactor, cid_title("title_xid") === cid_leaderactor("leaderactor_xid"), "left")
      .join(cid_cidtags, cid_title("title_xid") === cid_cidtags("cidtags_xid"), "left")

    println("cid union done. cid_union schema:")
    cid_union_res2.printSchema()
    cid_union_res2.show()
    println("begin to write cid_union to: " + output_path + "/cid_union")
    cid_union_res2.write.mode("overwrite").parquet(output_path + "/cid_union")



  }

  def xid_get_cos(spark: SparkSession, cid_union_input_path:String, vid_union_input_path: String, output_path: String) : Unit = {
    import spark.implicits._
    val cid_union_reread = spark.read.parquet(cid_union_input_path)
    println("write cid_union and reread done. From: " + cid_union_input_path)
    val cid_union = cid_union_reread.select($"title_xid", $"title_features", $"tag_features", $"leaderactor_features", $"cidtags_features",
      $"title_tags", $"tag_tags", $"leaderactor_tags", $"cidtags_tags"
    )
      .map(line=>{
        val title_xid = line.getAs[String](0)

        val title_features = line.getAs[SV](1)
        val tag_features = line.getAs[SV](2)
        val leaderactor_features = line.getAs[SV](3)
        val cidtags_features = line.getAs[SV](4)

        val title_tags = line.getAs[mutable.WrappedArray[String]](5)
        val tag_tags = line.getAs[mutable.WrappedArray[String]](6)
        val leaderactor_tags = line.getAs[mutable.WrappedArray[String]](7)
        val cidtags_tags = line.getAs[mutable.WrappedArray[String]](8)
        (title_xid, title_features, tag_features, leaderactor_features, cidtags_features, title_tags, tag_tags, leaderactor_tags, cidtags_tags)

      })
    cid_union.show()


    println("---------[begin to add cid vectors in title/tags/leaderactor/cidtags/...]----------")



    val cid_result_data = cid_union.rdd.map(line => {
      val cid = line._1

      val title_feature = line._2 //title_feature
      val title = new SparseVector[Double](title_feature.indices, title_feature.values, title_feature.size)

      val temp = title
      if (line._3 != null) {
        val tags_feature = line._3
        val tags = new SparseVector[Double](tags_feature.indices, tags_feature.values, tags_feature.size)
        temp += tags
      }

      if (line._4 != null) {
        val leaderactor_feature = line._4
        val leaderactor = new SparseVector[Double](leaderactor_feature.indices, leaderactor_feature.values, leaderactor_feature.size)
        temp += leaderactor
      }
      if (line._5 != null) {
        val cidtags_feature = line._5
        val cidtags = new SparseVector[Double](cidtags_feature.indices, cidtags_feature.values, cidtags_feature.size)
        temp += cidtags
      }

      val all_tags = new ArrayBuffer[String]
      if (line._6 != null) {
        val title_tags = line._6
        all_tags ++= title_tags
      }
      if (line._7 != null) {
        val tag_tags = line._7
        all_tags ++= tag_tags
      }
      if (line._8 != null) {
        val leaderactor_tags = line._8
        all_tags ++= leaderactor_tags
      }
      if (line._9 != null) {
        val cidtags_tags = line._9
        all_tags ++= cidtags_tags
      }
      (cid, temp, all_tags.toArray)

    })
    cid_result_data.take(10).foreach(println)

    println("broadcast cid collect")
    val broadcast_cid_sv = spark.sparkContext.broadcast(cid_result_data.collect())
    println("broadcast cid collect done, num: " + cid_result_data.count())

    println("---------[begin to process cosine]----------")

    val rescaledData_reread = spark.read.parquet(vid_union_input_path)
    println("write rescaled data and reread done. From: " + vid_union_input_path)

    val vid_data = rescaledData_reread.select($"xid", $"features", $"tags").where("data_type=" + VID_TYPE)
        .filter(!_.isNullAt(2))

    val docSims = vid_data.repartition(200).cache().map( line => {

      val vid1 = line.getAs[String](0)
      val sv1 = line.getAs[SV](1)
      val vid_tags = line.getAs[mutable.WrappedArray[String]](2)
      //构建向量1
      val bsv1 = new SparseVector[Double](sv1.indices, sv1.values, sv1.size)
      //取相似度最大的前n个

      val ret = broadcast_cid_sv.value
        .map(line2 => {
          val cid = line2._1
          val bsv2 = line2._2
          val cid_tags = line2._3
          //构建向量2
          //val bsv2 = new SparseVector[Double](sv2.indices, sv2.values, sv2.size)
          //计算两向量点乘除以两向量范数得到向量余弦值
          val cosSim = bsv1.dot(bsv2) / (norm(bsv1) * norm(bsv2))

          (cid, cosSim, cid_tags)
        }).maxBy(_._2)

      (vid1, ret, vid_tags)

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


  def get_tf_idf(spark: SparkSession, inputPath:String, outputPath:String) : String = {
    import spark.implicits._
    val rdd = spark.sparkContext.textFile(inputPath)
      .map(line => line.split("\t", -1)).filter(_.length >107).filter(_(59) == "4")
      //.filter(_(57).toInt < 600)
      // 45:b_tag, 57:b_duration
      .map(arr => {
      val c_qq = arr(107)
      val tags: ArrayBuffer[String] = new ArrayBuffer[String]
      tags ++= arr(45).split("#", -1).filter(_ != "")
      tags ++= arr(88).split("\\+", -1).filter(_ != "")
      tags += arr(107)
      tags += arr(75)
      tags += arr(76)
      tags.distinct
      xid_tags(
        arr(1),
        tags.toArray.sorted,
        1.0,
        VID_TYPE,
        //arr(107)
        VID_TAGS
      )
    })
      .toDF

    // 到这里为止为 vid tags tag_size

    val hashingTF = new HashingTF().setInputCol("tags").setOutputCol("tag_raw_features").setNumFeatures(Math.pow(2, 22).toInt)
    val newSTF = hashingTF.transform(rdd)


//    val newSTF = rdd.map {
//      video_tag_infos =>
//        val tf = hashingTF.transform(video_tag_infos.tags)
//        val vid1 = video_tag_infos.vid
//        (vid1, tf)
//    }

    //下面计算IDF的值
    val idf = new IDF().setInputCol("tag_raw_features").setOutputCol("features")
    val idfModel = idf.fit(newSTF)

    // vid tags tag_size tag_raw_features features
    val rescaledData = idfModel.transform(newSTF)

    //val idf = new IDF().fit(newSTF.values)
    //转换为idf
    val temp_file_path = outputPath + "/temp_file/"
    rescaledData.write.mode("overwrite").parquet(temp_file_path)
    return temp_file_path
  }



  case class table(vid1: String, vid2 : String, value: Double)
  case class vid_sv(vid:String, sv:SV)


  case class index_vid_sv(index:Long, vid:String, sv:SV)
  case class vid_tags(vid :String, tags_vector:SV)
  case class vid_vid_value(vid1: String, vid2 : String, value: Double)


  case class cid_vid(cid:String, vid:String)
  case class vid_vector(vid:String, vec:SV)

  case class cid_sparsevector(cid:String, sparsevector:SparseVector[Double])
  def seqOp(a:Array[SV], b:Array[SV]) : Array[SV] = {
    a ++ b
  }
  def combOp(a:Array[SV], b:Array[SV]) : Array[SV] = {
    a ++ b
  }

  def get_cid_sv(spark: SparkSession, cid_rdd: RDD[cid_vid], vid_rdd: RDD[(String, SV)]) : RDD[(String, SparseVector[Double])] = {
    import spark.implicits._
    val df_cid = cid_rdd.toDF
    println("in get_cid_sv: show df_cid")
    df_cid.show()

    val df_vid = vid_rdd.map(line => vid_vector(line._1, line._2)).toDF
    println("in get_cid_sv: show df_vid")
    df_vid.show()

    val df_cid_vid_sv_union = df_cid.join(df_vid, df_cid("vid") === df_vid("vid"), "inner")  // 如果用left的话，有很多下架的vid也被推了
    println("in get_cid_sv: show df_cid_vid_sv_union")
    df_cid_vid_sv_union.show(100)

    df_cid_vid_sv_union.createOrReplaceTempView("union_db")
    val df_cid_sv_union = spark.sql("select cid, vec from union_db")
    println("in get_cid_sv: show df_cid_sv_union, cid_vec without distinct num: " + df_cid_sv_union.count())
    df_cid_sv_union.show(100)


    val df_cid_sv_result = df_cid_sv_union.map(line=>{
      val cid = line.get(0).asInstanceOf[String]
      val sv = line.get(1).asInstanceOf[SV]
      (cid, Array(sv))
    }).rdd

    val result =  df_cid_sv_result.aggregateByKey(Array.empty[SV])(seqOp, combOp)
      .map(line=>{
        val res = new SparseVector[Double](Array.emptyIntArray, Array.emptyDoubleArray, Math.pow(2, 22).toInt)
        for (sv <- line._2)
          {
            val temp = new SparseVector[Double](sv.indices, sv.values, sv.size)
            res += temp
          }
        (line._1, res)
      })


//    val df_cid_sv_result = df_cid_sv_union.map(line => (line.get(0).asInstanceOf[String], line.get(1)))
//        .rdd
//      .reduceByKey((vec1, vec2) => {
//        val sv1 = vec1.asInstanceOf[SV]
//        val sv2 = vec2.asInstanceOf[SV]
//        val bsv1 = new SparseVector[Double](sv1.indices, sv1.values, sv1.size)
//        val bsv2 = new SparseVector[Double](sv2.indices, sv2.values, sv2.size)
//        val a = (bsv1 + bsv2)
//      })
    println("in get_cid_sv: show cid_sv_result. num: " + result.count())
    result.take(10).foreach(println)

    return result
  }

  case class cid_vidstr(cid:String, vidstr:String)

  def get_cid_vid(spark: SparkSession, cid_info_path: String): RDD[cid_vid] = {
    val db_table_name = "table_cid_info"
    import spark.implicits._
    println("cid_info_path：" + cid_info_path)


    val rdd_usage : RDD[cid_vidstr] = spark.sparkContext.textFile(cid_info_path, 200)
      .map(line => line.split("\t", -1))
      .filter(_.length >= 136)
      .filter(arr => arr(61) == "0" || arr(61) == "4") //_(61)是b_cover_checkup_grade 要未上架的和在线的
      .filter(arr => (!arr(84).equals("")) || (!arr(119).equals(""))) //119为碎视频列表，84为长视频列表，我这里一并做一个过滤
      .filter(arr => arr(3).contains("正片"))
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

    println("in get_cid_vid: show rdd_usage, cid number: " + rdd_usage.map(line => line.cid).distinct().count())
    rdd_usage.toDF.show()


    val df_usage = rdd_usage.toDF.select("cid", "vidstr").explode("vidstr", "vid") {
      line:String => line.split("#", -1).filter(!_.equals(""))
    }
      .select("cid", "vid").map(line => cid_vid(line.get(0).asInstanceOf[String], line.get(1).asInstanceOf[String]))

    println("in get_cid_rdd: show cid_sv_result, cid number: " + rdd_usage.count() + " cid_vid number: " + df_usage.count() )
    df_usage.show()
    return df_usage.rdd
  }

  def process_vid_cid_recomm(spark:SparkSession, vid_idf_input:String, cid_info_input:String, output:String) : Int = {
    import spark.implicits._
    val videoIDF : RDD[(String, SV)] = spark.read.parquet(vid_idf_input).map(line=>
      (line.get(0).asInstanceOf[String], line.get(1).asInstanceOf[SV]))
      .rdd.repartition(200)

    println("idf transform done! broadcast done! show video_idf: ")
    videoIDF.take(10).foreach(println)

    val cid_vid_rdd = get_cid_vid(spark, cid_info_input)

    val cid_sv_rdd = get_cid_sv(spark, cid_vid_rdd, videoIDF)

    val broadcast_cid_sv = spark.sparkContext.broadcast(cid_sv_rdd.collect())
    println("show video_idf: ")

    val docSims = videoIDF.map( line => {
      val vid1 = line._1
      val sv1 = line._2.asInstanceOf[SV]
      //构建向量1
      val bsv1 = new SparseVector[Double](sv1.indices, sv1.values, sv1.size)
      //取相似度最大的前n个

      val ret = broadcast_cid_sv.value
        .map(line2 => {
          val cid = line2._1
          val bsv2 = line2._2.asInstanceOf[SparseVector[Double]]
          //构建向量2
          //val bsv2 = new SparseVector[Double](sv2.indices, sv2.values, sv2.size)
          //计算两向量点乘除以两向量范数得到向量余弦值
          val cosSim = bsv1.dot(bsv2) / (norm(bsv1) * norm(bsv2))

          (cid, cosSim)
        }).filter(_._2 > 0.1).sortWith(_._2>_._2).take(11)

      (vid1, ret)

    })

    println("print docsims schema")


    val simRdd_temp= docSims.toDF

    simRdd_temp.printSchema()
    val simRdd = simRdd_temp.orderBy($"_1")

    simRdd.write.mode("overwrite").parquet(output)

    return 0
  }


  def process_video_info(spark: SparkSession, inputPath:String, outputPath:String) : Int = {
    import spark.implicits._
    val videoIDF : RDD[(String, SV)] = spark.read.parquet(inputPath).map(line=>
      (line.get(0).asInstanceOf[String], line.get(1).asInstanceOf[SV]))
      .rdd
      .repartition(200) //(new MyOrdering[(String, SV)])


    println("idf transform done! broadcast done! show video_idf: ")
    videoIDF.take(10).foreach(println)

    println("test broadcastIDF: ")
    val broadcastIDF = spark.read.parquet(inputPath).sample(false, 0.1).collect()


    val docSims = videoIDF.map( line => {
      val vid1 = line._1
      val sv1 = line._2.asInstanceOf[SV]
      //构建向量1
      val bsv1 = new SparseVector[Double](sv1.indices, sv1.values, sv1.size)
      //取相似度最大的前n个

      val ret = broadcastIDF.filter(bline => {
        val sv2 = bline.get(1).asInstanceOf[SV]
        //构建向量2
        val bsv2 = new SparseVector[Double](sv2.indices, sv2.values, sv2.size)
        //计算两向量点乘除以两向量范数得到向量余弦值
        val cosSim = bsv1.dot(bsv2)
        if (cosSim == 0.0)
          false
        else
          true
      })
        .map(line2 => {
        val vid2 = line2.get(0).asInstanceOf[String]
        val sv2 = line2.get(1).asInstanceOf[SV]
        //构建向量2
        val bsv2 = new SparseVector[Double](sv2.indices, sv2.values, sv2.size)
        //计算两向量点乘除以两向量范数得到向量余弦值
        val cosSim = bsv1.dot(bsv2) / (norm(bsv1) * norm(bsv2))

        (vid2, cosSim)
      }).filter(_._2 > 0.1).sortWith(_._2>_._2).take(11)

      (vid1, ret)

    })

    //每篇文章相似并排序，取最相似的前10个
    println("print docsims schema")


    val simRdd_temp= docSims.toDF


    simRdd_temp.printSchema()
    val simRdd = simRdd_temp.orderBy($"_1")

    simRdd.write.mode("overwrite").parquet(outputPath)


    return 0
  }




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
