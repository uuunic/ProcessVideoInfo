package Algorithm


import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Encoder, Encoders, Row, SparkSession}


import scala.collection.mutable.ArrayBuffer
import org.apache.spark.mllib.feature.{HashingTF, IDF, Normalizer}
import org.apache.spark.mllib.linalg.{SparseVector => SV}
import breeze.linalg.{SparseVector, norm}


/**
  * Created by baronfeng on 2017/7/14.
  */
case class video_tag_infos(vid : String,

                          tags: Array[String],
                           tag_size: Int

                          )
case class vid_vid_sim(vid1:String, vid2:String, cosSim:Double)
case class vid_tag(vid:String, tags:Array[String], tag_size:Int)

object TagProcess {

  def get_tf_idf(spark: SparkSession, inputPath:String, outputPath:String) : String = {
    val db_table_name = "table_video_info"
    val rdd: RDD[video_tag_infos] = spark.sparkContext.textFile(inputPath)
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
      video_tag_infos(
        arr(1),
        tags.toArray.sorted,
        tags.length
        //arr(107)
      )
    })
      .filter(_.tag_size > 1
      )

    // 到这里为止为 vid tags tag_size
   

    val hashingTF = new HashingTF(Math.pow(2, 22).toInt)
    val newSTF = rdd.map {
      video_tag_infos =>
        val tf = hashingTF.transform(video_tag_infos.tags)
        val vid1 = video_tag_infos.vid
        (vid1, tf)
    }

    val idf = new IDF().fit(newSTF.values)
    //转换为idf

    val videoIDF = newSTF.mapValues(v=>idf.transform(v))
    import spark.implicits._

    val temp_file_path = outputPath + "/temp_file/"

    videoIDF.toDF.write.mode("overwrite").parquet(temp_file_path)
    return temp_file_path
  }



  case class table(vid1: String, vid2 : String, value: Double)
  case class vid_sv(vid:String, sv:SV)


  case class index_vid_sv(index:Long, vid:String, sv:SV)
  case class vid_tags(vid :String, tags_vector:SV)
  case class vid_vid_value(vid1: String, vid2 : String, value: Double)


  def get_cartesian2(spark:SparkSession, inputPath:String, outputPath:String) : Unit = {

    import spark.implicits._
    //为我们的rdd产生一列id 作为行号
    val parquet  = spark.read.parquet(inputPath).rdd.cache()
    //使用分布式矩阵IndexedRow的方法帮助我们计算相似度

    val simRDD = parquet.cartesian(parquet)

      .map(line=>{
        val vec1 = line._1.get(1).asInstanceOf[SV]
        val vec2 = line._2.get(1).asInstanceOf[SV]
        val vec1_trans = new SparseVector[Double](vec1.indices, vec1.values, vec1.size)
        val vec2_trans = new SparseVector[Double](vec2.indices, vec2.values, vec2.size)
        val cosSim = vec1_trans.dot(vec2_trans) / (norm(vec1_trans) * norm(vec2_trans))
        vid_vid_sim(line._1.get(0).asInstanceOf[String], line._2.get(0).asInstanceOf[String], cosSim)
      })
      .filter(_.cosSim>0.5).toDF

    simRDD.write.mode("overwrite").parquet(outputPath+"/result")


  }
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


    val vid_idf_path = args(1) + "/temp_file/"
    val cid_info_path = args(0)
    val outputPath = args(1)
    println("------------------[begin]-----------------")
    process_vid_cid_recomm(spark, vid_idf_path, cid_info_path, outputPath)
    //get_cid_vid(spark, cid_info_path)
    println("------------------[done]-----------------")
  }
}
