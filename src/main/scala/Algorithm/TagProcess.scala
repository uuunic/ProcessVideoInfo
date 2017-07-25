package Algorithm

import Algorithm.dataProcess.process_video_info
import org.apache.avro.generic.GenericData.StringType
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types.{ArrayType, StructField, StructType}

import scala.collection.mutable.ArrayBuffer
import org.apache.spark.mllib.feature.HashingTF
import org.apache.spark.mllib.feature.IDF
import org.apache.spark.mllib.linalg.{SparseVector => SV}
import breeze.linalg.{SparseVector, norm}
import org.apache.spark.storage.StorageLevel

/**
  * Created by baronfeng on 2017/7/14.
  */
case class video_tag_infos(vid : String,
           //                map_name : String,
          //                 b_tag: Array[String], // split #  45
          //                 b_title: String,
           //                b_duration: Int,
           //                first_recommand: String,
           //                second_recommand: String,
         //                  pioneer_tag: Array[String], //split: +
                          tags: Array[String],
                           tag_size: Int
                           //c_qq: String

                          )
case class vid_tag(vid:String, tags:Array[String], tag_size:Int)
class MyOrdering[T] extends Ordering[T]{
  def compare(x:T,y:T) = math.random compare math.random
}
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
        //    arr(3),
        //                arr(45).split("#", -1).filter(_ != ""),
        //    arr(49),
        //    arr(57).toInt,
        //   arr(75),
        //   arr(76),
        //               arr(88).split("\\+", -1).filter(_!=""),
        tags.toArray.sorted,
        tags.length
        //arr(107)
      )
    })
      .filter(line =>
        line.tag_size > 1
      )




    // 到这里为止为 vid tags tag_size

    val hashingTF = new HashingTF(Math.pow(2, 22).toInt)
    val newSTF = rdd.map {
      case video_tag_infos =>
        val tf = hashingTF.transform(video_tag_infos.tags)
        val vid1 = video_tag_infos.vid
        (vid1, tf)
    }
    // 减少内存占用，先释放掉
    //rdd.unpersist()

    //    newSTF.cache()
    //   newSTF.take(10).foreach(println)

    val idf = new IDF().fit(newSTF.values)
    //转换为idf

    val videoIDF = newSTF.mapValues(v=>idf.transform(v))
    import spark.implicits._

    val temp_file_path = outputPath + "/temp_file/"

    videoIDF.toDF.write.mode("overwrite").parquet(temp_file_path)
    return temp_file_path
  }

  def get_cartesian(spark:SparkSession, inputPath:String, outputPath:String) : Int = {
    import spark.implicits._
    val videoIDF  = spark.read.parquet(inputPath)
    val  videoIDF_rename = videoIDF
//      .withColumnRenamed("_1", "vid")
//      .withColumnRenamed("_2", "idf")
        .withColumn("_3", videoIDF.col("_2"))

    println("idf transform done! broadcast done! show video_idf: ")
    //  broadcastIDF.show(10)
    videoIDF_rename.show(10)
    return 0
  }


  def process_video_info(spark: SparkSession, inputPath:String, outputPath:String) : Int = {
    import spark.implicits._
    val videoIDF : RDD[(String, SV)] = spark.read.parquet(inputPath).map(line=>
      (line.get(0).asInstanceOf[String], line.get(1).asInstanceOf[SV]))
      .rdd
      .repartition(200) //(new MyOrdering[(String, SV)])


    //val broadcastIDF = spark.read.parquet(outputPath + "/temp_file/")
  //  broadcastIDF.printSchema()
  //  val broadcastIDF = spark.sparkContext.broadcast(videoIDF.collect())
    println("idf transform done! broadcast done! show video_idf: ")
  //  broadcastIDF.show(10)
    videoIDF.take(10).foreach(println)

    println("test broadcastIDF: ")
    val broadcastIDF = spark.read.parquet(inputPath).sample(false, 0.1).collect()
 //   val broadcastIDF_array = broadcastIDF.value.collect()
  //  println(broadcastIDF_temp.length + "; " + broadcastIDF_temp.length)
 //   val broadcastIDF = spark.sparkContext.broadcast(broadcastIDF_temp)

    val docSims = videoIDF.map( line => {
      val vid1 = line._1

    //  val idfs = broadcastIDF
      val sv1 = line._2.asInstanceOf[SV]
      //构建向量1
      val bsv1 = new SparseVector[Double](sv1.indices, sv1.values, sv1.size)
      //取相似度最大的前10个

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
      //.repartition(20)(new MyOrdering[(String, Array[(String, Double)])])
      //.persist(StorageLevel.MEMORY_AND_DISK)
//    docSims.take(10).foreach(println)
    //每篇文章相似并排序，取最相似的前10个
    println("print docsims schema")


    //docSims.take(10).foreach(line => println(line._2.asInstanceOf[Array[(String, Double)]]))

    //docSims.saveAsTextFile(outputPath + "/save_file")

    val simRdd_temp= docSims.toDF


  //  simRdd_temp.show(10)

    simRdd_temp.printSchema()
    val simRdd = simRdd_temp.orderBy($"_1")
    //val simRDD_broadcast = spark.sparkContext.broadcast(simRdd)
    //simRDD_broadcast.value.take(10).foreach(println)

    simRdd.write.mode("overwrite").parquet(outputPath)
/**
// 第二次读取rdd
    val rdd2: RDD[video_tag_infos] = spark.sparkContext.textFile(inputPath)
      .map(line => line.split("\t", -1)).filter(_.length >107)
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
      )
    })
      .filter(line =>
        line.tag_size > 1
      )
    val srcJoin=rdd2.map(video_tag_infos=>
      (video_tag_infos.vid, video_tag_infos.tags)
    )
  //  rdd2.unpersist()

   // srcJoin.take(10).foreach(println)

    //广播一份srcJoin
    val bSrcJoin = spark.sparkContext.broadcast(srcJoin.collect())

    //按标题输出
    val output_data = srcJoin.join(simRdd.rdd)
      .map(x=>(x._1,x._2._1,x._2._2.map(x=>(x._2,x._3)).map(x=>{
      val id=x._1
      val sim=x._2
      val  name=bSrcJoin.value.filter(x=>x._1==id).take(1).toList.mkString(",")
      name+" "+sim
    }
    )))
    println("data output process done! print top 10: ")
    output_data.take(10).foreach(x=>println(x._1+" "+x._2+" "+x._3))
    import spark.implicits._
    output_data.toDS.write.mode("overwrite").json(outputPath)
**/

    /**
    import spark.implicits._
    val video_info = rdd.toDS().cache()
    val tf = new HashingTF().setInputCol("tags").setOutputCol("rawfeatures").setNumFeatures(Math.pow(2, 24).toInt)
    val tfdata = tf.transform(video_info)
    val idf = new IDF().setInputCol("rawfeatures").setOutputCol("features").fit(tfdata)
    val idfdata= idf.transform(tfdata)
    println("idf data transform done!, begin to broadcast data")
    val broadcast_idfdata = spark.sparkContext.broadcast(idfdata.collect())



    idfdata.select("vid", "features").show(100)
    idfdata.write.mode("overwrite").json(outputPath)

**/
    /**
    val video_info2 = spark.sparkContext.broadcast(rdd)
    case class video_tag_count(vid: String, tags:Array[String], count:Int)
    case class video_recom_line(vid:String)
    val recom_db: RDD[video_recom_line] = video_info.map(info => {
      val tags_source : Array[String] = info.tags
      val tags_count : RDD[video_tag_count] = video_info2.filter(line => {
        val count : Int = (line.tags intersect  tags_source).length
        count != 0
      })
        .map(line => {
          val count : Int = (line.tags intersect  tags_source).length
          video_tag_count(line.vid, line.tags, count)
      }).sortBy(_.count)

      val arr_recom_list = tags_count.take(300)
      val arr_recom_buffer = new ArrayBuffer[Tuple3[String, Array[String], Int]]
      for (recom <- arr_recom_list){
        arr_recom_buffer += Tuple3(recom.vid, recom.tags, recom.count)
      }
      video_recom_line(info.vid)

    })
**/
 //   recom_db.saveAsTextFile(outputPath + "/rdd3")

    //val recom_df = recom_db.toDF()
   // recom_df.show(500)
    //recom_df.write.mode("overwrite").parquet(outputPath)
    //video_info.createTempView(db_table_name)

    //video_info.show(1000)

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


    val inputPath = args(0)
    val outputPath = args(1)
    println("------------------[begin]-----------------")
    //val temp_file_path = get_tf_idf(spark, inputPath, outputPath)
    //process_video_info(spark, temp_file_path, outputPath)
    val temp_file_path = outputPath + "/temp_file/"
    get_cartesian(spark, temp_file_path, outputPath)
    println("------------------[done]-----------------")
  }
}
