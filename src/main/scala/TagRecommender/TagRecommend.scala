package TagRecommender

import Utils.Hash._
import Utils.Tools.KeyValueWeight
import Utils.{Hash, TestRedisPool, Tools}
import breeze.linalg.{SparseVector, norm}
import com.huaban.analysis.jieba.JiebaSegmenter.SegMode
import com.huaban.analysis.jieba.{JiebaSegmenter, SegToken}
import org.apache.spark.ml.feature.{HashingTF, IDF}
import org.apache.spark.ml.linalg.{SparseVector => SV}
import org.apache.spark.sql._
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions._

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer




//import scala.reflect.ClassTag



/**
  * Created by baronfeng on 2017/8/14.
  */


//case class xid_tags_new(xid:String, tags:Array[String], title:String, tag_weight:Double, data_type:Int, key_name:Int) // cid or vid


object TagRecommend {
  val TAG_HASH_LENGTH: Int = Math.pow(2, 24).toInt // 目前暂定2^24为feature空间
  val STOP_WORDS_PATH: String = "/user/baronfeng/useful_dataset/stop_words.txt"

  val PIONEER_TAG_INDEX = 88  // 先锋标签的index位置
  val PIONEER_TAG_ID_INDEX = 89 //先锋标签对应的id位置

  val digit_regex = """^\d+$""".r  //纯数字 正则表达式

  val REPARTITION_NUM = 200

  //分词工具
  val segmenter = new JiebaSegmenter

  case class LineInfo(index:Int, weight:Double, split_str:String, is_distinct:Boolean)
  // cid col name : col_index, weight, split_str: if "": none, is_distinct

  case class DsVidLine(vid: String, duration: Int, tags:Array[Long], tags_source:Array[String], weight:Double, is_distinct:Boolean)


  def get_stop_words(spark: SparkSession) : Set[String] = {
    val data = spark.sparkContext.textFile(STOP_WORDS_PATH)
    data.map(_.trim).collect().toSet
  }

  // 从vid中获取tag，并按照各自的标题分组
  // input_path: args(0)
  // output_path: baronfeng/output/tag_recom_ver_1
  // return value: baronfeng/output/tag_recom_ver_1/vid_tag_collect
  def vid_tag_shuffle(spark: SparkSession, input_path: String, output_path: String) : String = {
    import spark.implicits._

    println("---------[begin to collect vid tags]----------")

    val stop_words = spark.sparkContext.broadcast(get_stop_words(spark))

    val rdd = spark.sparkContext.textFile(input_path)
      .map(line => line.split("\t", -1)).filter(_.length >116).filter(line => line(59) == "4").repartition(REPARTITION_NUM).cache()  // line(59) b_state
      //.filter(_(57).toInt < 600)
      // 45:b_tag, 57:b_duration



    for((tag_name, info) <- vid_useful_col) {
      println("begin to get tags type: " + tag_name)
      val tag_info_line = rdd.map(line => {
        val vid = line(1)
        val duration: Int = if(line(57).equals("0") || line(57).equals("")) 0 else line(57).toInt
        val data = line(info.index)
        val ret_tags = new ArrayBuffer[String]


        // 标题中的tag通过分词获得
        if (tag_name.equals("title")) {
          ret_tags ++= get_tag_from_title(data).map(_.trim)
          ret_tags.filter(!stop_words.value.contains(_))
        } else if (info.split_str.equals("") && !data.trim.equals("")) {
          ret_tags += data.trim
        } else if (info.split_str.equals("") && data.equals("")) {
          // do nothing
        } else {
          ret_tags ++= data.split(info.split_str, -1).map(_.trim).filter(!_.equals(""))

        }

        val ret_tags_without_digit = {
          if (info.index == PIONEER_TAG_ID_INDEX)
            ret_tags.filter(digit_regex.findFirstMatchIn(_).isDefined)
          else
            ret_tags.filter(digit_regex.findFirstMatchIn(_).isEmpty) // 去除标签中的各种数字
        }

        val ret_tags_strs = ret_tags_without_digit.toArray.clone()


        // 去除各种标签中的数字，这样如果是数字的话，就都是标签id了
        if(info.index != PIONEER_TAG_ID_INDEX && ret_tags_without_digit.nonEmpty){
            val pioneer_tags_line = line(PIONEER_TAG_INDEX)

            val pioneer_tags_id = line(PIONEER_TAG_ID_INDEX)
              .trim.split("\\+|#", -1)
              .filter(_.matches("""^\d+$"""))
              .map(_.toLong)
            val pioneer_tags = pioneer_tags_line.split("\\+|#", -1).filter(!_.equals(""))
            val pioneer_tags_map = pioneer_tags.zip(pioneer_tags_id).toMap
            for(i <- ret_tags_without_digit.indices) {
              val tag = ret_tags_without_digit(i)
              if(pioneer_tags_map.contains(tag)) {
                val tag_id = pioneer_tags_map(tag)

                ret_tags_without_digit.update(i, tag_id.toString)  // 使用先锋id代替tag
                // 不需要 因为直接就是数字了
                ret_tags_strs.update(i, ret_tags_strs(i) + "##" + tag_id.toString)  // 标记一下，方便后面使用
              } else {
                ret_tags_without_digit.update(i, Hash.hash_own(tag).toString)
                ret_tags_strs.update(i, ret_tags_strs(i) + "_inner")
              }
            }

        }
        DsVidLine(vid, duration, ret_tags_without_digit.map(_.toLong).toArray, ret_tags_strs, info.weight, info.is_distinct)

      })
        .filter(line => {(!line.tags.isEmpty) && (line.tags.length > 0)}).toDS


      println("get tags type: " + tag_name + " done.")
      //tag_info_line.show
      tag_info_line.write.mode("overwrite").parquet(output_path + "/vid_tag_collect/" + tag_name)

    }
    println("---------[collect vid tags done]----------")
    return output_path + "/vid_tag_collect"
  }


  /**
    * 从标题里获取tag，目前使用jieba分词
    * */
  def get_tag_from_title(title: String) : Array[String] = {
    if (title == null)
      return null
    val title_tags: ArrayBuffer[String] = new ArrayBuffer[String]
    val sig_data = segmenter.process(title, SegMode.SEARCH)
      .toArray
      .map(line => line.asInstanceOf[SegToken].word)
      .filter(!_.matches("\\pP"))
      .filter(!_.equals(""))

    title_tags ++= sig_data.filter(_.length > 1)
    //为电视剧《楚乔传_01》这种切词
    val regex_str = """_\d+$""".r
    if (regex_str.findFirstMatchIn(title).isDefined)
      title_tags += title.split("_",-1)(0)
    //else
      //title_tags += title

    title_tags.toArray

  }

  def tag_seqOp(a:Array[String], b:Array[String]) : Array[String] = {
    a ++ b
  }
  def tag_combOp(a:Array[String], b:Array[String]) : Array[String] = {
    a ++ b
  }



// features:SparseVector[Double] -> (features.index features.data features.length)  tuple3
  //case class SV_Wrapper(index:Array[Int], data:Array[Double], length: Int)
  case class vid_idf_line(vid: String,
                          duration: Int,
                          tags:Array[Long],
                          tags_source: Array[String],
                          features_index: Array[Int],
                          features_data: Array[Double]
                          //features_size: Int   = TAG_HASH_LENGTH
                         )

  // input_path: baronfeng/output/tag_recom_ver_1/vid_tag_collect
  // output_path: baronfeng/output/tag_recom_ver_1/
  // return value: baronfeng/output/tag_recom_ver_1/tf_idf_source_data/[tag_name]
  def vid_tf_idf(spark:SparkSession, vid_info_path:String, output_path: String) : String = {
    import spark.implicits._
    println("---------[begin to process tf-idf]----------")
    val hashingTF = new HashingTF().setInputCol("tags").setOutputCol("tag_raw_features").setNumFeatures(TAG_HASH_LENGTH)
    val idf = new IDF().setInputCol("tag_raw_features").setOutputCol("features")


    val tf_idf_source_output_path = output_path + "/tf_idf_source_data"

    for ((tag_name, vid_data) <- vid_useful_col) {

      val tag_data = spark.read.parquet(vid_info_path + "/" + tag_name).as[DsVidLine]

      val newSTF = hashingTF.transform(tag_data)

      println("tf " + tag_name + " done. begin idf")
      //下面计算IDF的值
      val idfModel = idf.fit(newSTF)
      val rescaledData = idfModel.transform(newSTF)
        .select($"vid", $"duration", $"tags", $"tags_source", $"weight", $"features")
        .toDF(tag_name + "_vid", tag_name + "_duration", tag_name + "_tags", tag_name + "_tags_source", tag_name + "_weight", tag_name + "_features")
      // vid tags tag_size tag_raw_features features
      rescaledData.write.mode("overwrite").parquet(tf_idf_source_output_path + "/" + tag_name)
      println("write idf source data done. type: "+ tag_name)


    }
    println("--------[idf done. begin join all vid data]--------")
    tf_idf_source_output_path

  }

  // input_path: baronfeng/output/tag_recom_ver_1/tf_idf_source_data
  // output_path: baronfeng/output/tag_recom_ver_1/
  // return value: baronfeng/output/tag_recom_ver_1/vid_idf
  def tf_idf_join(spark:SparkSession, tf_idf_source_output_path:String, output_path: String) : String = {
    // 从title开始，依次合并
    import spark.implicits._

    var vid_total:DataFrame = spark.read.parquet(tf_idf_source_output_path + "/title")
    for ((tag_name, vid_data) <- vid_useful_col if !tag_name.equals("title")) {
      //vid duration tags tags_source weight features
      val vid_data = spark.read.parquet(tf_idf_source_output_path + "/" + tag_name)
      vid_total = vid_total.join(vid_data, vid_total("title_vid") === vid_data(tag_name + "_vid"), "left")
    }

    println("put vid_total schema")
    vid_total.printSchema()
    val vid_result = vid_total.map(line => {
      val vid = line.getString(0)
      val duration = line.getInt(1)
      val tags_id = new ArrayBuffer[Long]
      val tags_source = new ArrayBuffer[String]
      var spv : SparseVector[Double] = new SparseVector[Double](new Array[Int](0), new Array[Double](0), TAG_HASH_LENGTH)
      for (i <- 0 until vid_useful_col.size) {

        //  $"vid", $"duration", $"tags", $"tags_source", $"weight", $"features"
        if (!line.isNullAt(6 * i) && !line.getAs[String](6 * i).equals("")) {
          tags_id ++= line.getAs[Seq[Long]](6 * i + 2)
          tags_source ++= line.getAs[Seq[String]](6 * i + 3)
          val weight = line.getAs[Double](6 * i + 4)
          val sv = line.getAs[SV](6 * i + 5)
          spv += new SparseVector[Double](sv.indices, sv.values, sv.size) * weight

        }
      }
      spv  = spv / norm(spv)
      vid_idf_line(vid, duration, tags_id.distinct.toArray, tags_source.distinct.toArray, spv.index, spv.data)

    })





    println("vid resulut done, print")
    vid_result.printSchema()
    vid_result.show(false)

    val idf_output_path = output_path + "/vid_idf"
    println("begin to write vid_tags_total to path: " + idf_output_path)
    vid_result.write.mode("overwrite").parquet(idf_output_path)
    println("--------[join all vid data done. begin to get guid vid weight]--------")
    idf_output_path

  }

  // Row 特指目前的vid_info里面的row，之后可能会泛化。
  case class TagInfo(tag: String, tag_id: Long, weight: Double)
  def filter_data(line: Row) : (String, Int, Seq[TagInfo]) = {  // tag, tag_id, weight

    val vid = line.getString(0)
    val duration = line.getInt(1)
    val tags_id = line.getAs[Seq[Long]](2)  // 这个是核对调试用的
    val tags_source = line.getAs[Seq[String]](3)
    val features_index = line.getAs[Seq[Int]](4)
    val features_data = line.getAs[Seq[Double]](5)
    val ret_arr = new ArrayBuffer[TagInfo]
    for(tag<-tags_source.distinct){
      //区分inter和outer id，映射回来
      if(tag.contains("_inner")) {
        val tag_pure = tag.replace("_inner", "")

        val tag_id = Hash.hash_own(tag_pure)
        val hash_value = hash_tf_idf(Hash.hash_own(tag_pure))
        val index = features_index.indexOf(hash_value)
        if (index == -1) {
          //没有命中，基本不可能 出现说明错了
        } else {
          val weight = features_data(index)
          ret_arr += TagInfo(tag_pure, tag_id, weight)
        }
      } else {  // 先锋id   tag:  xxx##123456这样的形式
        val tag_data = tag.split("##")
        val tag_pure = tag_data(0)
        val tag_id = tag_data(tag_data.length - 1).toLong
        val hash_value = hash_tf_idf(tag_id)

        val index = features_index.indexOf(hash_value)
        if (index == -1) {
          //没有命中，基本不可能
        } else {
          val weight = features_data(index)
          ret_arr += TagInfo(tag_pure, tag_id, weight)
        }
      }
    }
    (vid, duration, ret_arr.toSeq)
  }


  def tf_idf_to_tuple(spark: SparkSession, input_path: String, output_path: String) : String = {
    import spark.implicits._
    val ret_path = output_path + "/tf_idf_wash_path"
    println("wash tf_idf data")
    val data = spark.read.parquet(input_path).map(line=>filter_data(line)).toDF("vid", "duration", "tag_info")
    data.write.mode("overwrite").parquet(ret_path)
    println("--------[wash tf_idf data done, output path: " + ret_path + "]--------")
    ret_path
  }



  // input_path: args(2)
  // output_path: baronfeng/output/tag_recom_ver_1
  // return value: baronfeng/output/tag_recom_ver_1/guid_vid_weight
  def get_guid_tag_weight(spark: SparkSession, input_path:String, output_path: String) : String = {
    println("---------[begin to collect guid vid weight]----------")
    //val guid_data = process_4499(spark, input_path)
    val guid_data = spark.read.parquet(input_path)
    guid_data.createOrReplaceTempView("temp_db")
    val guid_vids = guid_data
      .sqlContext
      .sql("select t.guid, t.vid, sum(t.playduration) as playduration, t.duration, sum(t.sqrtx) as sqrtx " +
        " from (" +
        "select distinct guid, vid,  value2 as playduration, value3 as duration, value2/value3 as `sqrtx` " +
        " from temp_db " +
        " where value1 = \"50\" and event_info = \"play\"  and value2 >1.0 and value3 > 2.0 and value2/value3>0.01" +
        " ) t" +
        " group by t.guid, t.vid, t.duration")

      .coalesce(REPARTITION_NUM).toDF()
    println("guid, vid, playduration, duration, sqrtx print. distincted.")
    guid_vids.printSchema()
    //guid_vids.show()

    val guid_output_path = output_path + "/guid_vid_weight"
    println("write to parquet, to path: " + guid_output_path)
    guid_vids.write.mode("overwrite").parquet(guid_output_path)
    // 分别是 step=50, 4499播放数据, playduration播放量不为0, duration合法
    println("--------[guid vid weight done. begin get guid sv]--------")
    guid_output_path

  }


  val sv_weight: UserDefinedFunction = udf((features_data: Seq[Double], weight: Double)=> features_data.map(data=> data * weight))

  //guid_input_path: baronfeng/output/tag_recom_ver_1/guid_vid_weight
  //vid_sv_input_path: baronfeng/output/tag_recom_ver_1/vid_idf
  //output_path: baronfeng/output/tag_recom_ver_1/
  //return value: baronfeng/output/tag_recom_ver_1/guid_vid_sv
  def guid_vid_sv_join(spark: SparkSession, guid_input_path: String, vid_sv_input_path:String, output_path: String) : String = {
    import spark.implicits._
    // tags合并用
    spark.sqlContext.udf.register("flatten_long", (xs: Seq[Seq[Long]]) => {
      xs.flatten.distinct
    })
    spark.sqlContext.udf.register("flatten_string", (xs: Seq[Seq[String]]) => {
      xs.flatten.distinct
    })
    // sparsevector 计算用
    spark.sqlContext.udf.register("sv_weight", (features_data: Seq[Double], weight: Double)=> features_data.map(data=> data * weight))
    // 向量的相加
    spark.sqlContext.udf.register("sv_flatten", (features_indice: Seq[Seq[Int]], features_datas: Seq[Seq[Double]]) => {
      if(features_datas.length != features_indice.length) {
        null
      } else {
        val sv_ret = new SparseVector[Double](new Array[Int](0), new Array[Double](0), TAG_HASH_LENGTH)
        for(i <- features_indice.indices) {
          sv_ret += new SparseVector[Double](features_indice(i).toArray, features_datas(i).toArray, TAG_HASH_LENGTH)
        }
        (sv_ret.index, sv_ret.data)
      }
    })
    val result_output_path = output_path + "/guid_vid_sv"

    val guid_data = spark.read.parquet(guid_input_path)
    val vid_sv_data = spark.read.parquet(vid_sv_input_path)
      .as[vid_idf_line]
      .toDF("vid2", "duration", "tags", "tags_source", "features_index", "features_data")
      .cache()

  val guid_result_temp = guid_data.join(vid_sv_data, guid_data("vid") === vid_sv_data("vid2"), "left")
      .filter($"vid2".isNotNull && $"vid2" =!= "")
      .select($"guid", $"vid",  $"tags", $"tags_source", $"features_index", sv_weight($"features_data", $"sqrtx") as "features_data")

    guid_result_temp.createOrReplaceTempView("temp_db")
    // guid: String, vids:Seq[String], tags:Seq[String], features: (Seq[Int], Seq[Double])
    val sql_inner = "select guid, collect_list(vid) as vids, flatten_long(collect_set(tags)) as tags,  flatten_string(collect_set(tags_source)) as tags_source,  sv_flatten(collect_list(features_index), collect_list(features_data)) as features from temp_db group by guid "
    val sql_outer = "select guid, vids, tags, tags_source, features._1 as index, features._2 as data from ( " + sql_inner + " )"
    val guid_result = guid_result_temp.sqlContext.sql(sql_outer)
    guid_result.printSchema()



    val res_with_pair = guid_result.map(line => {  // guid vids tags tags_source features_index features_data
      val guid = line.getString(0)
      val vids = line.getAs[Seq[String]](1)
      val tag_ids = line.getAs[Seq[Long]](2)
      val tags_source = line.getAs[Seq[String]](3)
      val features_index = line.getAs[Seq[Int]](4)
      val features_data = line.getAs[Seq[Double]](5)
      val ret_arr = new ArrayBuffer[(String, Long, Double)]
      for(tag<-tags_source.distinct){
        //区分inter和outer id，映射回来
        if(tag.contains("_inner")) {
          val tag_pure = tag.replace("_inner", "")

          val tag_id = Hash.hash_own(tag_pure)
          val hash_value = hash_tf_idf(Hash.hash_own(tag_pure))
          val index = features_index.indexOf(hash_value)
          if (index == -1) {
            //println("tag: " + tag + ", hash_value: " + hash_value + ", index: " + index)

          } else {

            val weight = features_data(index)
            ret_arr += ((tag_pure, tag_id, weight))
          }
        } else {  // 先锋id   tag:  xxx##123456这样的形式
          val tag_data = tag.split("##")
          val tag_pure = tag_data(0)
          val tag_id = tag_data(tag_data.length - 1).toLong
          val hash_value = hash_tf_idf(tag_id)

          val index = features_index.indexOf(hash_value)
          if (index == -1) {
            //println("tag: " + tag + ", hash_value: " + hash_value + ", index: " + index)

          } else {

            val weight = features_data(index)
            ret_arr += ((tag_pure, tag_id, weight))
          }
        }
      }
      (guid, vids,  ret_arr)
    }).toDF("guid", "vids", "tagid_weight")
    println("write to parquet, to path: " + result_output_path)
    res_with_pair.write.mode("overwrite").parquet(result_output_path)
    println("--------[get guid sv done]--------")



    result_output_path
  }



  def seqOp(a:Array[SV], b:Array[SV]) : Array[SV] = {
    a ++ b
  }
  def combOp(a:Array[SV], b:Array[SV]) : Array[SV] = {
    a ++ b
  }

  case class cid_vidstr(cid:String, vidstr:String)



  def monthly_guid_wash(spark:SparkSession, vid_source_path: String, log_source_path: String, output_path: String) : Unit = {
    //val vid_tags = output_path + "/vid_tag_collect"
    //val vid_tags = vid_tag_shuffle(spark, vid_source_path, output_path)

    //val idf_source_output_path = output_path + "/tf_idf_source_data"
    //val idf_source_output_path = vid_tf_idf(spark, vid_tags, output_path)

    val idf_output_path = output_path + "/vid_idf"
    //val idf_output_path = tf_idf_join(spark, idf_source_output_path, output_path)

    val conf = spark.sparkContext.hadoopConfiguration
    val fs = org.apache.hadoop.fs.FileSystem.get(conf)
    for (date <- Tools.get_last_month_date_str()) {

      val source_path = log_source_path + "/dt=" + date
      val exists = fs.exists(new org.apache.hadoop.fs.Path(source_path))

      if (exists && fs.listStatus(new org.apache.hadoop.fs.Path(source_path)).nonEmpty) {
        //val guid_output_data = output_path + "/guid_vid_weight"
        println("begin to process guid data, path: " + source_path)
        val guid_output_data = get_guid_tag_weight(spark, source_path + "/*", output_path + "/dt=" + date)

        //guid_input_path: baronfeng/output/tag_recom_ver_1/guid_vid_weight
        //vid_sv_input_path: baronfeng/output/tag_recom_ver_1/vid_idf
        //output_path: baronfeng/output/tag_recom_ver_1/
        //return value: baronfeng/output/tag_recom_ver_1/guid_vid_sv
        val result_path = guid_vid_sv_join(spark, guid_output_data, idf_output_path, output_path + "/dt=" + date)
        println("process guid data output_path: " + result_path + " done.")
      } else {
        println("path [" + source_path + "] not exists!")
      }
    }

  }

  def time_weight(day_before: Int): Double = {
    Math.pow(day_before.toDouble, -0.35)
  }

  val sv_data_weight = udf((features_data: ((Seq[Int], Seq[Double])), weight: Double)=> {
    val data = features_data._2.map(value => value * weight)
    (features_data._1, data)
  })
  // guid_path = baronfeng_video/output/tag_recom_ver_2      /dt=20170821/guid_vid_sv
  def monthly_guid_join(spark:SparkSession, guid_path: String, output_path: String) : Unit = {
    // tags合并用
    spark.sqlContext.udf.register("flatten", (xs: Seq[Seq[String]]) => xs.flatten.distinct)
    // sparsevector 计算用
    spark.sqlContext.udf.register("sv_weight", (features_data: Seq[Double], weight: Double)=> features_data.map(data=> data * weight))
    // 向量的相加
    spark.sqlContext.udf.register("sv_flatten", (guid_tag_weight: Seq[Seq[Row]]) => {  //(String, Long, Double)
      val tag_map = new mutable.HashMap[String, Long]()

      val temp = new SparseVector[Double](new Array[Int](0), new Array[Double](0), TAG_HASH_LENGTH)
      for(tag_id_weight <-guid_tag_weight) {

        val feature_index = tag_id_weight.map(line=>Hash.hash_tf_idf(line.getLong(1))).toArray
        val feature_data = tag_id_weight.map(_.getDouble(2)).toArray
        tag_map ++= tag_id_weight.map(line => (line.getString(0), line.getLong(1)))
        temp += new SparseVector[Double](feature_index, feature_data, TAG_HASH_LENGTH)
      }
      tag_map.map(kv => {
        val hash_id = Hash.hash_tf_idf(kv._2, TAG_HASH_LENGTH)
        val index = temp.index.indexOf(hash_id)
        val weight = temp.data(index)
        (kv._1, kv._2, weight)
      }).toSeq.sortWith(_._3 > _._3)
    })

    spark.sqlContext.udf.register("sv_data_weight", (tagid_weight: Seq[Row], weight: Double)=> {  //(String, Long, Double)
      tagid_weight.map(line=>(line.getString(0), line.getLong(1), line.getDouble(2)/weight))
    })

    // guid: String, vids:Seq[String], tags:Seq[String], features: (Seq[Int], Seq[Double])

    val conf = spark.sparkContext.hadoopConfiguration
    val fs = org.apache.hadoop.fs.FileSystem.get(conf)

    //val yesterday = Tools.get_n_day(-1)


    var useful_date = Tools.get_last_month_date_str()
    val last_useful_date = 20170905.toString
    var guid_data = spark.read.parquet(guid_path + "/dt=" + last_useful_date.toString + "/guid_vid_sv")
    for(date <- useful_date if date != last_useful_date) {
      val guid_path_temp = guid_path + "/dt=" + date.toString + "/guid_vid_sv"
      val exists = fs.exists(new org.apache.hadoop.fs.Path(guid_path_temp))

      val weight = time_weight(Tools.diff_date(date, 20170905.toString) + 1)
      if(exists) {
        println("process data: " + guid_path_temp + " time_weight: " + weight)
        val guid_data_daily = spark.read.parquet(guid_path_temp)
        guid_data_daily.createOrReplaceTempView("temp_db" + date)
        val guid_data_daily_res = spark.sqlContext.sql("select guid, vids, sv_data_weight(tagid_weight, " + weight + "d) as tagid_weight from temp_db"+date)
        guid_data = guid_data.union(guid_data_daily_res)
      } else {
        println("cannot find data " + guid_path_temp)
      }
    }
    guid_data.createOrReplaceTempView("temp_guid_db")
    val data_res = spark.sqlContext.sql("select guid, flatten(collect_set(vids)) as vids, sv_flatten(collect_set(tagid_weight)) as tag_vid_weight  from temp_guid_db group by guid")
    val result_path = output_path + "/guid_total_result_30days"
    data_res.write.mode("overwrite").parquet(result_path)
    println("process guid data output_path: " + result_path + " done.")
  }

// output_path + "/guid_total_result_30days"
  def put_guid_tag_to_redis(spark: SparkSession, path : String): Unit = {
    println("put guid_tag to redis")
    import spark.implicits._
    val ip = "100.107.17.202"   //sz1159.show_video_hb_online.redis.com
    val port = 9039
    //val limit_num = 1000
    val bzid = "uc"
    val prefix = "G1"
    val tag_type: Int = 2501
    val data = spark.read.parquet(path).map(line=>{
      val guid = line.getString(0)
      val value_weight = line.getAs[Seq[Row]](2)
        val value_weight_res = {if(value_weight.length>100) value_weight.take(100) else value_weight}
                .map(v=>(v.getLong(1).toString, v.getDouble(2))).distinct  //_1 tag _2 tag_id _3 weight
      KeyValueWeight(guid, value_weight_res)
    }).filter(d => Tools.boss_guid.contains(d.key)).cache
    //data.collect().foreach(line=>println(line.key))
    //println("\n\n\n\n")
    val test_redis_pool = new TestRedisPool(ip, port, 40000)
    val broadcast_redis_pool = spark.sparkContext.broadcast(test_redis_pool)
    Tools.put_to_redis(data, broadcast_redis_pool, bzid, prefix, tag_type /*, limit_num = 1000 */)
    println("put to redis done, number: " + data.count)

  }

  // output_path + "/tf_idf_wash_path"
  def put_vid_tag_to_redis(spark: SparkSession, path : String): Unit = {
    println("put vid_tag to redis")
    import spark.implicits._
    val ip = "100.107.17.229"    // zkname sz1163.short_video_tg2vd.redis.com  这个需要清空  真正要写的是sz1162.short_video_vd2tg.redis.com 我先改过来
    val port = 9014
    //val limit_num = 1000
    val bzid = "sengine"
    val prefix = "v0_sv_vd2tg"
    val tag_type: Int = 2501
    val data = spark.read.parquet(path).map(line=>{
      val vid = line.getString(0)
      val value_weight = line.getAs[Seq[Row]](2)

        .map(v=>(v.getLong(1).toString, v.getDouble(2))).distinct.sortWith(_._2 > _._2) //_1 tag _2 tag_id _3 weight
      val value_weight_res = if(value_weight.length > 100) value_weight.take(100) else value_weight
      KeyValueWeight(vid, value_weight_res)
    })
    //data.collect().foreach(line=>println(line.key))
    //println("\n\n\n\n")
    val test_redis_pool = new TestRedisPool(ip, port, 40000)
    val broadcast_redis_pool = spark.sparkContext.broadcast(test_redis_pool)
    Tools.put_to_redis(data, broadcast_redis_pool, bzid, prefix, tag_type /*, limit_num = 1000 */)
    println("put to redis done, number: " + data.count)

  }

  // output_path + "/tf_idf_wash_path"
  def get_tag_vid_data(spark: SparkSession, path : String): Unit = {
    val res_path = "temp_data/tag_vid_weight"

    spark.sqlContext.udf.register("tuple_vid_weight", (vid: String, weight: Double)=> {  //(String, Long, Double)
      (vid, weight)
    })


    println("put vid_tag to redis")
    import spark.implicits._
    val ip = "100.107.17.228"    // zkname sz1163.short_video_tg2vd.redis.com  这个需要清空  真正要写的是sz1162.short_video_vd2tg.redis.com 我先改过来
    val port = 9012
    //val limit_num = 1000
    val bzid = "sengine"
    val prefix = "v0_sv_tg2vd"
    val tag_type: Int = 2501
    val data = spark.read.parquet(path).map(line=>{
      val vid = line.getString(0)
      val value_weight = line.getAs[Seq[Row]](2)

        .map(v=>(v.getLong(1).toString, v.getDouble(2))) //_1 tag _2 tag_id _3 weight

      KeyValueWeight(vid, value_weight)
    })
      .flatMap(line=>{
        val vid = line.key
        val value_weight = line.value_weight
        value_weight.map(tagid_weight => {(vid, tagid_weight._1, tagid_weight._2)})
      }).toDF("vid", "tag_id", "weight")

    data.createOrReplaceTempView("temp_db")

    val res_data = spark.sql("select tag_id, collect_list(tuple_vid_weight(vid, weight)) from temp_db group by tag_id")
      .map(line =>{
        val tag_id = line.getString(0)
        val vid_weight = line.getAs[Seq[Row]](1).map(d=>{(d.getString(0), d.getDouble(1))}).sortWith(_._2>_._2)
        val vid_weight_res = if(vid_weight.length>5000) vid_weight.take(5000) else vid_weight
        KeyValueWeight(tag_id, vid_weight_res)
      })

    res_data.write.mode("overwrite").parquet(res_path)

    //data.collect().foreach(line=>println(line.key))
    //println("\n\n\n\n")
//    val test_redis_pool = new TestRedisPool(ip, port, 40000)
//    val broadcast_redis_pool = spark.sparkContext.broadcast(test_redis_pool)
//    Tools.delete_to_redis(data, broadcast_redis_pool, bzid, prefix, tag_type /*, limit_num = 1000 */)
    println("put to redis done, number: " + res_data.count)

  }

  // output_path + "/tf_idf_wash_path"
  def put_tag_vid_to_redis(spark: SparkSession, path : String): Unit = {
    val res_path = "temp_data/tag_vid_weight"



    println("put tag_vid to redis")
    import spark.implicits._

    val ip = "100.107.17.228"    // zkname sz1163.short_video_tg2vd.redis.com  这个需要清空  真正要写的是sz1162.short_video_vd2tg.redis.com 我先改过来
    val port = 9012
    //val limit_num = 1000
    val bzid = "sengine"
    val prefix = "v0_sv_tg2vd"
    val tag_type: Int = 2501
    val data = spark.read.parquet(res_path).as[KeyValueWeight]

    //data.collect().foreach(line=>println(line.key))
    //println("\n\n\n\n")
    val test_redis_pool = new TestRedisPool(ip, port, 40000)
    val broadcast_redis_pool = spark.sparkContext.broadcast(test_redis_pool)
    Tools.put_to_redis(data, broadcast_redis_pool, bzid, prefix, tag_type /*, limit_num = 1000 */)
    println("put to redis done, number: " + data.count)

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

    val vid_input_path = args(0)
    val guid_input_path = args(1)
    val output_path = args(2)
    println("------------------[begin]-----------------")


    val vid_tags = output_path + "/vid_tag_collect"
    //val vid_tags = vid_tag_shuffle(spark, vid_input_path, output_path)

    val idf_source_output_path = output_path + "/tf_idf_source_data"
    //val idf_source_output_path = vid_tf_idf(spark, vid_tags, output_path)

    val idf_output_path = output_path + "/vid_idf"
    //val idf_output_path = tf_idf_join(spark, idf_source_output_path, output_path)

   // tf_idf_to_tuple(spark, idf_output_path, output_path)

    //val guid_output_data = output_path + "/guid_vid_weight"
    //val guid_output_data = get_guid_tag_weight(spark, guid_input_path, output_path)

    //guid_input_path: baronfeng/output/tag_recom_ver_1/guid_vid_weight
    //vid_sv_input_path: baronfeng/output/tag_recom_ver_1/vid_idf
    //output_path: baronfeng/output/tag_recom_ver_1/
    //return value: baronfeng/output/tag_recom_ver_1/guid_vid_sv
    //val result_path = guid_vid_sv_join(spark, guid_output_data, idf_output_path, output_path)
    //println("done, result_path: " + result_path)

    //monthly_guid_wash(spark, vid_input_path, guid_input_path, output_path)

    val guid_path = output_path
    //monthly_guid_join(spark, guid_path, output_path)

    //put_vid_tag_to_redis(spark, output_path + "/tf_idf_wash_path")
    //put_guid_tag_to_redis(spark, output_path + "/guid_total_result_30days")
    put_tag_vid_to_redis(spark, output_path + "/tf_idf_wash_path")
    println("------------------[done]-----------------")
  }




  val vid_useful_col: Map[String, LineInfo] = Map(
    "pioneer_tag" -> LineInfo(PIONEER_TAG_INDEX, 2.0, "\\+|#", true),  //先锋标签只取id
    "tags" -> LineInfo(45, 1.0, "#", true),
    "director" -> LineInfo(30, 1.0, "#", false),
    "writer" -> LineInfo(32, 1.0, "#", false),
    "guests" -> LineInfo(40, 1.0, "#", false),
    "keywords" -> LineInfo(46, 1.0, "\\+", true),
    "relative_stars" -> LineInfo(66, 1.0, "#", false),
    "stars_name" -> LineInfo(71, 1.0, "#", false),
    "sportsman" -> LineInfo(95, 1.0, "#", false),
//    "first_recommand" -> LineInfo(75, 1.0, "", false),
//    "sec_recommand" -> LineInfo(76, 1.0, "", false),
    "singer_name" -> LineInfo(67, 1.0, "", false),
    "game_name" -> LineInfo(114, 1.0, "", false),
    "title" -> LineInfo(49, 0.8, "", false)
  )

  val cid_useful_col: Map[String, LineInfo] = Map(
    "pioneer_tag" -> LineInfo(PIONEER_TAG_ID_INDEX, 2.0, "\\+|#", true),  //先锋标签只取id
    "tags" -> LineInfo(45, 1.0, "#", true),
    "director" -> LineInfo(30, 1.0, "#", false),
    "writer" -> LineInfo(32, 1.0, "#", false),
    "guests" -> LineInfo(40, 1.0, "#", false),
    "keywords" -> LineInfo(46, 1.0, "\\+", true),
    "relative_stars" -> LineInfo(66, 1.0, "#", false),
    "stars_name" -> LineInfo(71, 1.0, "#", false),
    "sportsman" -> LineInfo(95, 1.0, "#", false),
    //    "first_recommand" -> LineInfo(75, 1.0, "", false),
    //    "sec_recommand" -> LineInfo(76, 1.0, "", false),
    "singer_name" -> LineInfo(67, 1.0, "", false),
    "game_name" -> LineInfo(114, 1.0, "", false),
    "title" -> LineInfo(49, 0.8, "", false)
  )




}
