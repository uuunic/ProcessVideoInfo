package BasicData


import Utils.Defines._
import Utils.Hash.hash_tf_idf
import Utils.Tools.KeyValueWeight
import Utils.{Hash, TestRedisPool, Tools}
import breeze.linalg.{SparseVector, norm}
import com.huaban.analysis.jieba.JiebaSegmenter.SegMode
import com.huaban.analysis.jieba.{JiebaSegmenter, SegToken}
import org.apache.spark.ml.feature.{HashingTF, IDF}
import org.apache.spark.ml.linalg.{SparseVector => SV}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

import scala.collection.mutable.ArrayBuffer
/**
  * Created by baronfeng on 2017/9/15.
  */
object VidTagsWeight {
  //分词工具
  val segmenter = new JiebaSegmenter
  val REPARTITION_NUM = 400

  val PIONEER_TAG_INDEX = 88  // 先锋标签的index位置
  val PIONEER_TAG_ID_INDEX = 89 //先锋标签对应的id位置
  val digit_regex = """^\d+$""".r  //纯数字 正则表达式

  case class LineInfo(index:Int, weight:Double, split_str:String, is_distinct:Boolean)
  case class DsVidLine(vid: String, duration: Int, tags:Array[Long], tags_source:Array[String], weight:Double, is_distinct:Boolean)

  val vid_useful_col: Map[String, LineInfo] = Map(
    "pioneer_tag" -> LineInfo(PIONEER_TAG_INDEX, 2.0, "\\+|#", is_distinct = true),  //先锋标签只取id
    "tags" -> LineInfo(45, 1.0, "#", is_distinct = true),
 //   "director" -> LineInfo(30, 1.0, "#", is_distinct = false),   //keywords writers directors 数量太少，直接去掉
 //   "writer" -> LineInfo(32, 1.0, "#", is_distinct = false),
    "guests" -> LineInfo(40, 1.0, "#", is_distinct = false),
//    "keywords" -> LineInfo(46, 1.0, "\\+", is_distinct = true),
    "relative_stars" -> LineInfo(66, 1.0, "#", is_distinct = false),
    "stars_name" -> LineInfo(71, 1.0, "#", is_distinct = false),
    "sportsman" -> LineInfo(95, 1.0, "#", is_distinct = false),
    //    "first_recommand" -> LineInfo(75, 1.0, "", false),
    //    "sec_recommand" -> LineInfo(76, 1.0, "", false),
    "singer_name" -> LineInfo(67, 1.0, "", is_distinct = false),
    "game_name" -> LineInfo(114, 1.0, "", is_distinct = false),
    "title" -> LineInfo(49, 0.8, "", is_distinct = false),   // title目前不用，只用来搞join用。必须有title才会有下一步
    "cid_tags" -> LineInfo(-1, 1.0, "", is_distinct = false)
  )

  val video_ids_index = 84
  val cid_useful_col: Map[String, LineInfo] = Map(
    "tags" -> LineInfo(49, 1.0, "\\+", is_distinct = true),
    "leading_actor" -> LineInfo(34, 1.0, "#", is_distinct = false)
  )

  /**
    * @return 停止词的set
    * */
  def get_stop_words(spark: SparkSession) : Set[String] = {
    val data = spark.sparkContext.textFile(STOP_WORDS_PATH)
    data.map(_.trim).collect().toSet
  }

  // 从vid中获取tag，并按照各自的标题分组
  // input_path: args(0)
  // output_path: baronfeng/output/tag_recom_ver_1
  // return value: baronfeng/output/tag_recom_ver_1/vid_tag_collect

  /**
    * @param vid_info_path vid_info的位置，目前为/data/stage/outface/omg/tdw/export_video_t_video_info_extend_hour/ds=YYYYMMDDHH
    * @param cid_info_path cid_info的位置，目前为/data/stage/outface/omg/export_video_t_cover_info_extend_hour/ds=YYYYMMDD23  注意只有23点的时候落地了
    * @param vid_tags_output_path 输出位置，目前为BasicData/VidTagsWeight/YYYYMMDD/${channel}/
    * */
  def vid_tag_shuffle(spark: SparkSession, vid_info_path: String, cid_info_path: String, vid_tags_output_path: String) : Int = {
    import spark.implicits._

    println("---------[begin to collect vid tags]----------")

    val stop_words = spark.sparkContext.broadcast(get_stop_words(spark))
    val cid_tags_word = "cid_tags"

    val rdd = spark.sparkContext.textFile(vid_info_path)
      .map(line => line.split("\t", -1))
      .filter(_.length > 116)
      .filter(line => line(59) == "4") // 在线上
          .filter(line=>line(1).length == 11) //vid长度为11
      .repartition(REPARTITION_NUM).cache()  // line(59) b_state


    for((tag_name, info) <- vid_useful_col if tag_name != cid_tags_word) {
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
        if (info.index != PIONEER_TAG_ID_INDEX && ret_tags_without_digit.nonEmpty) {
          val pioneer_tags_line = line(PIONEER_TAG_INDEX)

          val pioneer_tags_id = line(PIONEER_TAG_ID_INDEX)
            .trim.split("\\+|#", -1)
            .filter(_.matches("""^\d+$"""))
            .map(_.toLong)
          val pioneer_tags = pioneer_tags_line.split("\\+|#", -1).filter(!_.equals(""))
          val pioneer_tags_map = pioneer_tags.zip(pioneer_tags_id).toMap
          for (i <- ret_tags_without_digit.indices) {
            val tag = ret_tags_without_digit(i)
            if (pioneer_tags_map.contains(tag)) {
              val tag_id = pioneer_tags_map(tag)

              ret_tags_without_digit.update(i, tag_id.toString) // 使用先锋id代替tag
              // 不需要 因为直接就是数字了
              ret_tags_strs.update(i, ret_tags_strs(i) + "##" + tag_id.toString) // 标记一下，方便后面使用， 记住，是先hash成long，再加的##
            } else {
              ret_tags_without_digit.update(i, Hash.hash_own(tag).toString) // 记住，是先hash成long，再加的inner标签
              ret_tags_strs.update(i, ret_tags_strs(i) + "_inner")
            }
          }

        }
        DsVidLine(vid, duration, ret_tags_without_digit.map(_.toLong).toArray, ret_tags_strs, info.weight, info.is_distinct)

      })
        .filter(line => {(!line.tags.isEmpty) && (line.tags.length > 0)}).toDS

      val ret_path = vid_tags_output_path + "/" + tag_name
      printf("get tags type: %s done, write to: %s\n", tag_name, ret_path)
      //tag_info_line.show
      tag_info_line.write.mode("overwrite").parquet(ret_path)

    }

    //开始处理cid相关的tag，一并处理就可以了

    println("begin to get tags from cover_info: " + cid_tags_word)
    val cid_data = spark.read.textFile(cid_info_path).map(_.split("\t", -1))
      .filter(_.length >= 136)
      .filter(arr => arr(61) == "4") //_(61)是b_cover_checkup_grade 要未上架的和在线的
      .filter(line => !line(video_ids_index).equals("")) //84为长视频列表
      .flatMap(line => {
      val vids = line(video_ids_index).split("#", -1).map(_.trim).filter(_.nonEmpty)
      val tags = new ArrayBuffer[String]

      for ((tag_name, info) <- cid_useful_col) {
        if (info.split_str == "") {
          tags.append(line(info.index).trim())
        } else {
          tags ++= line(info.index).split(info.split_str, -1).map(_.trim).filter(_.nonEmpty)
        }

      }
      vids.map(vid => {
        val tags_id = tags.map(tag => Hash.hash_own(tag)).toArray
        DsVidLine(vid, 0, tags_id, tags.map(line=>line + "_inner").toArray, 1.0, is_distinct = false)  //标记上内部标签
      })
    })
      .filter(_.tags.length != 0)
      .groupBy($"vid", $"duration", $"weight", $"is_distinct")
      .agg(collect_list($"tags") as "tags", collect_list($"tags_source") as "tags_source")
      .map(line => {
        val vid = line.getString(0)
        //          val duration = line.getInt(1)
        //          val weight = line.getDouble(2)
        //          val is_distinct = line.getDouble(3)
        val tags_id = line.getAs[Seq[Seq[Long]]](4).flatten.distinct
        val tags_source = line.getAs[Seq[Seq[String]]](5).flatten.distinct
        DsVidLine(vid, 0, tags_id.toArray, tags_source.toArray, 1.0, is_distinct = false)
      })


    cid_data.write.mode("overwrite").parquet(vid_tags_output_path + "/" + cid_tags_word)

    println("---------[collect vid tags done]----------")
    0
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
    title_tags.toArray

  }

  case class vid_idf_line(vid: String,
                          duration: Int,
                          tags:Array[Long],
                          tags_source: Array[String],
                          features_index: Array[Int],
                          features_data: Array[Double]
                         )


  val vid_idf_useful_strs = Array("vid", "duration", "tags", "tags_source", "weight", "features")
  def vid_tf_idf(spark:SparkSession, vid_tags_path:String, idf_output_path: String) : Unit = {
    import spark.implicits._
    println("---------[begin to process tf-idf]----------")
    val hashingTF = new HashingTF().setInputCol("tags").setOutputCol("tag_raw_features").setNumFeatures(TAG_HASH_LENGTH)
    val idf = new IDF().setInputCol("tag_raw_features").setOutputCol("features")

    for (tag_name <- vid_useful_col.keys) {
      printf(" begin [%s] idf", tag_name)
      val tag_data = spark.read.parquet(vid_tags_path + "/" + tag_name).as[DsVidLine].cache()

      printf(" read [%s] done, count: %d, begin process\n", tag_name, tag_data.count())
     // tag_data.printSchema()
      val newSTF = hashingTF.transform(tag_data)


      //下面计算IDF的值
      val idfModel = idf.fit(newSTF)
      val rescaledData = idfModel.transform(newSTF)
        // select 要求至少有一个元素，所以用这种脏一些的方法来处理
        .select(vid_idf_useful_strs(0), vid_idf_useful_strs.drop(1):_*)   //$"vid", $"duration", $"tags", $"tags_source", $"weight", $"features"
        // tag_name + "_vid", tag_name + "_duration", tag_name + "_tags", tag_name + "_tags_source", tag_name + "_weight", tag_name + "_features"
        .toDF(vid_idf_useful_strs.map(line=>tag_name + "_" + line): _*) // "vid" => "title_vid"
      // vid tags tag_size tag_raw_features features
      rescaledData.write.mode("overwrite").parquet(idf_output_path + "/" + tag_name)
      println("write idf source data done. type: "+ tag_name)
    }
    println("--------[idf done. begin join all vid data]--------")

  }

  def tf_idf_join(spark:SparkSession, idf_path:String, join_output_path: String) : Unit = {
    // 从title开始，依次合并
    import spark.implicits._

    var vid_total:DataFrame = spark.read.parquet(idf_path + "/title")
    for ((tag_name, vid_data) <- vid_useful_col if !tag_name.equals("title")) {
      //vid duration tags tags_source weight features
      val vid_data = spark.read.parquet(idf_path + "/" + tag_name)
      vid_total = vid_total.join(vid_data, vid_total("title_vid") === vid_data(tag_name + "_vid"), "left")
    }


    println("put vid_total schema, be careful: title_vid is in schema, but not in process!")
    vid_total.printSchema()
    println(s"process these ${vid_useful_col.size - 1} tag types:")
    for (s <- vid_useful_col.filter(_._1 != "title")) {
      println(s"[${s._1}]")
    }
    val vid_result = vid_total.map(line => {
      val vid = line.getString(0)
      val duration = line.getInt(1)
      val tags_id = new ArrayBuffer[Long]
      val tags_source = new ArrayBuffer[String]
      var spv: SparseVector[Double] = null

      val column_num = vid_idf_useful_strs.length

      // title相关的不在，所以下标从1开始
      for (i <- 1 until vid_useful_col.size) {

        //  $"vid", $"duration", $"tags", $"tags_source", $"weight", $"features"
        if (!line.isNullAt(column_num * i) && !line.getAs[String](column_num * i).equals("")) {
          tags_id ++= line.getAs[Seq[Long]](column_num * i + 2)
          tags_source ++= line.getAs[Seq[String]](column_num * i + 3)
          val weight = line.getAs[Double](column_num * i + 4)
          val sv = line.getAs[SV](column_num * i + 5)
          if (spv == null){
            spv = new SparseVector[Double](sv.indices, sv.values, sv.size) * weight
          } else {
            spv += new SparseVector[Double](sv.indices, sv.values, sv.size) * weight
          }
        }
      }

      spv = spv / norm(spv)
      vid_idf_line(vid, duration, tags_id.distinct.toArray, tags_source.distinct.toArray, spv.index.filter(_ != 0), spv.data.filter(_ != 0.0))

    })
      .filter(line => {
        line.tags.nonEmpty && line.tags_source.nonEmpty
      })
      .distinct().cache()
    vid_result.printSchema()
    vid_result.show(false)


    println("begin to write vid_tags_total to path: " + join_output_path)
    vid_result.write.mode("overwrite").parquet(join_output_path)
    println("--------[join all vid data done.]--------")
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


  def tf_idf_to_tuple(spark: SparkSession, join_path: String, clean_output_path: String) : Unit = {
    import spark.implicits._
    val ret_path = clean_output_path
    println("wash tf_idf data")
    val data = spark.read.parquet(join_path).map(line=>filter_data(line)).toDF("vid", "duration", "tag_info")
    data.write.mode("overwrite").parquet(ret_path)
    println("--------[wash tf_idf data done, output path: " + ret_path + "]--------")
  }

  // output_path + "/tf_idf_wash_path"
  def get_tag_vid_data(spark: SparkSession, clean_vid_tag_path : String, tag_vid_output_path: String, vid_length: Int): Unit = {
    import spark.implicits._
    spark.sqlContext.udf.register("tuple_vid_weight", (vid: String, weight: Double)=> {  //(String, Long, Double)
      (vid, weight)
    })

    println("get tag_vid data, every tag has at most [" + vid_length +"] vids.")
    val data = spark.read.parquet(clean_vid_tag_path).map(line => {
      val vid = line.getString(0)
      val value_weight = line.getAs[Seq[Row]](2)

        .map(v => (v.getLong(1).toString, v.getDouble(2))) //_1 tag _2 tag_id _3 weight

      KeyValueWeight(vid, value_weight)
    })
      .flatMap(line => {
        val vid = line.key
        val value_weight = line.value_weight
        value_weight.map(tagid_weight => {
          (vid, tagid_weight._1, tagid_weight._2)
        })
      }).toDF("vid", "tag_id", "weight")

    data.createOrReplaceTempView("temp_db")

    val res_data = spark.sql("select tag_id, collect_list(tuple_vid_weight(vid, weight)) from temp_db group by tag_id")
      .map(line =>{
        val tag_id = line.getString(0)
        val vid_weight = line.getAs[Seq[Row]](1).map(d=>{(d.getString(0), d.getDouble(1))}).sortWith(_._2>_._2)
        val vid_weight_res = vid_weight.take(vid_length)
        KeyValueWeight(tag_id, vid_weight_res)
      })
    res_data.repartition(400).write.mode("overwrite").parquet(tag_vid_output_path)
    println("get tag_vid data done, tag number: " + res_data.count)
    println("--------[tag_vid data write done.]--------")

  }

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
        .take(100)
      KeyValueWeight(vid, value_weight)
    })
    //data.collect().foreach(line=>println(line.key))
    //println("\n\n\n\n")
    val test_redis_pool = new TestRedisPool(ip, port, 40000)
    val broadcast_redis_pool = spark.sparkContext.broadcast(test_redis_pool)
    Tools.put_to_redis(data, broadcast_redis_pool, bzid, prefix, tag_type /*, limit_num = 1000 */)
    println("put to redis done, number: " + data.count)
  }

  def put_tag_vid_to_redis(spark: SparkSession, path : String): Unit = {
    val res_path = "temp_data/tag_vid_weight"

    println("put tag_vid to redis")
    import spark.implicits._

    val ip = "100.107.17.228"    // zkname sz1163.short_video_tg2vd.redis.com  这个需要清空  真正要写的是sz1162.short_video_vd2tg.redis.com 我先改过来
    val port = 9012
    //val limit_num = 1000
    val bzid = "sengine"
    val prefix = "v0_sv_tg2vd"
    val tag_type: Int = -1  // 不需要字段中加入tag_type,就设置成-1
    val data = spark.read.parquet(res_path).as[KeyValueWeight]

    //data.collect().foreach(line=>println(line.key))
    //println("\n\n\n\n")
    val test_redis_pool = new TestRedisPool(ip, port, 40000)
    val broadcast_redis_pool = spark.sparkContext.broadcast(test_redis_pool)
    Tools.put_to_redis(data, broadcast_redis_pool, bzid, prefix, tag_type /*, limit_num = 1000 */)
    println("put to redis done, number: " + data.count)

  }

  def main(args: Array[String]) {
    val spark = SparkSession
      .builder
      .appName(this.getClass.getName.split("\\$").last)
      .getOrCreate()


    val vid_input_path = args(0)
    val cid_input_path = args(1)
    val vid_length = args(2).toInt
    val output_path = args(3)
    val date_str = args(4)
    println("------------------[begin]-----------------")

    val vid_tags_path = output_path + "/vid_tags/" + date_str
    vid_tag_shuffle(spark, vid_input_path, cid_input_path, vid_tags_path)


    val vid_idf_path = output_path + "/vid_idf_path/" + date_str
    vid_tf_idf(spark, vid_tags_path, vid_idf_path)

    val idf_join_path = output_path + "/idf_join_path/" + date_str
    tf_idf_join(spark, vid_idf_path, idf_join_path)

    val clean_output_path = output_path + "/cleaned_data/" + date_str
    tf_idf_to_tuple(spark, idf_join_path, clean_output_path)

    val tag_vid_path = output_path + "/tag_vid/" + date_str
    get_tag_vid_data(spark, clean_output_path, tag_vid_path, vid_length)

    println("------------------[done]-----------------")
  }

}
