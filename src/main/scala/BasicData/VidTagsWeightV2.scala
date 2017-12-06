package BasicData

import Utils.Defines._
import Utils.Hash.hash_tf_idf
import Utils.Tools.KeyValueWeight
import Utils._
import breeze.linalg.{SparseVector, norm}
import com.hankcs.hanlp.HanLP.Config
import com.hankcs.hanlp.dictionary.CustomDictionary
import com.hankcs.hanlp.seg.CRF.CRFSegment
import com.huaban.analysis.jieba.JiebaSegmenter.SegMode
import com.huaban.analysis.jieba.{JiebaSegmenter, SegToken}
import org.apache.spark.ml.feature.{HashingTF, IDF}
import org.apache.spark.ml.linalg.{SparseVector => SV}
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

import scala.collection.JavaConversions._
import scala.collection.mutable.ArrayBuffer

/**
  * Created by baronfeng on 2017/9/15.
  */
object VidTagsWeightV2 {
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
//        .toDF(vid_idf_useful_strs.map(line=>tag_name + "_" + line): _*) // "vid" => "title_vid"
      // vid tags tag_size tag_raw_features features
      rescaledData.write.mode("overwrite").parquet(idf_output_path + "/" + tag_name)
      println("write idf source data done. type: "+ tag_name)
    }
    println("--------[idf done. begin join all vid data]--------")

  }

  def feature_weight: UserDefinedFunction = udf((features: SV, weight: Double) =>{
    val values = features.values.map(_ * weight)
    new SV(features.size, features.indices, values)
  })

  def tf_idf_join_v2(spark:SparkSession, idf_path:String, join_output_path: String) : Unit = {
    // 从title开始，依次合并
    import spark.implicits._

    var vid_total:DataFrame = null
    for ((tag_name, vid_data) <- vid_useful_col if !tag_name.equals("title")) {
      //vid duration tags tags_source weight features
      if(vid_total == null){
        vid_total = spark.read.parquet(idf_path + "/" + tag_name)
      } else {
        val vid_data = spark.read.parquet(idf_path + "/" + tag_name)
        vid_total = vid_total.union(vid_data)
      }

    }


    println("put vid_total schema, be careful: title_vid is in schema, but not in process!")
    vid_total.printSchema()
    println(s"process these ${vid_useful_col.size - 1} tag types:")
    for (s <- vid_useful_col.filter(_._1 != "title")) {
      println(s"[${s._1}]")
    }
    //$"vid", $"duration", $"tags", $"tags_source", $"weight", $"features"
    val vid_result = vid_total.groupBy("vid").agg(max($"duration"), collect_set($"tags") as "tags", collect_set($"tags_source") as "tags_source", collect_list(feature_weight($"features", $"weight")) as "features").map(line => {
      val vid = line.getString(0)
      val duration = line.getInt(1)
      val tags_id = line.getSeq[Seq[Long]](2).flatten.distinct.toArray
      val tags_source = line.getSeq[Seq[String]](3).flatten.distinct.toArray
      var spv: SparseVector[Double] = line.getSeq[SV](4).map(sv=>new SparseVector[Double](sv.indices, sv.values, sv.size)).reduce(_ + _)

      spv = spv / norm(spv)
      vid_idf_line(vid, duration, tags_id, tags_source, spv.index.filter(_ != 0), spv.data.filter(_ != 0.0))

    })
      .filter(line => {
        line.tags.nonEmpty && line.tags_source.nonEmpty
      })
      .distinct().cache()
    vid_result.printSchema()
    vid_result.show(false)


    println(s"begin to write vid_tags_total to path: $join_output_path, number: ${vid_result.count()}")
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

  /**将join_data 转换为clean_data*/
  def tf_idf_to_tuple(spark: SparkSession, join_path: String, clean_output_path: String) : Unit = {
    import spark.implicits._
    val ret_path = clean_output_path
    println("wash tf_idf data")
    val data = spark.read.parquet(join_path).map(line=>filter_data(line)).toDF("vid", "duration", "tag_info")
    data.write.mode("overwrite").parquet(ret_path)
    println("--------[wash tf_idf data done, output path: " + ret_path + "]--------")
  }

  // output_path + "/tf_idf_wash_path"
  /**
    * tag-vid倒排数据，依赖数据源是vid-tag clean_data数据
  * */
  def get_tag_vid_data(spark: SparkSession,
                       clean_vid_tag_path : String,
                       vid_filter_path: String,
                       tag_vid_output_path: String,
                       vid_length: Int): Unit = {
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


    val vid_filter = spark.read.json(vid_filter_path).toDF("vid").cache
    println(s"get_vtag: get vid_filter from [$vid_filter_path], number: ${vid_filter.count}")
    // 倒排数据过滤内容池
    val data_filtered = data.join(vid_filter, "vid").select($"vid", $"tag_id", $"weight")

    data_filtered.createOrReplaceTempView("temp_db")
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

  def collect_cid_title_as_tags(spark: SparkSession, cid_path: String): Set[String] = {
    println("--------------get divide words--------------")
    import spark.implicits._
    val cid_data = spark.read.textFile(cid_path).map(_.split("\t", -1))
      .filter(_.length >= 136)
      .filter(_(61) == "4")
      .map(line=>{
        (line(1), line(3), line(19), line(20),line(53), line(54))
      })
      .toDF("cid", "map_name", "type", "type_name", "title", "als")
      .select($"title").as[String]
      .map(line=>{
        var s = line
        for(r <- Defines.pattern_list) {
          s = r.replaceAllIn(s, "")
        }
        s
      })
      .distinct().cache
    println("print cid_titles after regex")
    cid_data.show(false)
    println("--------------get divide words done--------------")
    cid_data.collect().toSet

  }

/**
  * @param tags_path tag的位置，“1”是在线的tag数量，印象中是40w量级
  * @param cid_info_path  得到cid 的名字，这样断cid的时候优先级可以更高一些
  * */
  def collect_tags(spark: SparkSession, tags_path: String, cid_info_path: String) : Set[String] = {
    val real_tags_path = Tools.get_latest_subpath(spark, tags_path)
    println(s"--------------begin to get poineer tags from $real_tags_path--------------")
    val tags = spark.sparkContext.textFile(real_tags_path)
      .map(_.split("\t", -1)).filter(_.length==14).filter(_(6)=="1")
      .map(_(2)).distinct().collect().toSet
    println(s"--------------get poineer tags number: ${tags.size}--------------")
    println(s"--------------begin to get cid titles as tags--------------")
    val clean_cid_titles = collect_cid_title_as_tags(spark, cid_path = cid_info_path)
    println(s"--------------cid title number: ${clean_cid_titles.size}--------------")
    (tags ++ clean_cid_titles).map(_.trim.toLowerCase).filter(_.length > 1)
  }
  def tuple2: UserDefinedFunction = udf((s1: String, s2: Long) => (s1, s2))

  def get_vid_qtag(spark: SparkSession,
                   source_4141_path: String,
                   vid_info_path: String,
                   cid_info_path: String,
                   tags_info_path: String,
                   vid_filter_path: String,
                   stop_words_path: String,
                   vid_qtag_output_path: String,
                   qtag_vid_output_path: String): Unit = {
    import spark.implicits._
    println(s"--------[get vid_qtag_daily from path: $source_4141_path]--------")
    // 取30天的数据，chenxue的数据里有 /time= YYYYMMDD 这个格式
    val last_month_days_path = Tools.get_last_month_date_str()
                        .map(date=>(date, source_4141_path + "/time=" + date))
    if(last_month_days_path.length == 0) {
      println("error, we have no vid qtags data here!")
      return
    }

    println(s"we have ${last_month_days_path.length} days qtag data to process.")


    val cid_info = spark.read.textFile(cid_info_path).map(_.split("\t", -1))
      .filter(_.length >= 136)
      .filter(arr => arr(61) == "4") //_(61)是b_cover_checkup_grade 要未上架的和在线的
      .filter(line => !line(video_ids_index).equals("")) //84为长视频列表
      .map(line => {
      val vids = line(video_ids_index).split("#", -1).map(_.trim).filter(_.nonEmpty)
      val cid = line(1).trim
      (cid, vids)
    }).filter(_._2.nonEmpty)
      .toDF("cid", "vids")

//hanlp的切分词，从cid_info和tag里来

    val useful_split_tags = collect_tags(spark, tags_info_path, cid_info_path = cid_info_path)
    val tags_broadcast = spark.sparkContext.broadcast(useful_split_tags)
    println(s"get_qtag:  get split tags for hanlp from cid_info and tags_info, number: ${useful_split_tags.size}")
// 选出来的停止词
    val stop_words = spark.read.text(stop_words_path).map(_.getString(0).trim().toLowerCase).collect().toSet
    val stop_words_broadcast = spark.sparkContext.broadcast(stop_words)
    println(s"get_qtag: get omg_stop_words from [$stop_words_path], number: ${stop_words.size}")

    // 内容池， qtag-vids要过滤
    val vid_filter = spark.read.json(vid_filter_path).cache
    println(s"get_qtag: get vid_filter from [$vid_filter_path], number: ${vid_filter.count}")
    vid_filter.show()

    // vid pioneer_tag pioneer_tag_id的对应关系
    val vid_pioneer_tag = spark.read.textFile(vid_info_path)
      .map(line => line.split("\t", -1))
      .filter(_.length > 116)
      .filter(line => line(59) == "4") // 在线上
      .filter(line=>line(1).length == 11) //vid长度为11

      .flatMap(line => {
        val vid = line(1)
        val pioneer_tags = line(PIONEER_TAG_INDEX).trim.split("\\+|#", -1).filter(!_.equals(""))
        val pioneer_tags_id = line(PIONEER_TAG_ID_INDEX)
          .trim.split("\\+|#", -1)
          .filter(_.matches("""^\d+$"""))
          .map(_.toLong)
        val vid_tag_tagid = pioneer_tags.zip(pioneer_tags_id).map(tp => (vid, tp._1, tp._2))
        vid_tag_tagid.map(w => {((w._1, w._2), w._3)})
      }).toDF("vid_t_pioneer_tag", "pioneer_tag_id")
      .repartition(REPARTITION_NUM)

      .cache()
    println("show vid_pioneer_tag:")
    vid_pioneer_tag.show
    // guid id query 都是String
    val vid_qtag_data_temp = spark.read.parquet(last_month_days_path.map(_._2): _*)
                          .select($"guid", $"id", $"query").filter(_.getString(2).length >= 2)
      .groupBy($"id", $"query").agg(count($"guid") as "count").repartition(REPARTITION_NUM, $"id", $"query")
      // join 一下cid  防止有cid中的vid没有放进去
      .join(cid_info, $"id" === cid_info("cid"), "left").select($"id", $"cid", $"vids", $"query", $"count")
      .flatMap(line =>{
        val id = line.getString(0)
        val vids = if(id.length == 11) {
          Array(id)
        } else if(!line.isNullAt(1)) {
          line.getSeq[String](2).toArray
        } else {
          Array("")
        }
        vids.map(vid=>(vid, line.getString(3), line.getLong(4)))
      }).filter(_._1 != "")
      .toDF("vid", "query", "count")
      .groupBy("vid", "query").agg(sum($"count") as "count").filter($"count" > 20)
      // 开始拆分
      .mapPartitions(part => {
        Config.IOAdapter = new HadoopFileIoAdapter()
        val stop_words_p = stop_words_broadcast.value
        for(s <- tags_broadcast.value) {
          try {
            CustomDictionary.add(s)
          } catch {  // 有exception 暂时不捕捉
            case ex: ArrayIndexOutOfBoundsException => println("this key leads to ArrayIndexOutOfBoundsException: " + s)
            case ex: NullPointerException => println("this key leads to NullPointerException: " + s)
          }
        }
        //val segment_broadcast = HanLP.newSegment().enableCustomDictionary(true)
        //暂时选用crf分词
        val segment_crf = new CRFSegment().enablePartOfSpeechTagging(true).enableCustomDictionary(true)
        part.flatMap(f = row => {
          val vid = row.getAs[String]("vid")
          val query = row.getAs[String]("query").toLowerCase
          val count = row.getAs[Long]("count")
          val crf = segment_crf.seg(query)
            .filter(_.word.length >= 2)
            .filter(line => useful_set.keySet.contains(line.nature.toString)) //按照词性过滤一遍

            .map(_.word.trim)
            .filter(w => !stop_words_p.contains(w))  //  按照停止词过滤一遍
            .map(word => (vid, word, count))

          crf.map(w=> {((w._1, w._2), w._3)})
        })
      })
      .toDF("vid_query", "count").cache
    println(s"get vid_query_count temp file, number: ${vid_qtag_data_temp.count()}")
    vid_qtag_data_temp.show

    val vid_qtag_data_temp2 = vid_qtag_data_temp
      .groupBy("vid_query").agg(sum($"count") as "count").cache
    println(s"show temp_data2, number: ${vid_qtag_data_temp2.count()}")
    vid_qtag_data_temp2.show

    val vid_qtag_data = vid_qtag_data_temp2

      // 替换query为id
      .join(vid_pioneer_tag,
      vid_qtag_data_temp2("vid_query") === vid_pioneer_tag("vid_t_pioneer_tag"),
      "left")
      .select($"vid_query",  $"vid_t_pioneer_tag", $"count", $"pioneer_tag_id")
      .map(line => {
        val vid_query = line.getAs[Row]("vid_query")
        val vid = vid_query.getString(0)
        val query = vid_query.getString(1)

        val count = line.getAs[Long]("count")
        val tag_id = if (line.isNullAt(1)) {
          Hash.hash_own(query)
        } else {
          line.getAs[Long]("pioneer_tag_id")
        }
        (vid, tag_id.toString, count)
      })
      .toDF("vid", "query", "count")
      .cache
//    vid_qtag_data.show()
    println(s"get vid qtag data done, number: ${vid_qtag_data.count}.")

    val vid_qtags = vid_qtag_data.groupBy($"vid").agg(collect_list(tuple2($"query", $"count")) as "query_count", max($"count") as "max_count")
      .map(line=>{
        val vid = line.getString(0)
        val max_count = line.getLong(2)
        val query_count = line.getSeq[Row](1)
          .map(r => (r.getString(0), r.getLong(1)))

          .sortBy(_._2)(Ordering[Long].reverse)
          .take(100)
          .map(tp => (tp._1, tp._2.toDouble / max_count))
          .filter(_._2 > 0.01)  // 限制太小的点击率的就不要了
        KeyValueWeight(vid, query_count)
      }).cache
    vid_qtags.coalesce(REPARTITION_NUM).write.mode("overwrite").parquet(vid_qtag_output_path)
    println(s"put vid_qtags to path $vid_qtag_output_path, number: ${vid_qtags.count()}: ")
    vid_qtags.show()



    // qtag-vids要过滤
    val qtag_vids = vid_qtag_data.join(vid_filter, "vid")
      .groupBy($"query").agg(collect_list(tuple2($"vid", $"count")) as "vid_count", max($"count") as "max_count")
      .map(line=>{
        val query = line.getString(0)
        val max_count = line.getLong(2)
        val vid_count = line.getSeq[Row](1)
          .map(r => (r.getString(0), r.getLong(1)))

          .sortBy(_._2)(Ordering[Long].reverse)
//          .take(100)
          .map(tp => (tp._1, tp._2.toDouble / max_count))
          .filter(_._2 > 0.01)   // 限制太小的点击率的就不要了
        KeyValueWeight(query, vid_count)
      }).cache
    qtag_vids.coalesce(REPARTITION_NUM).write.mode("overwrite").parquet(qtag_vid_output_path)
    println(s"put qtag_vids to path $qtag_vid_output_path, number: ${qtag_vids.count}")
    //qtag_vids.show()

  }



  // 不过内容池，在写的时候再过内容池，加入vid的播控信息
  def join_vid_tag(spark: SparkSession, vtag_path: String, qtag_path: String, video_info_path: String, output_path: String): Unit = {
    import spark.implicits._
    // play_control data
    val play_control_data = spark.sparkContext.textFile(video_info_path)
      .map(line => line.split("\t", -1))
      .filter(_.length > 116)
      .filter(line => line(59) == "4") // 在线上
      .filter(line=>line(1).length == 11) //vid长度为11
      .map(arr => {
      val vid = arr(1)

      /**
      1505276 - 低俗
      1512774 - 正常
      1530844 - 惊悚
      1565745 - 恶心
        * */
      val video_pic_scale = arr(99).toInt // '视频图片尺度', 99

      /**
      1505275 - 低俗
      1512760 - 标题党
      1512772 - 正常
      1565744 - 恶心
        * */
      val video_title_scale = arr(113).toInt  // 标题尺度

      /**
      1487112 - 正常
      1487114 - 低俗
      1493303 - 惊悚
      1494973 - 暴力
      1565743 - 恶心
        * */
      val content_level = arr(87).toInt // 内容尺度

    })

    val vtag_data = spark.read.parquet(vtag_path)
      .map(line=>{
        val vid = line.getString(0)
        val value_weight = line.getAs[Seq[Row]](2)
          .map(v=>(v.getLong(1).toString, v.getDouble(2))).distinct.sortWith(_._2 > _._2) //_1 tag _2 tag_id _3 weight
          .take(100)
        KeyValueWeight(vid, value_weight)
      }).toDF("vid_v", "vtag_weight")

    // qtag data
    val qtag_data = spark.read.parquet(qtag_path).as[KeyValueWeight].toDF("vid_q", "qtag_weight")

    val joined_data = vtag_data.join(qtag_data, $"vid_v" === qtag_data("vid_q"), "outer")
      .select($"vid_v", $"vtag_weight", $"vid_q", $"qtag_weight")
      .map(line =>{
        val vid = if(line.isNullAt(0)) {
          line.getString(2)
        } else {
          line.getString(0)
        }
        val value_weight = if(line.isNullAt(0)) {  // 没有vtag
          line.getSeq[Row](3).map(row=>(row.getString(0), row.getDouble(1)))
            .sortBy(_._2)(Ordering[Double].reverse)
            .take(100)
        } else if (line.isNullAt(2)) {  // 没有qtag
          line.getSeq[Row](1).map(row=>(row.getString(0), row.getDouble(1)))
            .sortBy(_._2)(Ordering[Double].reverse)
            .take(100)
        } else {
          val vtags = line.getSeq[Row](1).map(row=>(row.getString(0), row.getDouble(1)))
            .sortBy(_._2)(Ordering[Double].reverse)
            .take(100).toMap
          val qtags = line.getSeq[Row](3).map(row=>(row.getString(0), row.getDouble(1)))
            .sortBy(_._2)(Ordering[Double].reverse)
            .take(100).toMap

          val tags_result = (vtags /: qtags) {
            case (map, (k, v: Double)) =>
              map + ( k -> (v + map.getOrElse(k, 0.0)) )
          }
          tags_result.toSeq.sortBy(_._2)(Ordering[Double].reverse)
            .take(100)
        }
        KeyValueWeight(vid, value_weight.map(vw => (vw._1, Tools.normalize(vw._2)))) // 归一化一下
      }).cache
    println(s"get vtag & qtag joined data to $output_path, number: ${joined_data.count()}")
    joined_data.write.mode("overwrite").parquet(output_path)
  }

  def join_tag_vid(spark: SparkSession,
                   vtag_path: String,
                   qtag_path: String,
                   ctr_path: String,
                   output_path: String
                  ): Unit = {
    import spark.implicits._
    println("--------------[join_tag_vid]--------------")
    val vtag_data = spark.read.parquet(vtag_path).as[KeyValueWeight].cache()
    println(s"join_tag_vid: get vtag data, tag number: ${vtag_data.count()}")

    val vtag_data_flat = vtag_data.flatMap(line => {
      val vtagid = line.key
      val vid_weight = line.value_weight.map(vw => (vtagid, vw._1, vw._2))
      vid_weight  // vtagid vid weight
    }).toDF("tagid", "vid", "weight").cache


    val qtag_data = spark.read.parquet(qtag_path).as[KeyValueWeight].cache
    println(s"join_tag_vid: get qtag data, tag number: ${qtag_data.count()}")

    val qtag_data_flat = qtag_data.flatMap(line => {
      val vtagid = line.key
      val vid_weight = line.value_weight.map(vw => (vtagid, vw._1, vw._2))
      vid_weight  // qtagid vid weight
    }).toDF("tagid", "vid", "weight").cache

    val ctr_data = spark.read.parquet(ctr_path).select($"vid" as "vid_t", $"ctr").cache

    val tag_vid_data = vtag_data_flat.union(qtag_data_flat).groupBy($"tagid", $"vid").agg(sum($"weight") as "weight")
    val tag_vid_data_with_ctr = tag_vid_data.join(ctr_data, $"vid" === ctr_data("vid_t"), "left")

      .select($"tagid", $"vid", $"weight", $"ctr")
      .withColumn("weight_after", when($"ctr".isNotNull && $"ctr" > $"weight" * 0.1, $"ctr").otherwise($"weight" * 0.1))
      .withColumn("weight_type", when($"ctr".isNotNull && $"ctr" > $"weight" * 0.1, "ctr").otherwise("weight"))
        .select($"tagid", $"vid", $"weight_after" as "weight", $"weight_type")
      .cache
    println("we get tag_vid_data_with_ctr, show:")
    tag_vid_data_with_ctr.sample(withReplacement = true, 0.01).show(50)

    val result = tag_vid_data_with_ctr
      .groupBy($"tagid").agg(collect_list(struct($"vid", $"weight")) as "vid_weight")
      .map(line => {
        val tagid = line.getString(0)
        val vid_weight = line.getSeq[Row](1).map(r => (r.getString(0), r.getDouble(1))).sortBy(_._2).take(200)
        KeyValueWeight(tagid, vid_weight)
      }).cache
    println(s"join_tag_vid done, put to path $output_path, number: ${result.count}")
    result.write.mode("overwrite").parquet(output_path)

  }

  /**
    * write to redis
    * @param path vid-tags 总的正排数据
    * @param filter_path vid_filter的位置，需要过滤下内容池再写入
    * @param control_flags put or delete
    * @param data_type put key 的版本号
    *
    * */
  def put_vid_tag_to_redis(spark: SparkSession,
                           path : String,
                           filter_path: String,
                           control_flags: Set[String],
                           data_type: String = "v0"): Unit = {

    if(control_flags.contains("delete") || control_flags.contains("put")) {
      println("put vid_tag to redis")
      import spark.implicits._
      val ip = "100.107.17.229" // zkname sz1162.short_video_vd2tg.redis.com
      val port = 9014
      val bzid = "sengine"
      val prefix = s"${data_type}_sv_vd2tg"
      val tag_type: Int = 2501
      // filter the vid
      val vid_filter = spark.read.json(filter_path).toDF("key").cache
      vid_filter.show

      val data_all = spark.read.parquet(path).as[KeyValueWeight]

      val data = data_all.map(line => {
        val vid = line.key
        val value_weight = line.value_weight
          .distinct.sortBy(_._2)(Ordering[Double].reverse) //_1 tag _2 tag_id _3 weight
          .take(100)
        KeyValueWeight(vid, value_weight)
      })
        .join(vid_filter, "key").as[KeyValueWeight]
        .cache




      val test_redis_pool = new TestRedisPool(ip, port, 40000)
      val broadcast_redis_pool = spark.sparkContext.broadcast(test_redis_pool)
      if (control_flags.contains("delete")) {
        Tools.delete_to_redis(data, broadcast_redis_pool, bzid, prefix, tag_type /*, limit_num = 1000 */)
      }
      if (control_flags.contains("put")) {
        Tools.put_to_redis(data, broadcast_redis_pool, bzid, prefix, tag_type /*, limit_num = 1000 */)
      }
      println("put to redis done, number: " + data.count)
      Tools.stat(spark, data, s"V1_test_$data_type")
    }
  }

  def put_tag_vid_to_redis(spark: SparkSession, path : String, control_flags:Set[String], data_type: String = "v0"): Unit = {
    val res_path = path
    if(control_flags.contains("delete") || control_flags.contains("put")) {
      println("put tag_vid to redis")
      import spark.implicits._

      val ip = "100.107.18.31" // zkname sz1163.short_video_tg2vd.redis.com
      val port = 9000
      //val limit_num = 1000
      val bzid = "sengine"
      val prefix = s"${data_type}_sv_tg2vd"
      val tag_type: Int = -1 // 不需要字段中加入tag_type,就设置成-1

      val data = spark.read.parquet(res_path).as[KeyValueWeight].cache()
      println(s"put tag_vid to redis, data_type: $data_type, number： ${data.count()}")
      //data.collect().foreach(line=>println(line.key))
      //println("\n\n\n\n")
      val test_redis_pool = new TestRedisPool(ip, port, 40000)
      val broadcast_redis_pool = spark.sparkContext.broadcast(test_redis_pool)
      if (control_flags.contains("delete")) {
        Tools.delete_to_redis(data, broadcast_redis_pool, bzid, prefix, tag_type /*, limit_num = 1000 */)
      }
      if (control_flags.contains("put")) {
        Tools.put_to_redis(data, broadcast_redis_pool, bzid, prefix, tag_type /*, limit_num = 1000 */)
      }
      println("put tag_vid to redis done")
      Tools.stat(spark, data, s"T1_test_$data_type")
    }
  }

  def main(args: Array[String]) {
    val spark = SparkSession
      .builder
      .appName(this.getClass.getName.split("\\$").last)
      .getOrCreate()


    val vid_input_path = args(0)
    val cid_input_path = args(1)
    val vid_filter_path = Tools.get_latest_subpath(spark, args(2))  // 需要找最近的
    val ctr_path = Tools.get_latest_subpath(spark, args(3))
    val source_4141_path = args(4)
    val tags_info_path = args(5)
    val stop_words_path = args(6)
    val vid_length = args(7).toInt
    val output_path = args(8)
    val date_str = args(9)
    val control_flags = args(10).split(Defines.FLAGS_SPLIT_STR, -1).toSet
    val data_type = args(11)
    println("------------------[begin]-----------------")
    println(s"control flags: ${control_flags.mkString("||")}")
    val vid_tags_path = output_path + "/vid_tags/" + date_str
    if(control_flags.contains("shuffle")) {
      vid_tag_shuffle(spark, vid_input_path, cid_input_path, vid_tags_path)
    }

    val vid_idf_path = output_path + "/vid_idf_path/" + date_str
    if(control_flags.contains("idf")) {
      vid_tf_idf(spark, vid_tags_path, vid_idf_path)
    }

    val idf_join_path = output_path + "/idf_join_path/" + date_str
    if(control_flags.contains("idf_join")) {
      tf_idf_join_v2(spark, vid_idf_path, idf_join_path)
    }

    val clean_output_path = output_path + "/cleaned_data/" + date_str
    if(control_flags.contains("cleaned_data")) {
      tf_idf_to_tuple(spark, idf_join_path, clean_output_path)
    }

    val vtag_vid_path = output_path + "/tag_vid/" + date_str
    if(control_flags.contains("tag_vid")) {
      get_tag_vid_data(spark,
        clean_output_path,
        vid_filter_path = vid_filter_path,
        vtag_vid_path,
        vid_length)
    }

    val vid_qtag_path = output_path + "/vid_qtag/" + date_str
    val qtag_vid_path = output_path + "/qtag_vid/" + date_str
    if(control_flags.contains("qtag")) {

      get_vid_qtag(spark,
        source_4141_path = source_4141_path,
        vid_info_path = vid_input_path,
        cid_info_path = cid_input_path,
        tags_info_path = tags_info_path,
        vid_filter_path = vid_filter_path,
        stop_words_path = stop_words_path,
        vid_qtag_output_path = vid_qtag_path,
        qtag_vid_output_path = qtag_vid_path
      )
    }

    val tags_output = output_path + "/all_vid_tags/" + date_str
    val tags_vid_output = output_path + "/all_tags_vid/" + date_str
    if(control_flags.contains("join_vid_tags")) {
//      join_vid_tag(spark, vtag_path = clean_output_path, qtag_path = vid_qtag_path, output_path = tags_output)
      join_tag_vid(spark, vtag_path = vtag_vid_path, qtag_path = qtag_vid_path, ctr_path = ctr_path, output_path = tags_vid_output)
    }

    put_vid_tag_to_redis(spark, path = tags_output, filter_path = vid_filter_path, control_flags = control_flags, data_type = data_type)
    put_tag_vid_to_redis(spark, path = tags_vid_output, control_flags = control_flags, data_type = data_type)


    println("------------------[done]-----------------")
  }

}
