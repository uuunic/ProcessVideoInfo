package BasicData

import Utils.Defines._
import Utils.Hash.hash_tf_idf
import Utils.Tools.KeyValueWeight
import Utils._
import com.hankcs.hanlp.HanLP.Config
import com.hankcs.hanlp.dictionary.CustomDictionary
import com.hankcs.hanlp.seg.CRF.CRFSegment
import com.huaban.analysis.jieba.JiebaSegmenter.SegMode
import com.huaban.analysis.jieba.{JiebaSegmenter, SegToken}
import org.apache.spark.ml.feature.{HashingTF, IDF}
import org.apache.spark.ml.linalg.{SparseVector => SV}
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

import scala.collection.JavaConversions._
import scala.collection.mutable.ArrayBuffer
import scala.util.matching.Regex

/**
  * Created by baronfeng on 2017/9/15.
  */
object VidTagsWeightV3 {
  //分词工具
  val segmenter = new JiebaSegmenter
  val REPARTITION_NUM = 400

  val PIONEER_TAG_INDEX = 88  // 先锋标签的index位置
  val PIONEER_TAG_ID_INDEX = 89 //先锋标签对应的id位置
  val digit_regex: Regex = """^\d+$""".r  //纯数字 正则表达式

  val cid_tags_word: String = "cid_tags"

  case class LineInfo(index:Int, weight:Double, split_str:String, is_distinct:Boolean)

  case class DsVidLine(vid: String, duration: Int, tag_tagid:Seq[(String, Long, String)],  weight:Double, is_distinct:Boolean)

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
    cid_tags_word -> LineInfo(-1, 1.0, "", is_distinct = false)
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


// dataframe: tag_t: String, tagid: Long
  def get_tag_tagid(spark: SparkSession, tag_info_path: String): DataFrame = {
    import spark.implicits._
    // tag:poineer_tag_id对应关系，其中tag会有多个id对应  也就是String, Seq[Int]的风格

    val tag_tagid = spark.read.textFile(tag_info_path)
      .map(_.split("\t", -1))
      .filter(line => line.length == 14 && line(6).toInt == 1) // 只要线上的
      .map(r => (r(1).toLong, r(2))).toDF("tagid", "tag_t").distinct
      .groupBy("tag_t").agg(collect_list($"tagid") as "tagid", count($"tagid") as "tagid_number")
      .filter($"tagid_number" === 1)
      .select($"tag_t", explode($"tagid") as "tagid")
      .repartition(REPARTITION_NUM).cache

    println(s"get the tag_tagid from $tag_info_path, number: ${tag_tagid.count()}")
    tag_tagid
  }

  /**
    * @param vid_info_path vid_info的位置，目前为/data/stage/outface/omg/tdw/export_video_t_video_info_extend_hour/ds=YYYYMMDDHH
    * @param cid_info_path cid_info的位置，目前为/data/stage/outface/omg/export_video_t_cover_info_extend_hour/ds=YYYYMMDD23  注意只有23点的时候落地了
    * @param vid_tags_output_path 输出位置，目前为BasicData/VidTagsWeight/YYYYMMDD/${channel}/
    * */
  def vid_tag_shuffle(spark: SparkSession,
                      vid_info_path: String,
                      cid_info_path: String,
                      tag_info_path: String,
                      vid_tags_output_path: String) : Int = {
    import spark.implicits._

    println("---------[begin to collect vid tags]----------")

    val stop_words = spark.sparkContext.broadcast(get_stop_words(spark))


    // vid基础数据，只要线上的，全量
    val vid_info = spark.sparkContext.textFile(vid_info_path)
      .map(line => line.split("\t", -1))
      .filter(_.length > 116)
      .filter(line => line(59) == "4") // 在线上
      .filter(line => line(1).length == 11) //vid长度为11
      .repartition(REPARTITION_NUM)
      .cache() // line(59) b_state

    // tag:poineer_tag_id对应关系，其中tag会有多个id对应  也就是String, Seq[Int]的风格

    val tag_tagid = get_tag_tagid(spark, tag_info_path = tag_info_path)


    for ((tag_name, info) <- vid_useful_col if tag_name != cid_tags_word) {
      println("begin to get tags type: " + tag_name)
      val tag_info_line = vid_info.flatMap(line => {
        val vid = line(1)
        val duration: Int = if (line(57).equals("0") || line(57).equals("")) 0 else line(57).toInt
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
        val ret_tags_without_digit = ret_tags.filter(digit_regex.findFirstMatchIn(_).isEmpty)
        ret_tags_without_digit.map(tag => (vid, duration, tag))
      }).toDF("vid", "duration", "tag")
        .join(tag_tagid, $"tag" === tag_tagid("tag_t"), "left")
        .select("vid", "duration", "tag", "tagid")
        .map(line => {
          val vid = line.getString(0)
          val duration = line.getInt(1)
          val tag = line.getString(2)
          val tagid = if (line.isNullAt(3)) Hash.hash_own(tag) else line.getLong(3)
          val tag_type = if (line.isNullAt(3)) "inner_tag" else "pioneer_tag"
          (vid, duration, tag, tagid, tag_type)
        }).toDF("vid", "duration", "tag", "tagid", "tag_type")
        .groupBy($"vid").agg(max($"duration") as "duration", collect_list(struct($"tag", $"tagid", $"tag_type")) as "tag_tagid")
        .withColumn("weight", lit(info.weight)).withColumn("is_distinct", lit(info.is_distinct))
        .select($"vid", $"duration", $"tag_tagid", $"weight", $"is_distinct")
        .as[DsVidLine]

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
      vids.map(vid => (vid, tags))
    })
      .filter(_._2.nonEmpty)
      .flatMap(vinfo => {
        val vid = vinfo._1
        val tags = vinfo._2
        tags.map(tag => (vid, tag))
      })
      .toDF("vid", "tag")
      .join(tag_tagid, $"tag" === tag_tagid("tag_t"), "left")
      .select("vid", "tag", "tagid")
      .map(line => {
        val vid = line.getString(0)
        val tag = line.getString(1)
        val tagid = if (line.isNullAt(2)) Hash.hash_own(tag) else line.getLong(2)
        val tag_type = if (line.isNullAt(2)) "inner_tag" else "pioneer_tag"
        (vid, tag, tagid, tag_type)
      }).toDF("vid", "tag", "tagid", "tag_type")
      .groupBy($"vid").agg(lit(0: Int) as "duration", collect_list(struct($"tag", $"tagid", $"tag_type")) as "tag_tagid")
      .withColumn("weight", lit(1.0: Double)).withColumn("is_distinct", lit(false))
      .select($"vid", $"duration", $"tag_tagid", $"weight", $"is_distinct")
      .as[DsVidLine]

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
  val vid_idf_useful_strs = Array("vid", "duration", "tag_tagid", "tag_id", "weight", "features")
  case class TagWeightInfo(tag: String, tagid: Long, weight: Double, tag_type: String)
  def vid_tf_idf(spark:SparkSession, vid_tags_path:String, idf_output_path: String) : Unit = {
    import spark.implicits._
    println("---------[begin to process tf-idf]----------")
    val hashingTF = new HashingTF().setInputCol("tag_id").setOutputCol("tag_raw_features").setNumFeatures(TAG_HASH_LENGTH)
    val idf = new IDF().setInputCol("tag_raw_features").setOutputCol("features")

    for (tag_name <- vid_useful_col.keys) {
      printf(" begin [%s] idf", tag_name)
      val tag_data = spark.read.parquet(vid_tags_path + "/" + tag_name).as[DsVidLine]
          .withColumn("tag_id", $"tag_tagid.tagid")
          .coalesce(REPARTITION_NUM)
          .cache()

      printf(" read [%s] done, count: %d, begin process\n", tag_name, tag_data.count())
     // tag_data.printSchema()
      val newSTF = hashingTF.transform(tag_data)

      //下面计算IDF的值
      val idfModel = idf.fit(newSTF)
      val rescaledData = idfModel.transform(newSTF)
        // select 要求至少有一个元素，所以用这种脏一些的方法来处理
        .select(vid_idf_useful_strs(0), vid_idf_useful_strs.drop(1):_*)

        .map(line => {
          val vid = line.getAs[String]("vid")
          val duration = line.getAs[Int]("duration")
          // tag tagid tag_type
          val tag_tagid = line.getAs[Seq[Row]]("tag_tagid").map(row => (row.getString(0), row.getLong(1), row.getString(2)))
          val tag_id = line.getAs[Seq[Long]]("tag_id")
          val weight = line.getAs[Double]("weight")

          val features = line.getAs[SV]("features")
          val tagidhash_weight = features.indices.zip(features.values).toMap
          val tag_info_res = tag_tagid.map(tp => {
            val tag = tp._1
            val tagid = tp._2
            val tag_type = tp._3
            // 这个是tagid的hash值，也是最后的features里的index的位置
            val tagid_hash = hash_tf_idf(tagid)
            val tagid_weight = tagidhash_weight.getOrElse(tagid_hash, 0.0)
            TagWeightInfo(tag, tagid, tagid_weight * weight, tag_type)
          })
          (vid, duration, tag_info_res)
        }).toDF("vid", "duration", "tag_info")

      rescaledData.write.mode("overwrite").parquet(idf_output_path + "/" + tag_name)
      println("write idf source data done. type: "+ tag_name)
    }
    println("--------[idf done. begin join all vid data]--------")

  }

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

    //$"vid", $"duration", $"tags", $"tag_info"
    val vid_result = vid_total.coalesce(REPARTITION_NUM)
      .groupBy($"vid").agg(max($"duration") as "duration", collect_list($"tag_info") as "tag_info")
      .map(line => {
        val vid = line.getAs[String]("vid")
        val duration = line.getAs[Int]("duration")
        val tag_infos = line.getAs[Seq[Seq[Row]]]("tag_info").flatten
            .map(info => TagWeightInfo(info.getString(0), info.getLong(1), info.getDouble(2), info.getString(3)))
          .groupBy(x => (x.tag, x.tagid, x.tag_type))
          .map(s => TagWeightInfo(s._1._1, s._1._2, s._2.map(_.weight).sum, s._1._3))
          .toSeq.sortBy(_.weight)(Ordering[Double].reverse)
          .take(30)
        val weight_max = tag_infos.map(_.weight).max
        (vid, duration, tag_infos.map(tg => TagWeightInfo(tg.tag, tg.tagid, tg.weight / weight_max, tg.tag_type)))
      }).toDF("vid", "duration", "tag_info")

    vid_result.printSchema()
    vid_result.show(false)


    println(s"begin to write vid_tags_total to path: $join_output_path, number: ${vid_result.count()}")
    vid_result.write.mode("overwrite").parquet(join_output_path)
    println("--------[join all vid data done.]--------")
  }



  // output_path + "/tf_idf_wash_path"
  /**
    * tag-vid倒排数据，目前依赖数据源为idf_join_path
  * */
  def get_vtag_vid_data(spark: SparkSession,
                       vid_tag_path : String,
                       vid_filter_path: String,
                       tag_vid_output_path: String,
                       vid_length: Int): Unit = {
    import spark.implicits._
    spark.sqlContext.udf.register("tuple_vid_weight", (vid: String, weight: Double)=> {  //(String, Long, Double)
      (vid, weight)
    })

    println("get tag_vid data, every tag has at most [" + vid_length +"] vids.")
    val data = spark.read.parquet(vid_tag_path).flatMap(line => {
      val vid = line.getString(0)
      val tag_weight = line.getAs[Seq[Row]](2)
        .map(v => TagWeightInfo(v.getString(0), v.getLong(1), v.getDouble(2), v.getString(3))) //_1 tag _2 tag_id _3 weight

      tag_weight.map(info => (info.tag, info.tagid, info.tag_type, info.weight, vid))
    })
      .toDF("tag", "tagid", "tag_type", "weight", "vid")


    val vid_filter = spark.read.json(vid_filter_path).toDF("vid").cache
    println(s"get_vtag: get vid_filter from [$vid_filter_path], number: ${vid_filter.count}")
    // 倒排数据过滤内容池
    val data_filtered = data.join(vid_filter, "vid").select("tag", "tagid", "tag_type", "weight", "vid")

    val res_data = data_filtered.groupBy($"tag", $"tagid", $"tag_type").agg(collect_list(struct($"vid", $"weight")) as "vid_weight")

    res_data.repartition(REPARTITION_NUM / 2).write.mode("overwrite").parquet(tag_vid_output_path)
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
    val real_tags_path = tags_path
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

    val pioneertag_tagid = get_tag_tagid(spark, tags_info_path)

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

          crf.map(w=> {(w._1, w._2, w._3)})
        })
      })
      .toDF("vid", "query", "count").cache
    println(s"get vid_query_count temp file, number: ${vid_qtag_data_temp.count()}")
    vid_qtag_data_temp.show

    val vid_qtag_data_temp2 = vid_qtag_data_temp
      .groupBy("vid", "query").agg(sum($"count") as "count").cache
    println(s"show temp_data2, number: ${vid_qtag_data_temp2.count()}")
    vid_qtag_data_temp2.show

    val vid_qtag_data = vid_qtag_data_temp2

      // 替换query为id
      .join(pioneertag_tagid,
      vid_qtag_data_temp2("query") === pioneertag_tagid("tag_t"),
      "left")
      .select($"vid", $"query",  $"count", $"tag_t", $"tagid")
      .map(line => {
        val vid = line.getAs[String]("vid")
        val query = line.getAs[String]("query")

        val count = line.getAs[Long]("count")

        val tagid = if (line.isNullAt(3)) {
          Hash.hash_own(query)
        } else {
          line.getAs[Long]("tagid")
        }

        val tag_type = if (line.isNullAt(3)) {
          "inner_tag"
        } else {
          "pioneer_tag"
        }
        (vid, query, tagid, tag_type, count)
      })
      .toDF("vid", "tag", "tagid", "tag_type", "count")
      .cache
//    vid_qtag_data.show()
    println(s"get vid qtag data done, number: ${vid_qtag_data.count}.")

    val vid_qtags = vid_qtag_data
      .groupBy($"vid").agg(collect_list(struct($"tag", $"tagid", $"tag_type",$"count")) as "tag_count", max($"count") as "max_count")
      .map(line=>{
        val vid = line.getAs[String]("vid")
        val max_count = line.getAs[Long]("max_count")
        val tag_count = line.getSeq[Row](1)
          .map(r => (r.getString(0), r.getLong(1), r.getString(2), r.getLong(3)))

          .sortBy(_._4)(Ordering[Long].reverse)
          .take(30)
          .map(tp => TagWeightInfo(tp._1, tp._2, tp._4.toDouble / max_count, tp._3))
          .filter(_.weight > 0.01)  // 限制太小的点击率的就不要了
        (vid, tag_count)
      }).toDF("vid", "tag_weight").cache
    vid_qtags.coalesce(REPARTITION_NUM).write.mode("overwrite").parquet(vid_qtag_output_path)
    println(s"put vid_qtags to path $vid_qtag_output_path, number: ${vid_qtags.count()}: ")
    vid_qtags.show()

    // qtag-vids要过滤
    val qtag_vids = vid_qtag_data.join(vid_filter, "vid")
      .groupBy($"tag", $"tagid", $"tag_type").agg(collect_list(struct($"vid", $"count")) as "vid_count", max($"count") as "max_count")
      .map(line=>{
        val tag = line.getAs[String]("tag")
        val tagid = line.getAs[Long]("tagid")
        val tag_type = line.getAs[String]("tag_type")

        val max_count = line.getAs[Long]("max_count")

        val vid_weight = line.getAs[Seq[Row]]("vid_count")
          .map(r => (r.getString(0), r.getLong(1)))
          .sortBy(_._2)(Ordering[Long].reverse)
//          .take(100)
          .map(tp => (tp._1, tp._2.toDouble / max_count))
          .filter(_._2 > 0.01)   // 限制太小的点击率的就不要了
        (tag, tagid, tag_type, vid_weight)
      }).toDF("tag", "tagid", "tag_type", "vid_weight")
      .cache
    qtag_vids.coalesce(REPARTITION_NUM).write.mode("overwrite").parquet(qtag_vid_output_path)
    println(s"put qtag_vids to path $qtag_vid_output_path, number: ${qtag_vids.count}")
    //qtag_vids.show()

  }

  case class VidTagInfoList(vid: String, tag_info_list: Seq[TagWeightInfo])
  val hash_tag: UserDefinedFunction = udf((tag: String) => Hash.hash_own(tag))
  // 不过内容池，在写的时候再过内容池
  def join_vid_tag(spark: SparkSession, vtag_path: String, qtag_path: String, output_path: String): Unit = {
    import spark.implicits._
    // vid duration TagWeightInfo
    val vtag_source = spark.read.parquet(vtag_path).cache()
    val vtag_data = vtag_source

        .select($"vid", $"tag_info")
      .toDF("vid_v", "vtag_weight")

    // 这是vtag的tag和tagid
    val vtag_tagid = vtag_source.flatMap(line => {
      line.getAs[Seq[Row]](2)
        .map(v => (v.getString(0), v.getLong(1)))
    }).distinct()
      .toDF("tag", "tagid")
      .withColumn("tagid_compute", hash_tag($"tag") as "tagid_compute")
      .flatMap(row => {
        val tag = row.getAs[String]("tag")
        val tagid = row.getAs[Long]("tagid")
        val tagid_compute = row.getAs[Long]("tagid_compute")
        if(tagid == tagid_compute)
          Array(tagid).map(id => (tag, id))
        else
          Array(tagid, tagid_compute).map(id => (tag, id))
      }).distinct()
      .toDF("tag", "tagid")

    //vid: String, tag_weight:TagWeightInfo
    val qtag_data = spark.read.parquet(qtag_path).toDF("vid_q", "qtag_weight").cache()

    // 这是vtag的tag和tagid
    val qtag_tagid = qtag_data.select(explode($"qtag_weight") as "qtag_weight")
      .flatMap(line => {
        val qtag_info_source = line.getAs[Row]("qtag_weight")
        val qtag_info = TagWeightInfo(qtag_info_source.getString(0), qtag_info_source.getLong(1), qtag_info_source.getDouble(2), qtag_info_source.getString(3))
        val qtag = qtag_info.tag
        val qtag_id = qtag_info.tagid
        val qtag_inner_id = Hash.hash_own(qtag)
        if (qtag_id == qtag_inner_id)
          Array(qtag_id).map(id => (qtag, id))
        else
          Array(qtag_id, qtag_inner_id).map(id => (qtag, id))

      }).distinct()
      .toDF("tag", "tagid")

    val tag_tagid_output_path = "pm_data/tag_tagid/20171121"
    println(s"write tag_tagid to $tag_tagid_output_path")
    vtag_tagid.union(qtag_tagid).distinct().write.mode("overwrite").parquet(tag_tagid_output_path)


    println(s"begin to join vtag and qtag")
    val joined_data_temp = vtag_data.join(qtag_data, $"vid_v" === qtag_data("vid_q"), "outer")
      .select($"vid_v", $"vtag_weight", $"vid_q", $"qtag_weight").cache()
    println("joined data schema and show")
    joined_data_temp.printSchema()
    joined_data_temp.show
    val joined_data = joined_data_temp
      .map(line =>{
        val vid = if(line.isNullAt(0)) {
          line.getString(2)
        } else {
          line.getString(0)
        }

        val value_weight = if(line.isNullAt(0)) {  // 没有vtag
          line.getSeq[Row](3)
            .map(r=>TagWeightInfo(r.getString(0), r.getLong(1), r.getDouble(2), r.getString(3)))
            .sortBy(_.weight)(Ordering[Double].reverse)
            .take(30)
        } else if (line.isNullAt(2)) {  // 没有qtag
          line.getSeq[Row](1)
            .map(r=>TagWeightInfo(r.getString(0), r.getLong(1), r.getDouble(2), r.getString(3)))
            .sortBy(_.weight)(Ordering[Double].reverse)
            .take(30)
        } else {
          val vtags = line.getSeq[Row](1)
            .map(r => TagWeightInfo(r.getString(0), r.getLong(1), r.getDouble(2), r.getString(3)))
            .sortBy(_.weight)(Ordering[Double].reverse)
            .take(30)
            .map(t => (t.tag, t.tagid, t.tag_type)->(t.weight))
            .toMap

          val qtags = line.getSeq[Row](3)
            .map(r => TagWeightInfo(r.getString(0), r.getLong(1), r.getDouble(2), r.getString(3)))
            .sortBy(_.weight)(Ordering[Double].reverse)
            .take(30)
            .map(t => (t.tag, t.tagid, t.tag_type)->(t.weight))
            .toMap

          val tags_result = (vtags /: qtags) {
            case (map, (k, v: Double)) =>
              map + (k -> (v + map.getOrElse(k, 0.0)))
          }
          tags_result.map(kv => TagWeightInfo(kv._1._1, kv._1._2, kv._2, kv._1._3))
                .toSeq.sortBy(_.weight)(Ordering[Double].reverse)
            .take(30)
        }
        val value_weight_normal = value_weight.map(v => TagWeightInfo(v.tag, v.tagid, Tools.normalize(v.weight), v.tag_type))

        VidTagInfoList(vid, value_weight_normal) // 归一化一下
      }).cache
    println(s"get vtag & qtag joined data to $output_path, number: ${joined_data.count()}")

    joined_data.write.mode("overwrite").parquet(output_path)
  }

  // Row 特指目前的vid_info里面的row，之后可能会泛化。
  case class TagInfo(tag: String, tag_id: Long, weight: Double)

  def filter_vid_vtags_line(line: Row): (String, Int, Seq[TagInfo]) = { // tag, tag_id, weight

    val vid = line.getString(0)
    val duration = if(line.isNullAt(1)) 0 else line.getInt(1)
    val tag_info = line.getSeq[Row](2).map(info => TagInfo(info.getString(0), info.getLong(1), info.getDouble(2)))
    (vid, duration, tag_info.sortBy(_.weight)(Ordering[Double].reverse))
  }

  /**将join_data 转换为clean_data*/
  def tf_idf_to_tuple(spark: SparkSession, join_path: String, clean_output_path: String, video_info_path: String) : Unit = {
    import spark.implicits._
    val ret_path = clean_output_path
    println("wash tf_idf data")
    val vid_tag_data = spark.read.parquet(join_path).as[VidTagInfoList].map(line=> {
      val vid = line.vid
      val tag_info = line.tag_info_list.map(t => TagInfo(t.tag, t.tagid, t.weight))
      (vid, tag_info.sortBy(_.weight)(Ordering[Double].reverse))
    }).toDF("vid", "tag_info")
    val vid_duration_data = spark.read.textFile(video_info_path).map(_.split("\t", -1))
      .filter(_.length > 116)
      .filter(line => line(59) == "4") // 在线上
      .filter(line => line(1).length == 11) //vid长度为11
      .map(line => {
        val vid = line(1)
        val duration: Int = if (line(57).equals("0") || line(57).equals("")) 0 else line(57).toInt
        (vid, duration)
      }).toDF("vid_t", "duration")
    val data = vid_tag_data.join(vid_duration_data, $"vid" === vid_duration_data("vid_t"), "left")
      .select($"vid", $"duration", $"tag_info")
      .map(row => filter_vid_vtags_line(row)).toDF("vid", "duration", "tag_info")

    data.write.mode("overwrite").parquet(ret_path)
    println("--------[wash tf_idf data done, output path: " + ret_path + "]--------")
  }


  // 暂时不加ctr数据
  def join_tag_vid(spark: SparkSession,
                   vtag_path: String,  // vtag_vid
                   qtag_path: String,  // qtag_vid
//                   ctr_path: String,
                   tag_vid_output_path: String,
                   tag_tagid_output_path: String

                  ): Unit = {
    import spark.implicits._
    println("--------------[join_tag_vid]--------------")
    // tag", "tagid", "tag_type", "vid_weight"
    val vtag_data = spark.read.parquet(vtag_path).cache()
    println(s"join_tag_vid: get vtag data from $vtag_path, tag number: ${vtag_data.count()}")

    val vtag_data_flat = vtag_data.flatMap(line => {
      val vtag = line.getAs[String]("tag")
      val tagid = line.getAs[Long]("tagid")
      val tag_type = line.getAs[String]("tag_type")
      val vid_weight =line.getAs[Seq[Row]]("vid_weight")
          .map(r => (r.getString(0), r.getDouble(1)))

      vid_weight.map(tw => (vtag, tagid, tag_type, tw._1, tw._2))
        // vtagid vid weight
    }).toDF("tag", "tagid", "tag_type", "vid", "weight").cache

    //"tag", "tagid", "tag_type", "vid_weight"
    val qtag_data = spark.read.parquet(qtag_path).cache
    println(s"join_tag_vid: get qtag data, tag number: ${qtag_data.count()}")
    qtag_data.printSchema()

    val qtag_data_flat = qtag_data.flatMap(line => {
      val vtag = line.getAs[String]("tag")
      val tagid = line.getAs[Long]("tagid")
      val tag_type = line.getAs[String]("tag_type")
      val vid_weight =line.getAs[Seq[Row]]("vid_weight")
        .map(r => (r.getString(0), r.getDouble(1)))

      vid_weight.map(tw => (vtag, tagid, tag_type, tw._1, tw._2))
    }).toDF("tag", "tagid", "tag_type", "vid", "weight").cache

    val tag_vid_data = vtag_data_flat.union(qtag_data_flat)
      .groupBy($"tag", $"tagid", $"tag_type", $"vid").agg(sum($"weight") as "weight")

    tag_vid_data.sample(withReplacement = true, 0.01).show(50)

    val result = tag_vid_data
      .groupBy($"tag", $"tagid", $"tag_type").agg(collect_list(struct($"vid", $"weight")) as "vid_weight")
      .map(line => {
        val tag = line.getAs[String]("tag")
        val tagid = line.getAs[Long]("tagid")
        val tag_type = line.getAs[String]("tag_type")
        val vid_weight = line.getAs[Seq[Row]]("vid_weight")
          .map(r => (r.getString(0), Tools.normalize(r.getDouble(1))))
          .sortBy(_._2)(Ordering[Double].reverse).take(1000)

        (tag, tagid, tag_type, vid_weight)
      }).toDF("tag", "tagid", "tag_type", "vid_weight").cache
    println(s"join_tag_vid done, put to path $tag_vid_output_path, number: ${result.count}")
    result.write.mode("overwrite").parquet(tag_vid_output_path)

    val tag_tagid = tag_vid_data.select("tag", "tagid", "tag_type").distinct().cache
    println(s"tag-tagid Map write to path: $tag_tagid_output_path, number: ${tag_tagid.count}")
    tag_tagid.write.mode("overwrite").parquet(tag_tagid_output_path)

  }

  case class VidControl(vid: String, is_normal: Boolean, timestamp: Long)
  def get_vid_tag_if_normal(spark: SparkSession, vid_info_path: String) : Dataset[VidControl] = {
    import spark.implicits._
    println(s"read vid_info path: $vid_info_path")
    val play_control_data = spark.sparkContext.textFile(vid_info_path, REPARTITION_NUM)
      .map(line => line.split("\t", -1))
      .filter(_.length >= 130)
      .filter(line => line(59) == "4") // 在线上
      .filter(line => line(1).length == 11) //vid长度为11
      .map(arr => {
      val vid = arr(1)

      /**
        * 1505276 - 低俗
        * 1512774 - 正常
        * 1530844 - 惊悚
        * 1565745 - 恶心
        **/
      val video_pic_scale = arr(99).trim // '视频图片尺度', 99

      /**
        * 1505275 - 低俗
        * 1512760 - 标题党
        * 1512772 - 正常
        * 1565744 - 恶心
        **/
      val video_title_scale = arr(113).trim // 标题尺度

      /**
        * 1487112 - 正常
        * 1487114 - 低俗
        * 1493303 - 惊悚
        * 1494973 - 暴力
        * 1565743 - 恶心
        **/
      val content_level = arr(87).trim // 内容尺度

      /**
        * 汉字的 低端 普通 高端
        *
        */

      val video_content_scale = arr(128).trim

      val is_normal: Boolean = {  // 空则默认正常
          (video_pic_scale == "" || video_pic_scale == "1512774") &&
          (video_title_scale == "" || video_title_scale == "1512772") &&
          (content_level == "" || content_level == "1487112") &&
          !video_content_scale.endsWith("低端")
      }

      val mtime = Tools.tranTimeToLong(arr(129))
      VidControl(vid, is_normal, mtime)
    }).toDS.cache()

    play_control_data
  }


  case class VidTagsWeightWithControl(vid: String, is_normal: Boolean, timestamp: Long, tag_info_list: Seq[TagWeightInfo])
  /**
    * write to redis
    * @param vid_tag_path vid-tags 总的正排数据
    * @param filter_path vid_filter的位置，需要过滤下内容池再写入
    * @param control_flags put or delete
    * @param data_type put key 的版本号
    *
    * */
  def put_vid_tag_to_redis(spark: SparkSession,
                           vid_tag_path : String,
                           filter_path: String,
                           play_control_data: Dataset[VidControl], //vid: String, is_normal: Boolean, mtime: Long
                           control_flags: Set[String],
                           data_type: String = "v0"): Unit = {

    if(control_flags.contains("delete") || control_flags.contains("put")) {
      println(s"put vid_tag to redis, vid_tag path: $vid_tag_path")
      import spark.implicits._
      val ip = "100.107.17.209" // zkname sz1162.short_video_vd2tg.redis.com
      val port = 9100
      val bzid = "sengine"
      val prefix = s"${data_type}_sv_vd2tg"
      val tag_type: Int = 2501
      // filter the vid
      val vid_filter = spark.read.json(filter_path).toDF("vid").cache
      println(s"vid_filter read from path $filter_path, number: ${vid_filter.count()}")
      vid_filter.show

      val data_all = spark.read.parquet(vid_tag_path)
        .join(vid_filter, "vid").as[VidTagInfoList]

        .join(play_control_data, "vid")
        .select($"vid", $"is_normal", $"timestamp", $"tag_info_list")
        .as[VidTagsWeightWithControl]

      val data = data_all.map(line => {
        val vid = line.vid
        val is_normal = line.is_normal
        val ts = line.timestamp
        val tag_weight = line.tag_info_list
          .distinct.sortBy(_.weight)(Ordering[Double].reverse) //
          .take(100)
        VidTagsWeightWithControl(vid, is_normal, ts, tag_weight)
      })
        .cache

      println("data sample: ")
      data.show()

      val test_redis_pool = new TestRedisPool(ip, port, 40000)
      val broadcast_redis_pool = spark.sparkContext.broadcast(test_redis_pool)
      if (control_flags.contains("put")) {
        Tools.delete_and_put_with_play_control(data, broadcast_redis_pool, bzid, prefix, tag_type /*, limit_num = 1000 */)
      }
      println("put vid_tags to redis done, number: " + data.count)
//      Tools.stat(spark, data, s"V1_test_$data_type")
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
    val tags_info_path = Tools.get_latest_subpath(spark, args(5))
    val stop_words_path = args(6)
    val vid_length = args(7).toInt
    val output_path = args(8)
    val date_str = args(9)
    val control_flags = args(10).split(Defines.FLAGS_SPLIT_STR, -1).toSet
    val data_type = args(11)
    val tag_tagid_output_path = args(12)
    println("------------------[begin]-----------------")
    println(s"control flags: ${control_flags.mkString("||")}")
    val vid_tags_path = output_path + "/vid_tags/" + date_str
    if(control_flags.contains("shuffle")) {
      vid_tag_shuffle(spark,
        vid_info_path = vid_input_path,
        cid_info_path = cid_input_path,
        tag_info_path = tags_info_path,
        vid_tags_output_path = vid_tags_path)
    }

    val vid_idf_path = output_path + "/vid_idf_path/" + date_str
    if(control_flags.contains("idf")) {
      vid_tf_idf(spark, vid_tags_path, vid_idf_path)
    }

    val idf_join_path = output_path + "/idf_join_path/" + date_str
    if(control_flags.contains("idf_join")) {
      tf_idf_join_v2(spark, vid_idf_path, idf_join_path)
    }


    val vtag_vid_path = output_path + "/tag_vid/" + date_str
    if(control_flags.contains("tag_vid")) {
      get_vtag_vid_data(spark,
        vid_tag_path = idf_join_path,
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
      join_vid_tag(spark, vtag_path = idf_join_path, qtag_path = vid_qtag_path, output_path = tags_output)
      join_tag_vid(
        spark,
        vtag_path = vtag_vid_path,
        qtag_path = qtag_vid_path,
//        ctr_path = ctr_path,
        tag_vid_output_path = tags_vid_output,
        tag_tagid_output_path = tag_tagid_output_path)
    }

    val clean_output_path = output_path + "/cleaned_data/" + date_str
    if(control_flags.contains("cleaned_data")) {
      tf_idf_to_tuple(spark, join_path = tags_output, clean_output_path = clean_output_path, video_info_path = vid_input_path)
    }


    // 先给出是否有is_normal和改动痕迹的逻辑
    val vid_normal_info = get_vid_tag_if_normal(spark, vid_info_path = vid_input_path)
    put_vid_tag_to_redis(spark,
      vid_tag_path = tags_output,
      filter_path = vid_filter_path,
      play_control_data = vid_normal_info,
      control_flags = control_flags,
      data_type = data_type)

//    put_tag_vid_to_redis(spark, path = tags_vid_output, control_flags = control_flags, data_type = data_type)


    println("------------------[done]-----------------")
  }

}
