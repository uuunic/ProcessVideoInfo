package Test

import Utils.TestRedisPool
import com.hankcs.hanlp.HanLP
import com.hankcs.hanlp.seg.CRF.CRFSegment
import com.hankcs.hanlp.seg.NShort.NShortSegment
import com.huaban.analysis.jieba.JiebaSegmenter
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.{Dataset, Row, SparkSession}


object Test {



  def put_to_redis(input:Dataset[KeyValueWeight],
                   broadcast_redis_pool: Broadcast[TestRedisPool],
                   bzid:String,
                   prefix: String,
                   tag_type: Int,
                   weight_format: String = "%.4f",
                   expire_time: Int = 400000,
                   limit_num: Int = -1) : Unit = {
    val output = if(limit_num == -1) input else input.limit(limit_num)

    //要写入redis的数据，RDD[Map[String,String]]
    output.repartition(20).foreachPartition { iter =>
      //val redis = new Jedis(ip, port, expire_time)
      val redis = broadcast_redis_pool.value.getRedisPool.getResource   // lazy 加载 应该可以用
    val ppl = redis.pipelined() //使用pipeline 更高效的批处理
    var count = 0
      iter.foreach(f => {
        val key = bzid + "_" + prefix + "_" + f.key
        val values_data = f.value_weight.sortWith(_._2>_._2).map(line=>{
          line._1 + ":" + tag_type.toString + ":" + line._2.formatted(weight_format)

        })
        val keys = Array(key)
        ppl.del(keys: _*)
        ppl.rpush(key, values_data: _*)
        ppl.expire(key, 60*60*24*2)   // 这里设置expire_time


        count += 1
        if(count % 30 == 0) {
          ppl.sync()
        }
      })
      ppl.sync()
      redis.close()

    }
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
    })
    //data.collect().foreach(line=>println(line.key))
    //println("\n\n\n\n")
    val test_redis_pool = new TestRedisPool(ip, port, 40000)
    val broadcast_redis_pool = spark.sparkContext.broadcast(test_redis_pool)
    put_to_redis(data, broadcast_redis_pool, bzid, prefix, tag_type /*, limit_num = 1000 */)
    println("put to redis done, number: " + data.count)

  }

  def printToFile(f: java.io.File)(op: java.io.PrintWriter => Unit) {
    val p = new java.io.PrintWriter(f)
    try { op(p) } finally { p.close() }
  }

  import java.io._



  case class KeyValueWeight(key: String, value_weight: Seq[(String, Double)])

  def main(args: Array[String]) {
    System.setProperty("hadoop.home.dir", "C:\\winutils")
    val spark = SparkSession
      .builder
      .appName("cid_vid_index_wash")
        .master("local")
      .getOrCreate()


    val test_data = Array("恶毒！吴聘果然是胡咏梅毒死的，她还杀了周莹另一位至亲之人",
      "香蕉+冬枣，明知作死也要试一试",
      "丰田车主：这辈子都没这样尴尬过！",
      "昆虫掉入饮料杯中染色了 教宝宝学习颜色认识昆虫",
      "小伙定制的福特野马跑车刚到货，撕开遮车布后当场懵圈了",
      "过家家玩具厨房！芭比娃娃玩水果切切乐，灶火做饭视频",
      "乔治照顾猪宝宝 小猪佩奇给猪宝宝喂米糊",
      "女子去算命，当场气死算命先生，我真是服你了！",
      "宋茜臭骂夏雨“流氓”，导致分手的原因是这样？",
      "让我们用橡皮泥做出佩奇小猪一家 佩奇小车车",
      "diy洛丽塔紧身袜",
      "夫妻两人逛街遭遇老同学侮辱，结果夫妻二人疯狂反击！",
      "为什么酒店的床尾都要放一块布？答案你万万想不到",
      "教你如何制作彩色的太空沙积木",
      "重新解读神剧《粉红女郎》，原来有这么多梗",
      "剧版《大话西游》黄子韬携手赵艺再现至尊宝紫霞的虐恋情深",
      "李晨范冰冰燃爆空战，《空天猎》vs《战狼2》谁更燃",
      "学习数字：轨道小火车乐园拉彩蛋",
      "猪猪侠之拼装特工队 猪猪侠和菲菲有难 泼比开飞机去救他们！",
      "在气球中装满水银，用针扎破后会发生什么？",
      "那年花开月正圆：临死的杜明礼终做件好事，一刀捅死了幕后贝勒爷",
      "少儿英语边玩边学 淘气的儿子不睡觉累坏了爸爸 学4个单词",
      "早教色彩认知英语：豆豆人吃大团大团的足球 撑得变了色",
      "女警花想要诱捕毒贩却被下药，警方破门而入还是晚了一步",
      "蜘蛛侠到小丑店里买彩色冰淇淋吃后变色了，学习颜色",
      "这货在自行车上装水炮 飞得比法拉利还快",
      "还记得当年的最美哪吒吗，现在已经是大美女了",
      "男嘉宾相亲现场吃女神豆腐，孟非看不下去了",
      "日本这座桥看着危险，吓退许多老司机，实际暗藏玄机",
      "灰太狼穿过异云变成巨人，这下看羊往哪里跑",
      "奇闻趣事：这只鹦鹉快三十岁了，精通各种对话各种口技，成精了")

//    val test_data = spark.read.textFile("F:\\query_10000").collect()

    println("------------------[begin]-----------------")
    val segment_nshort = new NShortSegment().enableCustomDictionary(false).enablePlaceRecognize(true).enableOrganizationRecognize(true)
    val segment = HanLP.newSegment()
    val segment_crf = new CRFSegment
    segment_crf.enablePartOfSpeechTagging(true)

    val segmenter = new JiebaSegmenter()


    import scala.collection.JavaConversions._
    val useful_set = Map(
      "a" -> "形容词",
      "ad" -> "副形词",
      "ag" -> "形容词性语素",
      "al" -> "形容词性惯用语",
      "an" -> "名形词",

      "b" -> "区别词",
      "begin" -> "仅用于始##始",
      "bg" -> "区别语素",
      "bl" -> "区别词性惯用语",

//      "c" -> "连词",
//      "cc" -> "并列连词",
//
//      "d" -> "副词",
//      "dg" -> "辄,俱,复之类的副词",
//      "dl" -> "连语",

      "e" -> "叹词",
      "end" -> "仅用于终##终",

      "f" -> "方位词",

      "g" -> "学术词汇",
      "gb" -> "生物相关词汇",
      "gbc" -> "生物类别",
      "gc" -> "化学相关词汇",
      "gg" -> "地理地质相关词汇",
      "gi" -> "计算机相关词汇",
      "gm" -> "数学相关词汇",
      "gp" -> "物理相关词汇",

      "h" -> "前缀",
      "i" -> "成语",
      "j" -> "简称略语",
      "k" -> "后缀",
      "l" -> "习用语",

//      "m" -> "数词",
//      "mg" -> "数语素",
//      "Mg" -> "甲乙丙丁之类的数词",
//      "mq" -> "数量词",

      "n" -> "名词",
      "nb" -> "生物名",
      "nba" -> "动物名",
      "nbc" -> "动物纲目",
      "nbp" -> "植物名",
      "nf" -> "食品，比如“薯片”",
      "ng" -> "名词性语素",
      "nh" -> "医药疾病等健康相关名词",
      "nhd" -> "疾病",
      "nhm" -> "药品",
      "ni" -> "机构相关（不是独立机构名）",
      "nic" -> "下属机构",
      "nis" -> "机构后缀",
      "nit" -> "教育相关机构",
      "nl" -> "名词性惯用语",
      "nm" -> "物品名",
      "nmc" -> "化学品名",
      "nn" -> "工作相关名词",
      "nnd" -> "职业",
      "nnt" -> "职务职称",
      "nr" -> "人名",
      "nr1" -> "复姓",
      "nr2" -> "蒙古姓名",
      "nrf" -> "音译人名",
      "nrj" -> "日语人名",
      "ns" -> "地名",
      "nsf" -> "音译地名",
      "nt" -> "机构团体名",
      "ntc" -> "公司名",
      "ntcb" -> "银行",
      "ntcf" -> "工厂",
      "ntch" -> "酒店宾馆",
      "nth" -> "医院",
      "nto" -> "政府机构",
      "nts" -> "中小学",
      "ntu" -> "大学",
      "nx" -> "字母专名",
      "nz" -> "其他专名",

//      "r" -> "代词",
//      "rg" -> "代词性语素",
//      "Rg" -> "古汉语代词性语素",
//      "rr" -> "人称代词",
//      "ry" -> "疑问代词",
//      "rys" -> "处所疑问代词",
//      "ryt" -> "时间疑问代词",
//      "ryv" -> "谓词性疑问代词",
//      "rz" -> "指示代词",
//      "rzs" -> "处所指示代词",
//      "rzt" -> "时间指示代词",
//      "rzv" -> "谓词性指示代词",
//      "s" -> "处所词",
//      "t" -> "时间词",
//      "tg" -> "时间词性语素",

//      "v" -> "动词",
//      "vd" -> "副动词",
//      "vf" -> "趋向动词",
//      "vg" -> "动词性语素",
      "vi" -> "不及物动词（内动词）",
      "vl" -> "动词性惯用语",
      "vn" -> "名动词"

    )
    val file = new File("F:\\example.txt")


    printToFile(file) { p =>
      test_data.foreach { txt => {
        p.println(txt)
        val data = segment.seg(txt)
        p.println("normal: " + data.filter(_.word.length >= 2).filter(line => useful_set.keySet.contains(line.nature.toString)).map(_.word).mkString(" "))

        p.println("CRF:    " + segment_crf.seg(txt).filter(_.word.length >= 2).filter(line => useful_set.keySet.contains(line.nature.toString)).map(_.word).mkString(" "))

        //      println("nshort: " + segment_nshort.seg(txt).filter(_.word.length >=2).filter(line=>useful_set.keySet.contains(line.nature.toString)))

        //      println("jieba:  " + segmenter.process(txt, SegMode.SEARCH).map(line=>line.word).filter(_.length >=2).toArray.mkString(", "))


        p.println()

      }
      }
    }


//
//    printToFile(file){p=>test_data.foreach {txt=> {
//      p.println(txt)
//      val data = segment.seg(txt)
//      p.println("normal_escape: " + data.filter(_.word.length >=2).filter(line=>{!useful_set.keySet.contains(line.nature.toString)}))
//
//      p.println("CRF_escape:    " + segment_crf.seg(txt).filter(_.word.length >=2).filter(line=>{!useful_set.keySet.contains(line.nature.toString)}))
//
////      println("nshort: " + segment_nshort.seg(txt).filter(_.word.length >=2).filter(line=>{!useful_set.keySet.contains(line.nature.toString)}))
//
////      println("jieba:  " + segmenter.process(txt, SegMode.SEARCH).map(line=>line.word).filter(_.length >=2).toArray.mkString(", "))
//      p.println
//    }
//    }
//    }

    println("------------------[done]-----------------")
  }

}