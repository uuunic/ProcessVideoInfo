package Test

import Utils.HadoopFileIoAdapter
import com.hankcs.hanlp.HanLP
import com.hankcs.hanlp.HanLP.Config
import com.hankcs.hanlp.dictionary.CustomDictionary
import com.hankcs.hanlp.seg.CRF.CRFSegment
import com.hankcs.hanlp.seg.NShort.NShortSegment
import org.apache.spark.sql.SparkSession

import scala.collection.JavaConversions._
/**
  * Created by baronfeng on 2017/10/20.
  */
object LpTest {

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

  val pattern_list = Array(
    """.*(一|二|三|四|五|六|七|八|九)$""".r,
    """(第(一|二|三|四|五|六|七|八|九|十|\d)(集|季|部|期))|(season \d+)|(\d+)$""".r,
    """(预告|电影|大电影|电影大全|系列|视频|电视剧|电视连续剧|电视剧大全|节目|电视节目)$""".r,
    """(合集|全集|电影合集|电影全集|电视剧全集)$""".r,
    """(版|中文|中文版|英文|英语|英文版|国语|国语版|2D版|3D版|3d版|2d|2D|3d|3D)$""".r,
    """(枪版|高清|高清版|DVD版|蓝光|蓝光版)""".r,
    """(\s)""".r,
    """[——！，。·～？、~@#￥%……&*（）：；《）《》“”\(\)»〔〕\-「」]\+""".r,
    """[<> _,()~\';\":-`!@#$%^&*+=\[\]{}>?/.\|\\\n\r\t]""".r
  )

  def printToFile(f: java.io.File)(op: java.io.PrintWriter => Unit) {
    val p = new java.io.PrintWriter(f)
    try { op(p) } finally { p.close() }
  }


//  val file = new File("F:\\example.txt")
//
//
//  printToFile(file) { p =>
//    test_data.foreach { txt => {
//      p.println(txt)
//      val data = segment.seg(txt)
//      p.println("normal: " + data.filter(_.word.length >= 2).filter(line => useful_set.keySet.contains(line.nature.toString)).map(_.word).mkString(" "))
//      p.println("CRF:    " + segment_crf.seg(txt).filter(_.word.length >= 2).filter(line => useful_set.keySet.contains(line.nature.toString)).map(_.word).mkString(" "))
//
//      p.println()
//
//    }
//    }
//  }

  def get_divide_words(spark: SparkSession, cid_path: String): Set[String] = {
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
          for(r <- pattern_list) {
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

  def get_poineer_tags(spark: SparkSession, tags_path: String) : Set[String] = {

    println(s"--------------begin to get poineer tags--------------")
    val source_data = spark.sparkContext.textFile(tags_path)
      .map(_.split("\t", -1)).filter(_.length==14).filter(_(6)=="1")
      .map(_(2)).distinct().collect().toSet
    println(s"--------------get poineer tags number: ${source_data.size}--------------")
    source_data
  }

  def add_custom_dic(divide_words: Set[String]) : Unit ={
    println(s"--------------begin to add custom dict, number: ${divide_words.size}--------------")
    Config.IOAdapter = new HadoopFileIoAdapter()
    for(s <- divide_words) {
      CustomDictionary.add(s)
    }
    println("--------------add custom dict done--------------")
  }

  def get_split_titles(spark: SparkSession, data_path: String, output_path: String, custom_keys: Set[String]) : Unit = {


    import spark.implicits._

    //        val path = HanLP.Config.CustomDictionaryPath.toBuffer
    //        path += "hdfs:///user/baronfeng/useful_dataset/hanlp/data/dictionary/custom/CustomDictionary.txt"
    //        HanLP.Config.CustomDictionaryPath = path.toArray


    val keys_broadcast = spark.sparkContext.broadcast(custom_keys)

    val data = spark.sparkContext.textFile(data_path).repartition(200)
      .mapPartitions(part => {
        Config.IOAdapter = new HadoopFileIoAdapter()
        for(s <- keys_broadcast.value) {
          CustomDictionary.add(s)
        }

        val segment_nshort_broadcast = new NShortSegment().enableCustomDictionary(true).enablePlaceRecognize(true).enableOrganizationRecognize(true)
        val segment_broadcast = HanLP.newSegment().enableCustomDictionary(true)
        val segment_crf_broadcast = new CRFSegment().enablePartOfSpeechTagging(true).enableCustomDictionary(true)
        part.map(title => {


          val nshort = segment_nshort_broadcast.seg(title)
            .filter(_.word.length >= 2)
            .filter(line => useful_set.keySet.contains(line.nature.toString))
            .map(_.word).mkString(" ")
          val normal = segment_broadcast.seg(title)
            .filter(_.word.length >= 2)
            .filter(line => useful_set.keySet.contains(line.nature.toString))
            .map(_.word).mkString(" ")
          val crf = segment_crf_broadcast.seg(title)
            .filter(_.word.length >= 2)
            .filter(line => useful_set.keySet.contains(line.nature.toString))
            .map(_.word).mkString(" ")
          (title, nshort, normal, crf)
        })
      }).toDF("title", "nshort", "normal", "crf").cache()
    println("show split titles")
    data.show()
    println(s"begin to write to path: $output_path")
    data.repartition(200).write.mode("overwrite").parquet(output_path)
  }

  def main(args: Array[String]) {
    println("------------------[begin]-----------------")
//    System.setProperty("hadoop.home.dir", "C:\\winutils")
    val spark = SparkSession
      .builder
      .appName(this.getClass.getName.split("\\$").last)
//      .master("local")
      .getOrCreate()

    val cid_data_path = args(0)
    val tags_data_path = args(1)
    val data_path = args(2)

    val output_path = args(3)
    val stop_words = get_divide_words(spark, cid_data_path)
//    add_custom_dic(stop_words)

    val tags = get_poineer_tags(spark, tags_data_path)
//    add_custom_dic(tags)

    val keys = stop_words ++ tags

    get_split_titles(spark, data_path = data_path, output_path = output_path, custom_keys = keys)

    println("------------------[done]-----------------")
  }


}
