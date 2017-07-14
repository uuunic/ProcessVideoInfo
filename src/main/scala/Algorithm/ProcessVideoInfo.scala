package Algorithm

import org.apache.spark.sql.{Column, SparkSession, functions}
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.functions.{array, map}

import org.apache.spark.rdd.RDD


/**
  * Created by baronfeng on 2017/6/22.
  * for distributed calculation
  */
object dataProcess {

  case class line_video_info(
                              tdbank_imp_date: String,
                              vid: String,
                              map_id: String, // '发布地图ID'
                              map_name: String, // '发布地图名称'
                              race_match_id: String, // '体育场次ID'
                              race_competition_id: String, // '体育赛事ID'
                              race_team_id: String, // '比赛队伍ID'
                              race_team_name: String, // '比赛队伍名称'
                              race_stars_id: String, // '球星ID'
                              sport_points_id: String, // '体育看点ID '
                              sport_points_name: String, // '体育看点名称 '
                              match_points_id: String, // '比赛看点，和matchapp的事件对应 '
                              match_cats_id: String, // '比赛类型，体育分类专用 '
                              sync_cover: String,
                              c_type: String, // '大分类ID'
                              c_type_name: String, // '大分类名称'
                              c_subtype_id: String, // '子分类ID，新的用主要类型ID替换'
                              c_subtype: String, // '子分类名称，新的用主要类型名称替换'
                              c_main_genre_id: String, // '主要类型ID'
                              c_main_genre: String, // '主要类型'
                              c_main_genres: String, // '媒资类型'
                              c_sub_genre_id: String, // '次要类型ID（多个）'
                              c_sub_genre: String, // '次要类型'
                              c_column_id: String, // '栏目ID'
                              c_column: String, // '栏目名称'
                              p_langue: String, // '语言'
                              p_area_id: String, // '制片地区ID'
                              p_area_name: String, // '制片地区'
                              p_year: String, // '出品时间'
                              d_director_id: String, // '导演ID'
                              d_director: String, // '导演名称'
                              d_writer_id: String, // '编剧ID'
                              d_writer: String, // '编剧'
                              d_leading_actor_id: String, // '主角ID'
                              d_leading_actor: String, // '主角'
                              d_costar_id: String, // '配角ID'
                              d_costar: String, // '配角'
                              d_presenter_id: String, // '主持人ID'
                              d_presenter: String, // '主持人'
                              d_guests_id: String, // '嘉宾ID'
                              d_guests: String, // '嘉宾'
                              d_producer: String, // '出品方'
                              b_plot_brief: String, // '剧情看点'
                              b_visual_brief: String, // '视听看点'
                              b_vtags: String, // '视频看点/标记列表'
                              b_tag: String, // '标签，多个加号隔开'
                              b_keyword: String, // '关键词'
                              b_viewing_experience: String, // '观影感受'
                              b_user_reviews: String, // '大众评价'
                              b_title: String, // '中文名'
                              b_alias: String, // '别名'
                              b_second_title: String, // '次标题'
                              b_series_name: String, // '系列名'
                              b_title_en: String, // '英文名'
                              b_description: String, // '简介'
                              b_drm: String, // '0:免费视频 1:普通付费视频 2:drm付费视频'
                              b_pay1080: String, // '是否1080p付费: 0不付费'
                              b_duration: String, // '时长'
                              b_episode_all: String, // '集数'
                              b_state: String, // '状态'
                              b_video_checkup_time: String, // '视频上架时间'
                              b_create_time: String, // '创建时间'
                              b_c_full: String, // '完整版标识'
                              d_mv_stars_id: String, // 'MV主演ID'
                              d_mv_stars: String, // 'MV主演'
                              d_relative_stars_id: String, // '相关明星ID'
                              d_relative_stars: String, // '相关明星'
                              singer_name: String, // '歌手名'
                              singer_id: String, // '歌手id'
                              relative_covers: String, // '相关剧ID'
                              stars: String, // '视频明星'
                              stars_name: String, // '视频明星名字'
                              c_covers: String, // '所属的专辑列表'
                              first_recommand_id: String, // '一级推荐分类id'
                              sec_recommand_id: String, // '二级推荐分类id'
                              first_recommand: String, // '一级推荐分类'
                              sec_recommand: String, // '二级推荐分类'
                              score_quality_id: String, // '视频质量id'
                              score_quality: String, // '视频质量'
                              score_interest_id: String, // '趣味性id'
                              score_interest: String, // '趣味性'
                              score_story_id: String, // '故事性id'
                              score_story: String, // '故事性'
                              score_classic_id: String, // '经典程度id'
                              score_classic: String, // '经典程度'
                              is_good_content_id: String, // '是否优质内容id'
                              is_good_content: String, // '是否优质内容id'
                              content_level: String, // '内容尺度'
                              pioneer_tag: String, // '先锋视频标签'
                              pioneer_tag_id: String, // '先锋标签ID'
                              cartoon_main_type_id: String, // '动漫类型id'
                              cartoon_main_type: String, // '动漫类型'
                              cartoon_aspect_id: String, // '动漫看点id'
                              cartoon_aspect: String, // '动漫看点'
                              sportsman_id: String, // '运动员id'
                              sportsman: String, // '运动员'
                              c_data_flag: String, // '数据标志'
                              related_time: String, // '相关时间'
                              video_title_scale: String, // '视频标题尺度'
                              video_pic_scale: String, // '视频图片尺度'
                              publish_datex: String, // '播出日期'
                              cover_pic_score: String, // '视频封面打分'
                              c_category: String,
                              video_pic_distortion_score: String, // '视频图片变形评分'
                              ugc_src_flag: String, // 'ugc推荐标识'
                              c_copyright: String, // '版权方id'
                              copyright_name: String, // '版权方展示名'
                              c_qq: String,
                              video_aspect_ratio: String, // '视频宽高比
                              filter_id: String, // '地域播控筛选id'
                              fid_1184: String, // '地域播控是否打开'
                              cover_black_percent: String, // '封面图黑边占比'
                              quality_machine_score: String, // '视频质量机器评分'
                              cinema_flag: String, // '院线'
                              game_name: String, // '游戏名'
                              is_normalized: String, // '是否已标准化'
                              cover_img_quality: String // '封面图质量'
                            )
  case class line_video_info_simple(b_tag: String, b_duration: Int)
  def process_video_info(spark: SparkSession, inputPath:String, outputPath:String): Int = {
    val temp_viewname = "parquet_video"
//    val sqlExpression =
//      //"select tdbank_imp_date, vid, map_id, map_name, c_type, c_type_name, b_tag, b_title " +
//      "select * " +
//      " from " + temp_viewname + " where b_duration < 600"
    val sqlExpression = "select b_tag, b_duration from " + temp_viewname + " where b_duration < 600"

    val rdd : RDD[line_video_info_simple] = spark.sparkContext.textFile(inputPath)
      .map(line => line.split("\t", -1)).filter(_.length > 58)
      // 45:b_tag, 57:b_duration
      .map(arr => line_video_info_simple(arr(45), arr(57).toInt))

    import spark.implicits._
    val parquetFile = rdd.toDF
    parquetFile.createOrReplaceTempView(temp_viewname)  //注册一下这个table

    parquetFile.printSchema()  // 展示表结构

    if ("".equals(sqlExpression) || " ".equals(sqlExpression) || "\t".equals(sqlExpression)){
      println("sorry, sqlExpression is null!!!!")
      spark.stop()
      return -1
    }

    val res = spark.sql(sqlExpression)

    val res_result = res.select($"b_tag").explode("b_tag", "tag"){
      line:String => line.split("#")
    }
      .filter(!$"tag".equalTo(""))

    // process the tag
    val res_df = res_result.select("tag")
    res_df.createOrReplaceTempView("temp_db")
    val res_df2 = res_df.sqlContext.sql("select tag, count(tag) as count from temp_db group by tag order by count desc")


    res_df2.show(100)
    res_df2.write.mode("overwrite").json(outputPath)
    println("------------------------------------is Done !----------------------------------------")
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
      .appName("spark-vid-count")
      //     .master("local")
      .getOrCreate()


    val inputPath = args(0)
    val outputPath = args(1)

    process_video_info(spark, inputPath, outputPath)

  }
}
