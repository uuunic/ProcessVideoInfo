package Algorithm

import org.apache.spark.sql.{Column, SparkSession, functions}
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.functions.{array, map}

import org.apache.spark.rdd.RDD


/**
  * Created by baronfeng on 2017/6/22.
  * for distributed calculation
  */
case class line_video_info(
                            tdbank_imp_date: String, //0
                            vid: String, //1
                            map_id: String, // '发布地图ID',2
                            map_name: String, // '发布地图名称',3
                            race_match_id: String, // '体育场次ID',4
                            race_competition_id: String, // '体育赛事ID',5
                            race_team_id: String, // '比赛队伍ID',6
                            race_team_name: String, // '比赛队伍名称',7
                            race_stars_id: String, // '球星ID',8
                            sport_points_id: String, // '体育看点ID ',9
                            sport_points_name: String, // '体育看点名称 ',10
                            match_points_id: String, // '比赛看点，和matchapp的事件对应 ',11
                            match_cats_id: String, // '比赛类型，体育分类专用 ',12
                            sync_cover: String, //13
                            c_type: String, // '大分类ID',14
                            c_type_name: String, // '大分类名称',15
                            c_subtype_id: String, // '子分类ID，新的用主要类型ID替换', 16
                            c_subtype: String, // '子分类名称，新的用主要类型名称替换', 17
                            c_main_genre_id: String, // '主要类型ID', 18
                            c_main_genre: String, // '主要类型', 19
                            c_main_genres: String, // '媒资类型', 20
                            c_sub_genre_id: String, // '次要类型ID（多个）', 21
                            c_sub_genre: String, // '次要类型', 22
                            c_column_id: String, // '栏目ID', 23
                            c_column: String, // '栏目名称', 24
                            p_langue: String, // '语言', 25
                            p_area_id: String, // '制片地区ID', 26
                            p_area_name: String, // '制片地区', 27
                            p_year: String, // '出品时间', 28
                            d_director_id: String, // '导演ID', 29
                            d_director: String, // '导演名称', 30
                            d_writer_id: String, // '编剧ID', 31
                            d_writer: String, // '编剧', 32
                            d_leading_actor_id: String, // '主角ID', 33
                            d_leading_actor: String, // '主角', 34
                            d_costar_id: String, // '配角ID', 35
                            d_costar: String, // '配角', 36
                            d_presenter_id: String, // '主持人ID', 37
                            d_presenter: String, // '主持人', 37
                            d_guests_id: String, // '嘉宾ID', 39
                            d_guests: String, // '嘉宾', 40
                            d_producer: String, // '出品方', 41
                            b_plot_brief: String, // '剧情看点', 42
                            b_visual_brief: String, // '视听看点', 43
                            b_vtags: String, // '视频看点/标记列表', 44
                            b_tag: String, // '标签，多个加号隔开', 45
                            b_keyword: String, // '关键词', 46
                            b_viewing_experience: String, // '观影感受', 47
                            b_user_reviews: String, // '大众评价', 48
                            b_title: String, // '中文名', 49
                            b_alias: String, // '别名', 50
                            b_second_title: String, // '次标题', 51
                            b_series_name: String, // '系列名', 52
                            b_title_en: String, // '英文名', 53
                            b_description: String, // '简介', 54
                            b_drm: String, // '0:免费视频 1:普通付费视频 2:drm付费视频', 55
                            b_pay1080: String, // '是否1080p付费: 0不付费', 56
                            b_duration: String, // '时长', 57
                            b_episode_all: String, // '集数', 58
                            b_state: String, // '状态', 59
                            b_video_checkup_time: String, // '视频上架时间', 60
                            b_create_time: String, // '创建时间', 61
                            b_c_full: String, // '完整版标识', 62
                            d_mv_stars_id: String, // 'MV主演ID', 63
                            d_mv_stars: String, // 'MV主演', 64
                            d_relative_stars_id: String, // '相关明星ID', 65
                            d_relative_stars: String, // '相关明星', 66
                            singer_name: String, // '歌手名', 67
                            singer_id: String, // '歌手id', 68
                            relative_covers: String, // '相关剧ID', 69
                            stars: String, // '视频明星', 70
                            stars_name: String, // '视频明星名字', 71
                            c_covers: String, // '所属的专辑列表', 72
                            first_recommand_id: String, // '一级推荐分类id', 73
                            sec_recommand_id: String, // '二级推荐分类id', 74
                            first_recommand: String, // '一级推荐分类', 75
                            sec_recommand: String, // '二级推荐分类', 76
                            score_quality_id: String, // '视频质量id', 77
                            score_quality: String, // '视频质量', 78
                            score_interest_id: String, // '趣味性id', 79
                            score_interest: String, // '趣味性', 80
                            score_story_id: String, // '故事性id', 81
                            score_story: String, // '故事性', 82
                            score_classic_id: String, // '经典程度id', 83
                            score_classic: String, // '经典程度', 84
                            is_good_content_id: String, // '是否优质内容id', 85
                            is_good_content: String, // '是否优质内容id', 86
                            content_level: String, // '内容尺度', 87
                            pioneer_tag: String, // '先锋视频标签', 88
                            pioneer_tag_id: String, // '先锋标签ID', 89
                            cartoon_main_type_id: String, // '动漫类型id', 90
                            cartoon_main_type: String, // '动漫类型', 91
                            cartoon_aspect_id: String, // '动漫看点id', 92
                            cartoon_aspect: String, // '动漫看点', 93
                            sportsman_id: String, // '运动员id', 94
                            sportsman: String, // '运动员', 95
                            c_data_flag: String, // '数据标志', 96
                            related_time: String, // '相关时间', 97
                            video_title_scale: String, // '视频标题尺度', 98
                            video_pic_scale: String, // '视频图片尺度', 99
                            publish_datex: String, // '播出日期', 100
                            cover_pic_score: String, // '视频封面打分', 101
                            c_category: String, // 102
                            video_pic_distortion_score: String, // '视频图片变形评分', 103
                            ugc_src_flag: String, // 'ugc推荐标识', 104
                            c_copyright: String, // '版权方id', 105
                            copyright_name: String, // '版权方展示名', 106
                            c_qq: String, // 107
                            video_aspect_ratio: String, // '视频宽高比, 108
                            filter_id: String, // '地域播控筛选id', 109
                            fid_1184: String, // '地域播控是否打开', 110
                            cover_black_percent: String, // '封面图黑边占比', 111
                            quality_machine_score: String, // '视频质量机器评分', 112
                            cinema_flag: String, // '院线', 113
                            game_name: String, // '游戏名', 114
                            is_normalized: String, // '是否已标准化', 115
                            cover_img_quality: String // '封面图质量'
                          )
case class line_video_info_simple(b_tag: String, b_duration: Int)

object dataProcess {

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

/*
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
  */
}
