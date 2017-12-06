package Algorithm

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession


/**
  * Created by baronfeng on 2017/6/22.
  * for distributed calculation
  */
case class line_video_info(
                            tdbank_imp_date: String, //0
                            vid: String, //1
                            map_id: String, // '发布地图ID',2
                            map_name: String, // '发布地图名称',3 正片
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
                            b_state: String, // '状态', 59 4为上架，8为下架
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
                            cover_img_quality: String, // '封面图质量' 116
                            music_style: String, // '音乐风格', 117
                            subscript: String, // '角标', 118
                            cover_img_content: String, // '封面图内容',119
                            cover_img_src: String, // '封面图来源',120
                            audit_status: String, // '敏感词审核状态',121
                            audit_cause: String, // '敏感词审核原因',122
                            original_tag: String, // '原创标记',123
                            try_duration: String, // '试看时长',124
                            copyright_id: String, // '版权ID,跟版权方ID不一样',125
                            playright: String, // '可播放平台列表',126
                            episode_all_name: String, // '集数（文本）',127
                            video_content_scale: String, // '视频内容级别',128
                            c_mtime: String // '修改时间' 129
                          )
case class line_video_info_simple(b_tag: String, b_duration: Int)

// 专辑列表的总清单
case class line_cover_info(
                            tdbank_imp_date: BigInt, //0,
                            cid: String, // 1 COMMENT '视频ID' ,
                            map_id: String, // 2 COMMENT '发布地图ID',
                            map_name: String, // 3 COMMENT '发布地图名称',
                            online_time: String, // 4 COMMENT '电影、电视剧、动漫上线时间',
                            end_date: String, // 5 COMMENT '电影、电视剧、动漫完结时间',
                            imgtag: String, // 6 COMMENT '角标',
                            is_sched: String, // 7 COMMENT '是否进排播表',
                            publish_period: String, // 8 COMMENT '播出周期',
                            tv_station: String, // 9 COMMENT '播出电视台',
                            episode_all: String, // 10 COMMENT '总集数',
                            main_genre: String, // 11 COMMENT '主要类型',
                            sub_genre: String, // 12 COMMENT '次要类型',
                            langue: String, // 13 COMMENT '影视语言',
                            publish_date: String, // 14 COMMENT '国内上映时间',
                            america_publish_date: String, // 15 COMMENT '北美上映时间',
                            costar_id: String, // 16 COMMENT '配角ID',
                            costar: String, // 17 COMMENT '配角',
                            box_office: String, // 18 COMMENT '票房收入',
                            c_type: String, // 19 COMMENT '大分类ID',
                            c_type_name: String, // 20 COMMENT '大分类名称',
                            c_subtype_id: String, // 21 COMMENT '子分类ID，新的用主要类型ID替换',
                            c_subtype: String, // 22 COMMENT '子分类名称，新的用主要类型名称替换',
                            c_main_genre_id: String, // 23 COMMENT '主要类型ID',
                            c_sub_genre_id: String, // 24 COMMENT '次要类型ID（多个）',
                            p_area_id: String, // 25 COMMENT '制片地区ID',
                            p_area_name: String, // 26 COMMENT '制片地区',
                            p_year: String, // 27 COMMENT '出品时间',
                            p_cartoon_age: String, // 28 COMMENT '动漫年龄段',
                            d_director_id: String, // 29 COMMENT '导演ID',
                            d_director: String, // 30 COMMENT '导演名称',
                            d_write_id: String, // 31 COMMENT '编剧ID',
                            d_writer: String, // 32 COMMENT '编剧',
                            d_leading_actor_id: String, // 33 COMMENT '主角ID',
                            d_leading_actor: String, // 34 COMMENT '主角',
                            d_presenter_id: String, // 35 COMMENT '主持人ID',
                            d_presenter: String, // 36 COMMENT '主持人',
                            d_guests_id: String, // 37 COMMENT '嘉宾ID',
                            d_guests: String, // 38 COMMENT '嘉宾',
                            d_producer: String, // 39 COMMENT '出品方',
                            b_plot_brief: String, // 40 COMMENT '剧情看点',
                            b_visual_brief: String, // 41 COMMENT '视听看点',
                            b_cartoon_aspect: String, // 42 COMMENT '出品方',
                            b_aspect_tag: String, // 43 COMMENT '剧情看点',
                            b_main_aspect: String, // 44 COMMENT '视听看点',
                            b_brief: String, // 45 COMMENT '看点、简短摘要',
                            b_sport_points_id: String, // 46 COMMENT '体育看点ID',
                            b_sport_points_name: String, // 47 COMMENT '体育看点名称',
                            b_variety_tags: String, // 48 COMMENT '综艺标签',
                            b_tag: String, // 49 COMMENT '标签，多个加号隔开',
                            b_keyword: String, // 50 COMMENT '关键词',
                            b_viewing_experience: String, // 51 COMMENT '观影感受',
                            b_user_reviews: String, // 52 COMMENT '大众评价',
                            b_title: String, // 53 COMMENT '中文名',
                            b_alias: String, // 54 COMMENT '别名',
                            b_second_title: String, // 55 COMMENT '次标题',
                            b_series_name: String, // 56 COMMENT '系列名',
                            b_title_en: String, // 57 COMMENT '英文名',
                            b_description: String, // 58 COMMENT '简介',
                            b_drm: String, // 59 COMMENT '是否drm专辑',
                            b_time_long: String, // 60 COMMENT '时长',
                            b_cover_checkup_grade: String, // 61 COMMENT '专辑状态',  0：未审核；4：已上架；8：已下架 100：已删除
                            b_real_pubtime: String, // 62 COMMENT '正片实际上线时间',
                            b_create_time: String, // 63 COMMENT '创建时间',
                            b_copyright_id: String, // 64  COMMENT '版权方ID',
                            b_copyright: String, // 65 COMMENT '版权方',
                            b_hot_level: String, // 66  COMMENT '级别',
                            b_douban_id: String, // 67  COMMENT '豆瓣ID',
                            b_pay_status: String, // 68  COMMENT '是否付费',
                            douban_score: String, // 69 COMMENT '豆瓣评分',
                            cover_checkup_time: String, // 70 COMMENT '视频上架时间',
                            b_imdb_id: String, // 71  COMMENT 'IMDB ID',
                            b_imdb_score: String, // 72  COMMENT 'imdb评分',
                            column_id: String, // 73  COMMENT '‘栏目id’',
                            b_mtime_id: String, // 74  COMMENT '时光网ID',
                            b_mtime_score: String, // 75 COMMENT '时光网评分',
                            drama_id: String, // 76 COMMENT '剧id',
                            d_singer_id: String, // 77 COMMENT '歌手ID',
                            d_singer_name: String, // 78 COMMENT '歌手名称',
                            d_look_him_id: String, // 79 COMMENT '只看TA ID',
                            d_look_him: String, // 80 COMMENT 'look_him',
                            d_leading_actorx: String, // 81 COMMENT '主角ID，不用d_leading_actor',
                            d_race_stars_id: String, // 82 COMMENT '球星ID',
                            d_race_stars: String, // 83 COMMENT '球星名称',
                            video_ids: String, // 84 COMMENT '长视频列表',
                            stars: String, // 85 COMMENT '娱乐分类下的明星',
                            stars_id: String, // 86 COMMENT '娱乐分类下的明星id',
                            original_author: String, // 87 COMMENT '原作者',
                            cartoon_aspect_id: String, // 88 COMMENT '动漫看点id',
                            producer_id: String, // 89 COMMENT '出品方ID列表',
                            payfree_num: String, // 90 COMMENT '前/后免费集数',
                            data_checkup_grade: BigInt, // 91 COMMENT '资料状态',
                            doulie_lable: String, // 92 COMMENT '豆列标签',
                            mv_type: String, // 93 COMMENT 'mv类型',
                            plot_brief_id: String, // 94 COMMENT '剧情看点ID',
                            playright: String, // 95 COMMENT '可播放平台列表',
                            cartoon_main_type_id: String, // 96 COMMENT '动漫类型id',
                            cartoon_main_type: String, // 97 COMMENT '动漫类型',
                            sportsman_id: String, // 98 COMMENT '运动员id',
                            sportsman: String, // 99 COMMENT '运动员',
                            c_new_pic_hz: String, // 100 COMMENT '栏目横图',
                            c_new_pic_vt: String, // 101 COMMENT '栏目竖图',
                            unit_id: String, // 102 COMMENT '新栏目id',
                            first_checkup_time: String, // 103 COMMENT '专辑下第一个视频加入专辑的时间',
                            series_id: String, // 104 COMMENT '系列id',
                            publish_time: String, // 105 COMMENT '播出时间',
                            show_year: String, // 106 COMMENT '外线年份',
                            year: String, // 107 COMMENT '年代',
                            epsode_pubtime: String, // 108 COMMENT '剧集(期数)更新时间',
                            episode_updated: String, // 109 COMMENT '更新集数',
                            epsode_state: String, // 110 COMMENT '播出状态',
                            c_data_flag: BigInt, // 111 COMMENT '数据标志',
                            content_level: String, // 112 COMMENT '内容尺度',
                            video_title_scale: String, // 113 COMMENT '标题尺度',
                            video_pic_scale: String, // 114 COMMENT '图片尺度',
                            test_user_reviews: String, // 115 COMMENT '测试-大众评价',
                            test_visual_brief: String, // 116 COMMENT '测试-视听看点',
                            test_viewing_experience: String, // 117 COMMENT '测试-观影感受',
                            test_plot_brief: String, // 118 COMMENT '测试-剧情看点',
                            c_vclips: String, // 119 COMMENT '碎视频列表',
                            c_copyright_uptime: String, // 120 COMMENT '下线时间',
                            test_main_type: String, // 121 COMMENT '测试-综艺节目主要类型',
                            test_second_main_type: String, // 122 COMMENT '测试-综艺节目次要类型',
                            zongyi_user_main_req: String, // 123 COMMENT '主要用户诉求',
                            zongyi_user_sub_req: String, // 124 COMMENT '次要用户诉求',
                            jiemu_aspect: String, // 125 COMMENT '节目看点',
                            viewing_experience: String, // 126 COMMENT '观影感受',
                            sentence_recommend: String, // 127 COMMENT '一句话推荐',
                            subject: String, // 128 COMMENT '本期主题',
                            series_number: String, // 129 COMMENT '电影/电视/动漫系列数',
                            c_prize: String, // 130 COMMENT '奖项',
                            award_situation: String, //131 COMMENT '获奖情况',
                            sentence_recommend_name: String, //132 COMMENT '一句话推荐文本',
                            zongyi_user_main_req_name: String, //133 COMMENT '主要用户诉求文本',
                            cinema_flag: String, //134 COMMENT '院线',
                            hidden_year: String, //135 COMMENT '出品年份',
                            period: String //136 COMMENT '期数（主要是综艺）'
                          )
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
