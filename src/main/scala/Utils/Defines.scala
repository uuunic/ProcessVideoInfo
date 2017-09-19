package Utils

/**
  * Created by baronfeng on 2017/9/15.
  */
object Defines {
  // tag停止词表
  val STOP_WORDS_PATH: String = "/user/baronfeng/useful_dataset/stop_words.txt"

  val TAG_HASH_LENGTH: Int = Math.pow(2, 24).toInt // 目前暂定2^24为feature空间

}
