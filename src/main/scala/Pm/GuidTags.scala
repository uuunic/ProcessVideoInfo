package Pm

import scala.collection.mutable.ArrayBuffer
import scala.io.Source
import org.apache.spark.unsafe.hash.Murmur3_x86_32.hashUnsafeBytes
import org.apache.spark.unsafe.types.UTF8String
/**
  * Created by baronfeng on 2017/9/3.
  */
object GuidTags {
  def hash(s: String, mod_num: Int = Math.pow(2, 24).toInt, seed: Int = 42) : Int = {
    val utf8 = UTF8String.fromString(s)

    val hashval = hashUnsafeBytes(utf8.getBaseObject, utf8.getBaseOffset, utf8.numBytes(), seed)
    ((hashval % mod_num) + mod_num) % mod_num
  }
  def main(args: Array[String]): Unit = {


    val filename = "fileopen.scala"
    var count = 0

    var guid: String = ""
    var vid: String = ""
    var tags: Array[String] = null
    var indice: Array[Int] = null
    var data: Array[Double] = null

    for (line <- Source.fromFile("data/guid_tags.txt").getLines) {
      count += 1
      if (line.trim != "")
        if(count % 4 == 1) {
          guid = line.trim
        } else if (count % 4 == 2) {
          vid = line
        } else if (count % 4 == 3) {
          tags = line.replace("(", "").replace(")", "").split(",").map(_.trim)
        } else if (count % 4 == 0) {
          val ret_arr = new ArrayBuffer[(String, Double)]
          val line_data = line.replace("),(", "###").split("###")
          indice = line_data(0).replace("(", "").replace(")", "").split(",").map(_.trim.toInt)
          data = line_data(1).replace("(", "").replace(")", "").split(",").map(_.trim.toDouble)
          for(tag<-tags.distinct){
            val hash_value = hash(tag)
            val index = indice.indexOf(hash_value)
            if(index == -1) {
              //println("tag: " + tag + ", hash_value: " + hash_value + ", index: " + index)

            } else {

              val weight = data(index)
              ret_arr += ((tag, weight))
            }
          }
          println(guid)
          println(vid)
          println("[" + ret_arr.sortWith(_._2 > _._2).map(line=>line._1 + "#" + line._2).mkString("], [") + "]\n")
        }
    }

  }

}
