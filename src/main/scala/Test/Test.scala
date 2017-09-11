package Test


import Utils.Hash
import Utils.Hash.hash_tf_idf
import org.apache.spark.sql.Row
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.unsafe.hash.Murmur3_x86_32._

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

object TopK {

  val K = 3

  val ord = Ordering.by[(String, Int), Int](_._2).reverse

  def main(args: Array[String]) {
    // 执行 wordcount
    val conf = new SparkConf().setAppName("TopK")
    val spark = new SparkContext(conf)
    val textRDD = spark.textFile("hdfs://10.0.8.162:9000/home/yuzx/input/wordcount.txt")
    val countRes = textRDD.flatMap(line => line.split(" ")).map(word => (word, 1)).reduceByKey(_ + _)

    // debug mapreduce 的结果
    countRes.foreach(println)

    /*
     每个 RDD 分区内进行 TOP K 计算
     需要每个分区内有自己的桶，如果整个程序使用一个 heap（将 heap 设定为成员变量） 会不正确
     为什么呢？
      */
    val topk = countRes.mapPartitions(iter => {
      val heap = new mutable.PriorityQueue[(String, Int)]()(ord)
      while (iter.hasNext) {
        val n = iter.next
        println("分区计算：" + n)
        putToHeap(heap, n)
      }

      heap.iterator
    }).collect()

    println("分区结果：")
    topk.foreach(println)

    // 每个分区的 TOP K 合并，计算总的 TopK
    val heap = new mutable.PriorityQueue[(String, Int)]()(ord)
    val iter = topk.iterator
    while (iter.hasNext) {
      putToHeap(heap, iter.next)
    }

    println("最终结果：")
    while (heap.nonEmpty) {
      println(heap.dequeue())
    }

    spark.stop()

  }

  def putToHeap(heap: mutable.PriorityQueue[(String, Int)], data: (String, Int)): Unit = {
    if (heap.nonEmpty && heap.size >= K) {
      if (heap.head._2 < data._2) {
        heap += data
        heap.dequeue()
      }
    } else {
      heap += data
    }
  }


  // Row 特指目前的vid_info里面的row，之后可能会泛化。
  def filter_data(line: Row) : Seq[(String, Long, Double)] = {  // tag, tag_id, weight

    val vid = line.getString(0)
    val duration = line.getInt(1)
    val tags_id = line.getAs[Seq[Long]](2)  // 这个是核对调试用的
    val tags_source = line.getAs[Seq[String]](3)
    val features_index = line.getAs[Seq[Int]](4)
    val features_data = line.getAs[Seq[Double]](5)
    val ret_arr = new ArrayBuffer[(String, Long, Double)]
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
          ret_arr += ((tag_pure, tag_id, weight))
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
          ret_arr += ((tag_pure, tag_id, weight))
        }
      }
    }
    ret_arr
  }

}