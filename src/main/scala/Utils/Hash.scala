package Utils

import org.apache.spark.SparkException
import org.apache.spark.unsafe.hash.Murmur3_x86_32._
import org.apache.spark.unsafe.types.UTF8String

import scala.util.hashing.MurmurHash3.bytesHash

/**
  * Created by baronfeng on 2017/9/4.
  */
object Hash {
  def hash_own(s: String, seed1: Int = 41, seed2:Int = 0xe17a1465) : Long = {
    val byte_s = s.getBytes()

    val data1: Long = bytesHash(byte_s, seed1)
    val data2: Long = bytesHash(byte_s, seed2)


    (data1 << 32) +  data2

  }


  def hash_tf_idf_no_abs(term: Any, mod_num: Int = Math.pow(2, 24).toInt, seed: Int = 42) : Int = {
    term match {
      case b: Boolean => hashInt(if (b) 1 else 0, seed)
      case b: Byte => hashInt(b, seed)
      case s: Short => hashInt(s, seed)
      case i: Int => hashInt(i, seed)
      case l: Long => hashLong(l, seed)
      case f: Float => hashInt(java.lang.Float.floatToIntBits(f), seed)
      case d: Double => hashLong(java.lang.Double.doubleToLongBits(d), seed)
      case s: String => {
        val utf8 = UTF8String.fromString(s)
        hashUnsafeBytes(utf8.getBaseObject, utf8.getBaseOffset, utf8.numBytes(), seed)
      }
      case _ => throw new SparkException("HashingTF with murmur3 algorithm does not " +
        s"support type ${term.getClass.getCanonicalName} of input data.")
    }
  }

  def hash_tf_idf(term: Any, mod_num: Int = Math.pow(2, 24).toInt, seed: Int = 42) : Int = {
    val hashval = hash_tf_idf_no_abs(term, seed)
    ((hashval % mod_num) + mod_num) % mod_num
  }

  @deprecated
  def stringHash64(str: String, seed : Int=0xe17a1465): Long = {
    val data = str.getBytes
    val length = data.length
  //  val seed = 0xe17a1465
    val m = 0xc6a4a7935bd1e995L
    val r = 47

    var h = (seed & 0xffffffffL) ^ (length * m)

    val length8 = length / 8

    for (i <- 0 until length8) {
      val i8 = i * 8
      var k = (data(i8 + 0) & 0xff).toLong + ((data(i8 + 1) & 0xff).toLong << 8) + ((data(i8 + 2) & 0xff).toLong << 16) + ((data(i8 + 3) & 0xff).toLong << 24) + ((data(i8 + 4) & 0xff).toLong << 32) + ((data(i8 + 5) & 0xff).toLong << 40) + ((data(i8 + 6) & 0xff).toLong << 48) + ((data(i8 + 7) & 0xff).toLong << 56)

      k *= m
      k ^= k >>> r
      k *= m

      h ^= k
      h *= m
    }

    if (length % 8 >= 7)
      h ^= (data((length & ~7) + 6) & 0xff).toLong << 48
    if (length % 8 >= 6)
      h ^= (data((length & ~7) + 5) & 0xff).toLong << 40
    if (length % 8 >= 5)
      h ^= (data((length & ~7) + 4) & 0xff).toLong << 32
    if (length % 8 >= 4)
      h ^= (data((length & ~7) + 3) & 0xff).toLong << 24
    if (length % 8 >= 3)
      h ^= (data((length & ~7) + 2) & 0xff).toLong << 16
    if (length % 8 >= 2)
      h ^= (data((length & ~7) + 1) & 0xff).toLong << 8
    if (length % 8 >= 1) {
      h ^= (data(length & ~7) & 0xff).toLong
      h *= m
    }

    h ^= h >>> r
    h *= m
    h ^= h >>> r

    h
  }

}
