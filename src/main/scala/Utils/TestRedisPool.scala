package Utils
import redis.clients.jedis.{JedisPoolConfig, JedisShardInfo, ShardedJedisPool}
/**
  * Created by baronfeng on 2017/9/14.
  */
class TestRedisPool {

  private var pool: ShardedJedisPool = null
//  var ip: String = null
//  var port: Int = 0
  var expire_time: Int = 0

  var ip_port_list: Array[(String, Int)] = Array[(String, Int)]()

  def this(ip: String, port: Int, expire_time: Int) {
    this()
//    this.ip = ip
//    this.port = port
    ip_port_list = Array((ip, port))
    this.expire_time = expire_time
  }

  def this(ip_port_list: Array[(String, Int)], expire_time: Int) {
    this()
    this.ip_port_list = ip_port_list
    this.expire_time = expire_time
  }

  def getRedisPool: ShardedJedisPool = {
    import collection.JavaConversions._
    if (pool == null) {
      this.synchronized {
        if (pool == null) {
          val config = new JedisPoolConfig
          config.setMaxTotal(500)

          config.setMaxIdle(30 * 60)
          config.setMinIdle(5 * 60)
          config.setMaxWaitMillis(1000 * 10)
          config.setTestWhileIdle(false)
          config.setTestOnBorrow(false)
          config.setTestOnReturn(false)
          val ip_port_info = ip_port_list.map(info => new JedisShardInfo(info._1, info._2, expire_time))
          pool = new ShardedJedisPool(config, ip_port_info.toList)
        }
      }
    }
    pool
  }
}
