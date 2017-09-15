package Utils
import redis.clients.jedis.JedisPool
import redis.clients.jedis.JedisPoolConfig
/**
  * Created by baronfeng on 2017/9/14.
  */
class TestRedisPool {



  private var pool: JedisPool = null
  var ip: String = null
  var port: Int = 0
  var expire_time: Int = 0

  def this(ip: String, port: Int, expire_time: Int) {
    this()
    this.ip = ip
    this.port = port
    this.expire_time = expire_time
  }

  def getRedisPool: JedisPool = {
    if (pool == null) {
      this.synchronized {
        if (pool == null) {
          val config = new JedisPoolConfig
          config.setMaxTotal(500)
          config.setMaxIdle(30)
          config.setMinIdle(5)
          config.setMaxWaitMillis(1000 * 10)
          config.setTestWhileIdle(false)
          config.setTestOnBorrow(false)
          config.setTestOnReturn(false)
          pool = new JedisPool(config, ip, port, expire_time)
        }
      }
    }
    pool
  }
}
