package xiangmu.utils

import org.apache.commons.pool2.impl.GenericObjectPoolConfig
import redis.clients.jedis.JedisPool

object RedisUtil {

  // 连接redis的工具方法。
  // 获得连接
  private val jedisPool = new JedisPool(new GenericObjectPoolConfig,"192.168.84.143",6379,30000,"123456",4)

  def getJedis = jedisPool.getResource

}
