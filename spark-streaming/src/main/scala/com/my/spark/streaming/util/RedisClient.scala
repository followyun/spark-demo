package com.my.spark.streaming.util

import org.apache.commons.pool2.impl.GenericObjectPoolConfig
import redis.clients.jedis.JedisPool

/**
  */
object RedisClient extends Serializable {
  @transient private lazy val pool: JedisPool = {
    val maxTotal = 10
    val maxIdle = 10
    val minIdle = 1
    val redisHost = "slave1"
    val redisPort = 6379
    val redisTimeout = 30000
    val poolConfig = new GenericObjectPoolConfig()
    poolConfig.setMaxTotal(maxTotal)
    poolConfig.setMaxIdle(maxIdle)
    poolConfig.setMinIdle(minIdle)
    poolConfig.setTestOnBorrow(true)
    poolConfig.setTestOnReturn(false)
    poolConfig.setMaxWaitMillis(100000)

    val hook = new Thread {
      override def run() = pool.destroy()
    }

    sys.addShutdownHook(hook)

    new JedisPool(poolConfig, redisHost, redisPort, redisTimeout)
  }

  def getPool: JedisPool = {
    assert(pool != null)
    pool
  }

  def main(args: Array[String]): Unit = {
    val jedis = RedisClient.getPool.getResource
    jedis.set("test", "value")
    RedisClient.getPool.returnResource(jedis)
  }
}
