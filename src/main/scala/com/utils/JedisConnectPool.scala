package com.utils

import redis.clients.jedis.{Jedis, JedisPool, JedisPoolConfig}

object JedisConnectPool {

  private val poolConfig = new JedisPoolConfig()

  poolConfig.setMaxIdle(10)
  poolConfig.setMaxTotal(20)

  private  val pool=new JedisPool(poolConfig,"hadoop001",6379,10000,"123456")

  def getConnection():Jedis={
    pool.getResource
  }
}
