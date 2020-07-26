package src.main.scala.stream_project.util

import redis.clients.jedis.{Jedis, JedisPool, JedisPoolConfig}


object RedisUtil {
    //连接池配置文件
    private val jedisPoolConfig: JedisPoolConfig = new JedisPoolConfig()

    jedisPoolConfig.setMaxTotal(100) //最大连接数  连接多少jedis数

    jedisPoolConfig.setMaxIdle(20) //最大空闲   可以为空的jedis

    jedisPoolConfig.setMinIdle(20) //最小空闲

    jedisPoolConfig.setBlockWhenExhausted(true) //忙碌时是否等待

    jedisPoolConfig.setMaxWaitMillis(500) //忙碌时等待时长 毫秒

    jedisPoolConfig.setTestOnBorrow(false) //每次获得连接的进行测试


    //public JedisPool(poolConfig: GenericObjectPoolConfig, host: String, port: Int)
    //传入2个参数
    //通过new获取一个连接池
    private val jedisPool: JedisPool = new JedisPool(jedisPoolConfig, "hadoop201", 6379)

    // 直接得到一个 Redis 的连接 ： 得到一个jedis的客户端
    def getJedisClient: Jedis = jedisPool.getResource
}
