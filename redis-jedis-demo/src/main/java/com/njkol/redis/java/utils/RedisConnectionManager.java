package com.njkol.redis.java.utils;

import java.time.Duration;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

/**
 * A connection manager for Redis
 * 
 * @author Nilanjan Sarkar
 *
 */
public class RedisConnectionManager {

	private static JedisPool jedisPool;
	private static Jedis jedis;

	public static Jedis getRedisConnection(String host,int port) {

		if (null == jedis) {

			JedisPoolConfig poolConfig = new JedisPoolConfig();
			poolConfig.setMaxTotal(128);
			poolConfig.setMaxIdle(128);
			poolConfig.setMinIdle(16);
			poolConfig.setTestOnBorrow(true);
			poolConfig.setTestOnReturn(true);
			poolConfig.setTestWhileIdle(true);
			poolConfig.setMinEvictableIdleTimeMillis(Duration.ofSeconds(60).toMillis());
			poolConfig.setTimeBetweenEvictionRunsMillis(Duration.ofSeconds(30).toMillis());
			poolConfig.setNumTestsPerEvictionRun(3);
			poolConfig.setBlockWhenExhausted(true);
			jedisPool = new JedisPool(poolConfig, host, port);
			jedis = jedisPool.getResource();
		}
		return jedis;
	}

	public static void closeConnection() {
		jedis.close();
		jedisPool.close();
	}
}
