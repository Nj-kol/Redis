package com.njkol.redis.java;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.log4j.Logger;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import static org.junit.Assert.*;

import com.njkol.redis.java.utils.RedisConnectionManager;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.Pipeline;
import redis.clients.jedis.Transaction;

/**
 * 
 * @author Nilanjan Sarkar
 * 
 *         Unit tests for testing Jedis client
 *
 */
public class JedisTests {

	private static Jedis jedis;
	private final static Logger logger = Logger.getLogger(JedisTests.class);

	@BeforeClass
	public static void setUp() {
		jedis = RedisConnectionManager.getRedisConnection("localhost", 6379);
	}

	@Test
	public void testStrings() {
		jedis.set("events/city/rome", "32,15,223,828");
		String cachedResponse = jedis.get("events/city/rome");
		logger.info("The string value retrieved is : " + cachedResponse);
		assertEquals("32,15,223,828", cachedResponse);
	}

	@Test
	public void testLists() {
		jedis.lpush("queue#tasks", "firstTask");
		jedis.lpush("queue#tasks", "secondTask");
		String task = jedis.lpop("queue#tasks");
		logger.info("The popped task is : " + task);
		assertEquals("secondTask", task);
	}

	@Test
	public void testSets() {
		jedis.sadd("nicknames", "nickname#1");
		jedis.sadd("nicknames", "nickname#2");
		jedis.sadd("nicknames", "nickname#1");
		Set<String> nicknames = jedis.smembers("nicknames");
		boolean exists = jedis.sismember("nicknames", "nickname#1");
		logger.info("The members of the set are : " + nicknames);
		assertTrue(exists);
	}

	@Test
	public void testHashes() {
		jedis.hset("user#1", "name", "Peter");
		jedis.hset("user#1", "job", "politician");
		Map<String, String> fields = jedis.hgetAll("user#1");
		logger.info("The members of the hash user#1 : " + fields);
		String job = fields.get("job");
		assertEquals("politician", job);
	}

	@Test
	public void testSortedSets() {
		Map<String, Long> scores = new HashMap<String, Long>();
		scores.put("PlayerOne", 3000l);
		scores.put("PlayerTwo", 1500l);
		scores.put("PlayerThree", 8200l);

		scores.keySet().forEach(player -> {
			jedis.zadd("ranking", scores.get(player), player);
		});

		long rank = jedis.zrevrank("ranking", "PlayerOne");
		logger.info("The members of the sorted set are : " + scores);
		assertEquals(1, rank);
	}

	@Test
	public void testPipelines() {

		logger.info("Start pipeling test");
		Pipeline commandpipe = jedis.pipelined();
		commandpipe.set("westbengal:1", "Kolkata");
		commandpipe.set("westbengal:2", "Siliguri");
		commandpipe.set("westbengal:3", "Malda");
		commandpipe.set("westbengal:4", "Kalyani");
		List<Object> results = commandpipe.syncAndReturnAll();
		logger.info("End pipeling test");
		assertEquals(jedis.get("westbengal:1"), "Kolkata");
	}

	@Test
	public void testTransaction() {
		Transaction transactionableCommands = jedis.multi();
		transactionableCommands.sadd("keys-1", "name" + "dsd");
		transactionableCommands.exec();
	}

	@AfterClass
	public static void tearDown() {
		jedis.flushAll();
		RedisConnectionManager.closeConnection();
	}
}