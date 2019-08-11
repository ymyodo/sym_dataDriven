package com.sym.redis.jedis.singleNode;

import org.junit.Before;
import org.junit.Test;
import redis.clients.jedis.Jedis;
/**
 * jedis测试类
 * @author 沈燕明
 *
 */
public class JedisTest {
	private Jedis jedis;

	@Before
	public void Init() {
		jedis = JedisPoolUtil.getJedis();
	}

	@Test
	public void set() {
		String str = "沈燕明,2018-02-05";
		jedis.set("str", str);
	}

	@Test
	public void get() {
		String str = jedis.get("str");
		System.out.println(str);
	}

	@Test
	public void del() {
		Long del = jedis.del("str");
		System.out.println("删除了..." + del + "条");
	}

	@Test
	public void getset() {
		String str = jedis.getSet("str", "沈燕明你是最棒的");
		System.out.println(str);
		String strNew = jedis.get("str");
		System.out.println(strNew);

	}

	@Test
	public void incr() {
		Long incr = jedis.incr("count");
		System.out.println("返回加完后的结果：" + incr);
	}
}
