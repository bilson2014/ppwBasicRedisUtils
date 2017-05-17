package com.paipianwang.pat.common.cache.util;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.core.BoundSetOperations;
import org.springframework.data.redis.core.HashOperations;
import org.springframework.data.redis.core.ListOperations;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.core.ValueOperations;
import org.springframework.data.redis.core.ZSetOperations;
import org.springframework.data.redis.serializer.Jackson2JsonRedisSerializer;
import org.springframework.stereotype.Component;

import com.paipianwang.pat.common.util.ValidateUtil;

@Component
@SuppressWarnings("unchecked")
public class RedisCacheFactory<T> {

	@Autowired
	private RedisTemplate redisTemplate = null;
	
	private Logger logger = LoggerFactory.getLogger(RedisCacheFactory.class);
	
	/**
	 * 缓存value操作
	 * @param key 缓存键值
	 * @param value 缓存内容
	 * @param time 过期时间
	 * @return true|false
	 */
	public boolean cacheValue(String key, T value, long time) {
		try {
			ValueOperations<String, T> operation = redisTemplate.opsForValue();
			operation.set(key, value);
			if(time > 0) 
				redisTemplate.expire(key, time, TimeUnit.SECONDS);
			return true;
		} catch (Exception e) {
			e.printStackTrace();
			logger.error("缓存["+ key +"]失败，value["+ value +"]",time);
		}
		return false;
	}
	
	/**
	 * 缓存value操作
	 * @param key 缓存键值
	 * @param value 缓存内容
	 * @return true|false
	 */
	public boolean cacheValue(String key, T value) {
		return cacheValue(key, value, -1);
	}
	
	public <T> T getValue(String key) {
		try {
			ValueOperations<String, T> operation = redisTemplate.opsForValue();
			return operation.get(key);
		} catch (Exception e) {
			e.printStackTrace();
			logger.error("获取缓存失败key["+ key +"],error["+ e +"]");
		}
		return null;
	}
	
	/**
	 * 判断缓存是否存在
	 * @param key 缓存键值
	 * @return true|false
	 */
	public boolean containsValueKey(String key) {
		return containsKey(key);
	}
	
	public boolean containsSetKey(String key) {
		return containsKey(key);
	}
	
	public boolean containsListKey(String key) {
		return containsKey(key);
	}
	
	public boolean containsKey(String key) {
		try {
			return redisTemplate.hasKey(key);
		} catch (Exception e) {
			e.printStackTrace();
			logger.error("判断缓存存在失败key["+ key +"],error["+ e +"]");
		}
		return false;
	}
	
	public boolean removeValue(String key) {
		return remove(key);
	}
	
	public boolean removeValue(List<String> keyList) {
		return remove(keyList);
	}
	
	public boolean removeSet(String key) {
		return remove(key);
	}
	
	public boolean removeList(String key) {
		return remove(key);
	}
	
	public boolean removeList(List<String> keyList) {
		return remove(keyList);
	}

	public boolean remove(String key) {
		try {
			redisTemplate.delete(key);
			return true;
		} catch (Exception e) {
			e.printStackTrace();
			logger.error("获取缓存失败key[" + key + "], error[" + e + "]");
		}
		return false;
	}
	
	public boolean remove(List<String> keyList) {
		try {
			redisTemplate.delete(keyList);
			return true;
		} catch (Exception e) {
			e.printStackTrace();
			logger.error("获取缓存失败key[" + keyList + "], error[" + e + "]");
		}
		return false;
	}
	
	public boolean removeSetWithValue(Map<String, Set<T>> map) {
		try {
			for (String key : map.keySet()) {
				BoundSetOperations<String, T> setOps =  redisTemplate.boundSetOps(key);
				Set<T> dataList = map.get(key);
				setOps.remove(dataList);
			}
			return true;
		} catch (Exception e) {
			logger.error("获取缓存失败key[" + map + "], error[" + e + "]");
		}
		return false;
	}
	
	public boolean cacheSet(String key, Set<T> dataSet) {
		return cacheSet(key, dataSet, -1);
	}
	
	public boolean cacheSet(String key, T value) {
		return cacheSet(key, value, -1);
	}

	public boolean cacheSet(String key, Set<T> dataSet, long time) {
		try {
			if(ValidateUtil.isValid(dataSet)) {
				BoundSetOperations<String, T> setOps =  redisTemplate.boundSetOps(key);
				
				for (T t : dataSet) {
					setOps.add(t);
				}
				if(time > 0)
					redisTemplate.expire(key, time, TimeUnit.SECONDS);
				return true;
			}
		} catch (Exception e) {
			e.printStackTrace();
			logger.error("缓存[" + key + "]失败, value[" + dataSet + "]", e);
		}
		return false;
	}
	
	public boolean cacheSet(String key, T value, long time) {
		try {
			if(value != null) {
				BoundSetOperations<String, T> setOps =  redisTemplate.boundSetOps(key);
				setOps.add(value);
				if(time > 0)
					redisTemplate.expire(key, time, TimeUnit.SECONDS);
				return true;
			}
		} catch (Exception e) {
			e.printStackTrace();
			logger.error("缓存[" + key + "]失败, value[" + value + "]", e);
		}
		return false;
	}
	
	public Set<T> getSet(String key) {
		try {
			BoundSetOperations<String, T> setOps = redisTemplate.boundSetOps(key);
			return setOps.members();
		} catch (Exception e) {
			logger.error("获取set缓存失败key[" + key + ", error[" + e + "]");
		}
		return null;
	}
	
	public boolean cacheList(String key, List<T> dataList, long time) {
		try {
			ListOperations<String, T> listOps = redisTemplate.opsForList();
			listOps.rightPushAll(key, dataList);
			if(time > 0)
				redisTemplate.expire(key, time, TimeUnit.SECONDS);
			return true;
		} catch (Exception e) {
			logger.error("缓存[" + key + "]失败, value[" + dataList + "]", e);
		}
		
		return false;
	}
	
	public boolean cacheList(String key, List<T> dataList) {
		return cacheList(key, dataList, -1);
	}
	
	public boolean cacheList(String key, T value) {
		return cacheList(key, value, -1);
	}
	
	public boolean cacheList(String key, T value, long time) {
		try {
			ListOperations<String, T> listOps = redisTemplate.opsForList();
			listOps.rightPushAll(key, value);
			if(time > 0)
				redisTemplate.expire(key, time, TimeUnit.SECONDS);
			return true;
		} catch (Exception e) {
			logger.error("缓存[" + key + "]失败, value[" + value + "]", e);
		}
		
		return false;
	}
	
	public List<T> getList(String key, long start, long end) {
		try {
			ListOperations<String, T> listOps = redisTemplate.opsForList();
			return listOps.range(key, start, end);
		} catch (Exception e) {
			logger.error("获取list缓存失败key[" + key + ", error[" + e + "]");
		}
		return null;
	}
	
	public long getListSize(String key) {
		try {
			ListOperations<String, T> listOps = redisTemplate.opsForList();
			return listOps.size(key);
		} catch (Exception e) {
			logger.error("获取list长度失败key[" + key + "], error[" + e + "]");
		}
		return 0;
	}
	
	public boolean removeOneOfListFromRight(String key) {
		try {
			ListOperations<String, T> listOps = redisTemplate.opsForList();
			listOps.rightPop(key);
			return true;
		} catch (Exception e) {
			logger.error("移除list缓存失败key[" + key + ", error[" + e + "]");
		}
		return false;
	}
	
	public boolean removeOneOfListFromLeft(String key) {
		try {
			ListOperations<String, T> listOps = redisTemplate.opsForList();
			listOps.leftPop(key);
			return true;
		} catch (Exception e) {
			logger.error("移除list缓存失败key[" + key + ", error[" + e + "]");
		}
		return false;
	}
	
	public boolean cacheZSort(String key, T value) {
		return cacheZSort(key, value, 0);
	}
	
	public boolean cacheZSort(String key, Set<T> dataSet) {
		return cacheZSort(key, dataSet, 0);
	}
	
	public boolean cacheZSort(String key, T value, long score) {
		try {
			ZSetOperations<String, T> zOps = redisTemplate.opsForZSet();
			zOps.add(key, value, score);
			return true;
		} catch (Exception e) {
			e.printStackTrace();
		}
		return false;
	}
	
	public boolean cacheZSort(String key, Set<T> dataSet, long score) {
		try {
			if(ValidateUtil.isValid(dataSet)) {
				ZSetOperations<String, T> zOps = redisTemplate.opsForZSet();
				for (T t : dataSet) {
					zOps.add(key, t, score);
				}
				return true;
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		return false;
	}
	
	public Set<T> getZSort(String key, long start, long end) {
		try {
			ZSetOperations<String, T> zOps = redisTemplate.opsForZSet();
			return zOps.range(key, start, end);
		} catch (Exception e) {
			e.printStackTrace();
		}
		return null;
	}
	
	public Set<T> getZSort(String key, double minScore, double maxScore) {
		try {
			ZSetOperations<String, T> zOps = redisTemplate.opsForZSet();
			return zOps.rangeByScore(key, minScore, maxScore);
		} catch (Exception e) {
			e.printStackTrace();
		}
		return null;
	}
	
	public long getZSortSize(String key) {
		try {
			ZSetOperations<String, T> zOps = redisTemplate.opsForZSet();
			return zOps.size(key);
		} catch (Exception e) {
			e.printStackTrace();
		}
		return 0;
	}
	
	public boolean cacheHash(String key, Map<String, T> map) {
		return cacheHash(key, map, -1);
	}
	
	public boolean cacheHash(String key, Map<String, T> map, long time) {
		try {
			if(ValidateUtil.isValid(map)) {
				HashOperations<String, String, T> hashOperation = redisTemplate.opsForHash();
				hashOperation.putAll(key, map);
				if(time > 0)
					redisTemplate.expire(key, time, TimeUnit.SECONDS);
				return true;
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		return false;
	}
	
	public boolean cacheHash(String key, String field, T value) {
		try {
			HashOperations<String, String, T> hashOperation = redisTemplate.opsForHash();
			hashOperation.put(key, field, value);
			return true;
		} catch (Exception e) {
			e.printStackTrace();
		}
		return false;
	}
	
	public List<T> getHash(String key, List<String> keyList, T t) {
		try {
			redisTemplate.setHashValueSerializer(new Jackson2JsonRedisSerializer<T>((Class<T>) t.getClass()));
			HashOperations<String, String, T> hashOperation = redisTemplate.opsForHash();
			final List<T> list = (List<T>)(List<?>)hashOperation.multiGet(key, keyList);
			return list;
		} catch (Exception e) {
			e.printStackTrace();
		}
		return null;
	}
	
	public List<T> getHash(String key, Set<String> keyList, T t) {
		try {
			redisTemplate.setHashValueSerializer(new Jackson2JsonRedisSerializer<T>((Class<T>) t.getClass()));
			HashOperations<String, String, T> hashOperation = redisTemplate.opsForHash();
			final List<T> list = hashOperation.multiGet(key, keyList);
			return list;
		} catch (Exception e) {
			e.printStackTrace();
		}
		return null;
	}
	
	public <T> T getHash(String key, String field) {
		try {
			HashOperations<String, String, T> hashOperation = redisTemplate.opsForHash();
			T t = hashOperation.get(key, field);
			return t;
		} catch (Exception e) {
			e.printStackTrace();
		}
		return null;
	}
	
	public boolean removeHash(String key) {
		return remove(key);
	}
	
	public boolean removeHash(String key, String... field) {
		try {
			HashOperations<String, String, T> hashOperation = redisTemplate.opsForHash();
			hashOperation.delete(key, field);
			return true;
		} catch (Exception e) {
			e.printStackTrace();
		}
		return false;
	}
	
}
