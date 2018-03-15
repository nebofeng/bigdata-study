package com.nebo.zookeeper.lock;

import com.nebo.zookeeper.lock.ConcurrentTest.ConcurrentTask;

/**
 * 构造多个线程测试分布式锁服务
 * 
 * @author dajiangtai
 * 
 */
public class ZkTest {
	public static void main(String[] args) {
		ConcurrentTask[] tasks = new ConcurrentTask[5];
		for (int i = 0; i < tasks.length; i++) {
			ConcurrentTask task = new ConcurrentTask() {
				public void run() {
					DistributedLock lock = null;
					try {
						lock = new DistributedLock("192.168.8.130:2181",
								"write");
						// 获取锁
						lock.lock();
						// 获取锁之后实现具体的业务
						System.out.println(lock.myZnode + "节点执行具体业务！");
					} catch (Exception e) {
						e.printStackTrace();
					} finally {
						// 解锁
						lock.unlock();
					}

				}
			};
			tasks[i] = task;
		}
		new ConcurrentTest(tasks);// 执行task
	}
}