package com.nebo.zookeeper.configmanage;

import java.io.IOException;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import org.apache.zookeeper.KeeperException;

import com.nebo.zookeeper.util.ActiveKeyValueStore;

/**
 * Updater 更新数据
 * @author dajiangtai
 *
 */
public class ConfigUpdater {
	public static final String PATH = "/configuration";
	private ActiveKeyValueStore _store;
	private Random _random = new Random();

	public ConfigUpdater(String hosts) throws IOException, InterruptedException {
		_store = new ActiveKeyValueStore();//定义一个类
		_store.connect(hosts);//连接Zookeeper
	}

	public void run() throws InterruptedException, KeeperException {
		// noinspection InfiniteLoopStatement
		while (true) {
			String value = _random.nextInt(100) + "";
			_store.write(PATH, value);//向znode写数据，也可以将xml文件写进去
			System.out.printf("Set %s to %s\n", PATH, value);
			TimeUnit.SECONDS.sleep(_random.nextInt(10));
		}
	}

	public static void main(String[] args) throws IOException,
			InterruptedException, KeeperException {
		String hosts="192.168.8.130:2181";
		ConfigUpdater updater = new ConfigUpdater(hosts);
		updater.run();
	}
}