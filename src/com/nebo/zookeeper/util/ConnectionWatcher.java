package com.nebo.zookeeper.util;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
/**
 * zookeeper连接
 * @author dajiangtai
 *
 */
public class ConnectionWatcher implements Watcher {
	private static final int SESSION_TIMEOUT = 5000;
	protected ZooKeeper zk;
	private CountDownLatch _connectedSignal = new CountDownLatch(1);

	public void connect(String hosts) throws IOException, InterruptedException {
		zk = new ZooKeeper(hosts, SESSION_TIMEOUT, this);
		_connectedSignal.await();
	}

	public void process(WatchedEvent event) {
		if (event.getState() == Event.KeeperState.SyncConnected) {
			_connectedSignal.countDown();
		}
	}

	public void close() throws InterruptedException {
		zk.close();
	}
}
