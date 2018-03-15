package com.nebo.zookeeper.queue;
import java.io.IOException;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import com.nebo.zookeeper.util.ActiveKeyValueStore;
/**
 * 监控队列是否已满，触发后续操作
 * @author dajiangtai
 *
 */
public class DistributedQueue implements Watcher {
	private ActiveKeyValueStore _store;
	public String rootNode="/queue";// 代表根节点
	private String startPath = "/queue/go";

	public DistributedQueue(String hosts) throws IOException,
			InterruptedException, KeeperException {
		_store = new ActiveKeyValueStore();
		// 连接Zookeeper
		_store.connect(hosts);
		// 判断根节点是否存在
		_store.exists(rootNode, null);
		// 注册监视startPath
		_store.registerWatcher(startPath, this);
	}

	public void process(WatchedEvent event) {
		// TODO Auto-generated method stub
		System.out.println("--------已经达到触发条件--------");
		if (event.getType() == Event.EventType.NodeCreated
				&& event.getPath().equals("/queue/go")) {
			System.out.println("所有成员已到达，进入下一个环节！");
		}
	}

	public static void main(String[] args) throws IOException,
			InterruptedException, KeeperException {
		String hosts = "192.168.8.130:2181";
		// 创建watcher
		DistributedQueue queue = new DistributedQueue(hosts);
		// 然后一直处于监控状态
		Thread.sleep(Long.MAX_VALUE);
	}
}
