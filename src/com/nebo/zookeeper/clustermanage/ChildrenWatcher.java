package com.nebo.zookeeper.clustermanage;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;

import com.nebo.zookeeper.util.ActiveKeyValueStore;

/**
 * Zookeeper-集群管理
 * Watcher 监控，使用原生api连接Zookeeper
 * @author dajiangtai
 * 
 */
public class ChildrenWatcher implements Watcher {
	private ActiveKeyValueStore _store;
	//临时节点列表
	List<String> oldChildrenList = new ArrayList<String>();

	public ChildrenWatcher(String hosts) throws InterruptedException, IOException, KeeperException {
		_store = new ActiveKeyValueStore();
		// 连接Zookeeper
		_store.connect(hosts);
		//查询临时节点集合
		oldChildrenList = _store.readChildren(ClientRegister1.rootPATH, this);
	}

	/**
	 * 读取znode节点数据
	 * 
	 * @throws InterruptedException
	 * @throws KeeperException
	 */
	public void displayConfig() throws InterruptedException, KeeperException {
		try {
			//查询当前临时节点集合
			List<String> currentChildrenList = _store.readChildren(ClientRegister1.rootPATH,
					this);
			for (String child : currentChildrenList) {
				if (!oldChildrenList.contains(child)) {
					System.out.println("新增加的regionServer节点为：" + child);
				}
			}
			for (String child : oldChildrenList) {
				if (!currentChildrenList.contains(child)) {
					System.out.println("挂掉的regionServer节点为：" + child);
				}
			}
			//临时子节点集合更新
			this.oldChildrenList = currentChildrenList;
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	// 监控临时节点变化
	public void process(WatchedEvent event) {
		if (event.getType() == Event.EventType.NodeChildrenChanged) {
			try {
				// 调用具体业务代码
				displayConfig();
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (KeeperException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}

	}

	public static void main(String[] args) throws IOException,
			InterruptedException, KeeperException {
		String hosts = "192.168.8.130:2181";
		// 创建watcher
		ChildrenWatcher watcher = new ChildrenWatcher(hosts);
		// 调用display方法
		watcher.displayConfig();
		// 然后一直处于监控状态
		Thread.sleep(Long.MAX_VALUE);
	}
}