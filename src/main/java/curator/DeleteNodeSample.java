package curator;

import java.util.concurrent.TimeUnit;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.api.CuratorEvent;
import org.apache.curator.framework.api.CuratorListener;
import org.apache.curator.framework.state.ConnectionState;
import org.apache.curator.framework.state.ConnectionStateListener;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.data.Stat;

public class DeleteNodeSample {
	
	public static void main(String[] args) {
		// --- 创建 zookeeper 会话 ---
		CuratorFramework client = CuratorFrameworkFactory.builder()
		.connectString("192.168.56.101:2181,192.168.56.101:2182,192.168.56.101:2183")
		.sessionTimeoutMs(5000)
		.connectionTimeoutMs(3000)
		.retryPolicy(new ExponentialBackoffRetry(1000, 3))
		.build();
		
		// --- 创建 session 监听器 ---
		client.getConnectionStateListenable().addListener(new ConnectionStateListener() {
			@Override
			public void stateChanged(CuratorFramework client, ConnectionState newState) {
				System.out.println("--- 客户端连接状态 ---");
				System.out.println("连接 isConnected = " + newState.isConnected());
				System.out.println("连接 name = " + newState.name());
				System.out.println("连接 ordinal = " + newState.ordinal());
				System.out.println();
			}
		});
		
		client.getCuratorListenable().addListener(new CuratorListener() {
			@Override
			public void eventReceived(CuratorFramework client, CuratorEvent event) throws Exception {
				System.out.println("--- 客户端状态 ---");
				System.out.println("客户端 type = " + event.getType());
				System.out.println("客户端 name = " + event.getName());
				System.out.println("客户端 path = " + event.getPath());
				System.out.println("客户端 data = " + event.getData());
				System.out.println("客户端 resultCode = " + event.getResultCode());
				System.out.println("客户端 ACL = " + event.getACLList());
				System.out.println("客户端 children = " + event.getChildren());
				System.out.println("客户端 context = " + event.getContext());
				System.out.println("客户端 Stat = " + event.getStat());
			}
			
		});
		
		// --- 连接会话 ---
		client.start();
		
		/*
		 * 只能删除叶子节点。
		 * client.delete().forPath(path); 
		 * 
		 * 删除一个节点，并递归删除其所有的子节点。
		 * client.delete().deletingChildrenIfNeeded().forPath(path);
		 * 
		 * 删除一个节点，强制指定版本进行删除。
		 * client.delete().withVersion(version).forPath(path);
		 * 
		 * 删除一个节点，强制保证删除。
		 * client.delete().guaranteed().forPath(path);
		 * 注意，guaranteed() 方法仅是一个保障，只要客户端会话有效，那么 Curator 会在后台持续性进行删除操作，直到节点删除成功。
		 * 因为在 ZooKeeper 客户端使用的过程中，可能会碰到这样的问题：客户端执行一个删除节点操作，但是由于一些网络原因，导致删除操作
		 * 失败。对于这样的异常，在有些场景中是致命的，如 “Master” 选举 —— 在这个场景中，ZooKeeper 客户端通常是通过节点的创建和
		 * 与删除来实现的。针对这个问题，Curator 中引入了一种重试机制：如果我们调用了 guaranteed() 方法，那么当客户端碰到上面这
		 * 些网络异常的时候，会记录下这次失败的删除操作，只要客户端会话有效，那么其就会在后台反复重试，知道节点删除成功。通过这样的措施，
		 * 就可以保证节点删除操作一定会生效。
		 */
		
		// --- 创建&删除节点 ---
		try {

			// 创建节点。
			client
			.create()
			.creatingParentsIfNeeded()
			.withMode(CreateMode.EPHEMERAL)
			.forPath("/zk-book/c1", "init".getBytes());
			
			System.out.println("创建节点：/zk-book/c1");
			TimeUnit.SECONDS.sleep(2);
			
			// 得到节点中的数据，并把该节点的状态保存到 stat 对象中。
			Stat stat = new Stat();
			byte[] data = client.getData().storingStatIn(stat).forPath("/zk-book/c1");
			System.out.println("获取节点：/zk-book/c1 中的数据");
			System.out.println("节点中保存的数据：" + new String(data, "UTF-8"));
			System.out.println("节点中保存的状态：" + stat);
			
			// 删除节点。
			client
			.delete()
			.deletingChildrenIfNeeded()
			.withVersion(stat.getVersion())
			.forPath("/zk-book");
			
			System.out.println("删除节点：/zk-book，及其子节点");
			
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			if (client != null) {
				client.close();
			}
		}
	}
}