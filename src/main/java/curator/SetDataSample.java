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

public class SetDataSample {
	
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
		 * 更新一个节点的数据内容。
		 * client.setData().forPath(path)
		 * 调用该方法后，会返回一个 stat 对象。
		 * 
		 * 更新一个节点的数据内容，强制指定版本进行更新。
		 * client.setData().withVersion(version).forPath(path)
		 * 注意，withVersion 接口就是用来实现 CAS (Compare and Swap) 的，version（版本信息） 通常
		 * 是从一个旧的 stat 对象中获取到的。 
		 */
		
		// --- 创建&删除节点 ---
		try {

			// 创建节点。
			client
			.create()
			.creatingParentsIfNeeded()
			.withMode(CreateMode.EPHEMERAL)
			.forPath("/zk-book", "init".getBytes());
			
			System.out.println("创建节点：/zk-book");
			TimeUnit.SECONDS.sleep(2);
			
			// 得到节点中的数据，并把该节点的状态保存到 stat 对象中。
			Stat stat = new Stat();
			byte[] data = client.getData().storingStatIn(stat).forPath("/zk-book");
			System.out.println("获取节点：/zk-book 中的数据");
			System.out.println("节点中保存的数据：" + new String(data, "UTF-8"));
			System.out.println("节点中保存的状态：" + stat);
			
			TimeUnit.SECONDS.sleep(2);
			// 修改该节点中特定版本的内容。
			System.out.println(client.setData().withVersion(stat.getVersion()).forPath("/zk-book").getVersion());
			
			// 由于此次使用了过期的 stat 对象，导致版本号不对，所以将抛出异常：KeeperErrorCode = BadVersion for /zk-book
			System.out.println(client.setData().withVersion(stat.getVersion()).forPath("/zk-book").getVersion());
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			if (client != null) {
				client.close();
			}
		}
	}
}