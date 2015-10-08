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

/**
 * 简单地创建 zookeeper 节点。
 * 
 * @author FrankTaylor <mailto:franktaylor@163.com>
 * @since 2015/10/8
 * @version 1.0
 */
public class CreateNodeSample {
	
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
		
		// --- 创建节点 ---
		try {
			
			/*
			 * 注意，如果没有设置节点属性，那么 Curator 默认创建的是持久节点。
			 */
			// 创建一个持久节点，初始内容为空。
			client
			.create()
			.forPath("/testNode1");
			
			TimeUnit.SECONDS.sleep(2);
			
			// 创建一个持久节点，初始内容为abc。
			client
			.create()
			.forPath("/testNode2", "abc".getBytes());
			
			TimeUnit.SECONDS.sleep(2);
			
			/*
			 * 创建一个临时节点，初始内容为空。
			 * 
			 * 注意，通过 withMode 方法可以设置节点的属性，Curator 框架可创建 4 种属性的节点：
			 * 1、CreateMode.EPHEMERAL：临时节点
			 * 2、CreateMode.EPHEMERAL_SEQUENTIAL：临时顺序节点
			 * 3、CreateMode.PERSISTENT：持久节点
			 * 4、CreateMode.PERSISTENT_SEQUENTIAL：持久顺序节点
			 */
			client
			.create()
			.withMode(CreateMode.EPHEMERAL)
			.forPath("/testNode3");
			
			TimeUnit.SECONDS.sleep(2);
			
			/*
			 * 创建一个临时节点，并自动递归创建父节点。
			 * 
			 * creatingParentsIfNeeded() 这个方法非常有用，在使用 ZooKeeper 的过程中，开发人员经常会碰到 NoNodeException 异常，
			 * 其中一个可能的原因就是试图对一个不存在的父节点创建子节点。因此在每次创建节点前，都需要判断一下该父节点是否存在。而使用 Curator 之后，
			 * 通过调用该接口，就能够自动递归创建所有需要的父节点。
			 * 
			 * 同时要注意的是，由于在 ZooKeeper 中规定了所有非叶子节点必须为持久节点，于是在调用了该方法后，只有 path 参数中最后一个node是临时
			 * 节点，而其上层节点均为持久节点。
			 */
			client
			.create()
			.creatingParentsIfNeeded()
			.withMode(CreateMode.EPHEMERAL)
			.forPath("/testNode4/testNode4-1/testNode4-1-1");
			
			TimeUnit.SECONDS.sleep(2);
			
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			if (client != null) {
				client.close();
			}
		}
		
	}
}