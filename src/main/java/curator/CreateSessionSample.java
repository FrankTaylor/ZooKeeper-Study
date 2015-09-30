package curator;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.api.CuratorEvent;
import org.apache.curator.framework.api.CuratorListener;
import org.apache.curator.framework.state.ConnectionState;
import org.apache.curator.framework.state.ConnectionStateListener;
import org.apache.curator.retry.ExponentialBackoffRetry;

public class CreateSessionSample {
	
	public static void main(String[] args) {
		
		// --- 创建 CuratorFramework 客户端  ---
		CuratorFramework client1 = createCuratorFramework();
		CuratorFramework client2 = createCuratorFrameworkWithFluentStyle();
		
		ExecutorService exec = Executors.newSingleThreadExecutor();
		// --- 给 CuratorFramework 的客户端加上连接状态监听器  ---
		
		System.out.println("给 CuratorFramework 的对象 client1 加上连接状态监听器！");
		client1.getConnectionStateListenable().addListener(new ConnectionStateListener() {
			@Override
			public void stateChanged(CuratorFramework client, ConnectionState newState) {
				System.out.println("--- 客户端 client1 连接状态 ---");
				System.out.println("client1连接 isConnected = " + newState.isConnected());
				System.out.println("client1连接 name = " + newState.name());
				System.out.println("client1连接 ordinal = " + newState.ordinal());
				System.out.println();
			}
		}, exec);
		
		System.out.println("给 CuratorFramework 的对象 client1 加上客户端监听器！");
		client1.getCuratorListenable().addListener(new CuratorListener() {
			@Override
			public void eventReceived(CuratorFramework client, CuratorEvent event) throws Exception {
				System.out.println("--- 客户端 client1 状态 ---");
				System.out.println("client1客户端 type = " + event.getType());
				System.out.println("client1客户端 name = " + event.getName());
				System.out.println("client1客户端 path = " + event.getPath());
				System.out.println("client1客户端 data = " + event.getData());
				System.out.println("client1客户端 resultCode = " + event.getResultCode());
				System.out.println("client1客户端 ACL = " + event.getACLList());
				System.out.println("client1客户端 children = " + event.getChildren());
				System.out.println("client1客户端 context = " + event.getContext());
				System.out.println("client1客户端 Stat = " + event.getStat());
			}
			
		}, exec);
		
		System.out.println("给 CuratorFramework 的对象 client2 加上连接状态监听器！");
		client2.getConnectionStateListenable().addListener(new ConnectionStateListener() {
			@Override
			public void stateChanged(CuratorFramework client, ConnectionState newState) {
				System.out.println("--- 客户端 client2 连接状态 ---");
				System.out.println("client2连接 isConnected = " + newState.isConnected());
				System.out.println("client2连接 name = " + newState.name());
				System.out.println("client2连接 ordinal = " + newState.ordinal());
				System.out.println();
			}
		}, exec);
		
		System.out.println("给 CuratorFramework 的对象 client2 加上客户端监听器！");
		client2.getCuratorListenable().addListener(new CuratorListener() {
			@Override
			public void eventReceived(CuratorFramework client, CuratorEvent event) throws Exception {
				System.out.println("--- 客户端 client2 状态 ---");
				System.out.println("client2客户端 type = " + event.getType());
				System.out.println("client2客户端 name = " + event.getName());
				System.out.println("client2客户端 path = " + event.getPath());
				System.out.println("client2客户端 data = " + event.getData());
				System.out.println("client2客户端 resultCode = " + event.getResultCode());
				System.out.println("client2客户端 ACL = " + event.getACLList());
				System.out.println("client2客户端 children = " + event.getChildren());
				System.out.println("client2客户端 context = " + event.getContext());
				System.out.println("client2客户端 Stat = " + event.getStat());
			}
			
		}, exec);
		
		// --- 创建会话 ---
		try {
			
			// 5秒后创建客户端连接。
			TimeUnit.SECONDS.sleep(5);
			
			client1.start();
			client2.start();
			
			// 5秒后关闭客户端。
			TimeUnit.SECONDS.sleep(5);
			
			client1.close();
			client2.close();
			
			// 关闭监听线程池。
			exec.shutdown();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		
	}

	/**
	 * 创建 ExponentialBackoffRetry 重试策略。
	 * 
	 * @return RetryPolicy
	 */
	private static RetryPolicy createExponentialBackoffRetry() {
		/*
		 * 重试策略。默认主要有四种实现分别是：
		 * 1、ExponentialBackoffRetry；
		 *   构造函数：
		 *   1.1、ExponentialBackoffRetry(int baseSleepTimeMs, int maxRetries);
		 *   1.2、ExponentialBackoffRetry(int baseSleepTimeMs, int maxRetries, int maxSleepMs);
		 *   参数解释：
		 *   1.1、baseSleepTimeMs 初始 sleep 时间；
		 *   1.2、maxRetries 最大重试次数；
		 *   1.3、maxSleepMs 最大 sleep 时间。
		 *   
		 *   ExponentialBackoffRetry 的重试策略设计如下：
		 *   当前 sleep 时间 = baseSleepTimeMs * Math.max(1, random.nextInt(1 << (retryCount + 1)))
		 *   由于 1 << (retryCount + 1) 是按几何倍数增长（例如，1 << 1 = 2；1 << 2 = 4；1 << 3 = 8；1 << 4 = 16；1 << 5 = 32）
		 *   所以，随着重试次数的增加，计算出的 sleep 时间会越来越大。如果该 sleep 在 maxSleepMs 的范围之内，那么就使用该 sleep 时间，否则就使用 maxSleepMs。
		 *   另外，maxRetries 参数控制了最大重试次数，以避免无限制的重试。
		 *   
		 * 2、RetryNTimes；
		 * 3、RetryOneTime；
		 * 4、RetryUntilElapsed。
		 */
		RetryPolicy retryPolicy = new ExponentialBackoffRetry(1000, 3);
		
		return retryPolicy;
	}
	
	/**
	 * 使用 CuratorFrameworkFactory 创建 CuratorFramework 客户端对象。
	 * 
	 * @return CuratorFramework
	 */
	private static CuratorFramework createCuratorFramework() {
		/*
		 * 可使用 CuratorFrameworkFactory 这个工厂类的两个静态方法来创建一个客户端：
		 * 构造函数：
		 * 1、static CuratorFramework newClient(String connectString, RetryPolicy retryPolicy);
		 * 2、static CuratorFramework newClient(String connectString, int sessionTimeoutMs, int connectionTimeoutMs, RetryPolicy retryPolicy);
		 * 参数解释：
		 * 1、connectString 设置 ZooKeeper 服务器列表。由英文状态逗号分隔的  “host:port,host:port,host:port” 字符串组成。
		 * 2、sessionTimeoutMs 会话超时，单位为毫秒。默认是 60 000ms；
		 * 3、connectionTimeoutMs 连接超时，单位为毫秒。默认是 15 000毫秒；
		 * 4、retryPolicy 重试策略；
		 * 
		 * 当使用 CuratorFrameworkFactory 创建出一个客户端 CuratorFramework 实例后，实质上并没有完成会话的创建，必须要调用 CuratorFramework#start() 方法来完成会话的创建。
		 */
		CuratorFramework client = CuratorFrameworkFactory.newClient("192.168.56.101:2181", 5000, 3000, createExponentialBackoffRetry());
		
		return client;
	}
	
	/**
	 * 使用 CuratorFrameworkFactory 根据 Fluent 风格来创建 CuratorFramework 客户端对象。
	 * 
	 * @return CuratorFramework
	 */
	private static CuratorFramework createCuratorFrameworkWithFluentStyle() {
		CuratorFramework client = CuratorFrameworkFactory.builder()
		.connectString("192.168.56.101:2181,192.168.56.101:2182,192.168.56.101:2183")
		.sessionTimeoutMs(5000)
		.connectionTimeoutMs(3000)
		.retryPolicy(createExponentialBackoffRetry())
		.build();
		
		return client;
	}
}