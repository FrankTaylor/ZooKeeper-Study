package curator;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.api.BackgroundCallback;
import org.apache.curator.framework.api.CuratorEvent;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.zookeeper.CreateMode;

public class CreateNodeBackgroundSample {
	
	private static String path = "/zk-book";
	
	private static CuratorFramework client = CuratorFrameworkFactory.builder()
	.connectString("192.168.56.101:2181,192.168.56.101:2182,192.168.56.101:2183")
	.sessionTimeoutMs(5000)
	.connectionTimeoutMs(3000)
	.retryPolicy(new ExponentialBackoffRetry(1000, 3))
	.build();
	
	private static CountDownLatch semaphore = new CountDownLatch(2);
	private static ExecutorService exec = Executors.newFixedThreadPool(2, new ThreadFactory() {
		@Override
		public Thread newThread(Runnable r) {
			Thread t = new Thread(r);
			t.setName("My Thread");
			return t;
		}
	});
	
	public static void main(String[] args) {
		
		client.start();
		
		System.out.println("Main Thread: " + Thread.currentThread().getName());
		
		/*
		 * Curator 中引入了 BackgroundCallback 接口，用来处理异步接口调用之后服务端返回的结果信息，其接口定义如下。
		 * 
		 * public interface BackgroundCallback {
		 *     public void processResult(CuratorFramework client, CuratorEvent event) throws Exception;
		 * }
		 * 
		 * 在 BackgroundCallback 方法中的 CuratorEvent 参数定义了 ZooKeeper 服务端发送到客户端的一系列事件参数，其中比较重要的有
		 * 事件类型和响应码两个参数。
		 * 
		 * 主要事件类型：
		 * CREATE：代表 CuratorFramework#create()
		 * DELETE：代表 CuratorFramework#delete()
		 * EXISTS：代表 CuratorFramework#checkExists()
		 * GET_DATA：代表 CuratorFramework#getData()
		 * SET_DATA：代表 CuratorFramework#setData()
		 * CHILDREN：代表 CuratorFramework#getChildren()
		 * SYNC：代表 CuratorFramework#sync()
		 * GET_ACL：代表 CuratorFramework#getACL()
		 * WATCHED：代表 Watchable#usingWatcher(Watcher) / Watchable#watched()
		 * CLOSING：代表 客户端与服务器连接断开事件。
		 * 
		 * 响应码（int）：
		 * 响应码用于标识事件的结果状态，所有响应码都被定义在 org.apache.zookeeper.KeeperException.Code 类中，比较常见的响应码有：
		 * 0(OK)、-4(ConnectionLoss)、-110(NodeExists) 和 -112(SessionExpired) 等，分别代表接口调用成功、客户端与服务端
		 * 连接已断开、指定节点已存在和会话已过期等。
		 * 
		 * 可以在 org.apache.curator.framework.api.CuratorEvent 类中对 CuratorEvent 做更深入的了解。
		 * 
		 * Curator 异步 API
		 * Backgroundable<T>
		 * --public T inBackground();
		 * --public T inBackground(Object context);
		 * --public T inBackground(BackgroundCallback callback);
		 * --public T inBackground(BackgroundCallback callback, Object context);
		 * --public T inBackground(BackgroundCallback callback, Executor executor);
		 * --public T inBackground(BackgroundCallback callback, Object context, Executor executor);
		 * 
		 * 在这些 API 接口中，可重点关注下 executor 这个参数。在 ZooKeeper 中，所有异步通知事件处理都是由 EventThread 这个线程来
		 * 处理的 —— EventThread 线程用于串行处理所有的时间通知。而 “串行处理机制” 在大部分应用场景下能够保证对事件处理的顺序性，但这个
		 * 特性也有其弊端，那就是一旦碰上一个复杂的处理单元，就会消耗过长的处理时间，从而影响对其他事件的处理。
		 * 
		 * 因此，在上面 inBackground() 方法中，允许用户传入一个 Executor 实例，这样一来，就可以把那些比较复杂的事件处理放到一个专门的
		 * 线程池中去，如 Executors.newFixedThreadPool(2)。
		 */
		try {
			
			// 1、第一次后台创建节点时，后台线程使用了自定义的线程池。
			client.create()
			.creatingParentsIfNeeded()
			.withMode(CreateMode.EPHEMERAL)
			.inBackground(new BackgroundCallback() {
				@Override
				public void processResult(CuratorFramework client, CuratorEvent event) throws Exception {
					System.out.println("event[code: " + event.getResultCode() + ", type: " + event.getType() + "]");
					System.out.println("Thread of processResult: " + Thread.currentThread().getName());
					
					semaphore.countDown();
				}
				
			}, exec)
			.forPath(path, "init".getBytes());
			
			// 2、第二次后台创建节点时，后台线程使用了默认的线程池。
			client.create()
			.creatingParentsIfNeeded()
			.withMode(CreateMode.EPHEMERAL)
			.inBackground(new BackgroundCallback() {
				@Override
				public void processResult(CuratorFramework client, CuratorEvent event) throws Exception {
					System.out.println("event[code: " + event.getResultCode() + ", type: " + event.getType() + "]");
					System.out.println("Thread of processResult: " + Thread.currentThread().getName());
					
					semaphore.countDown();
				}
				
			})
			.forPath(path, "init".getBytes());
			
			semaphore.await();
			exec.shutdown();
			
			/*
			 * out:
			 * event[code: 0, type: CREATE]
			 * Thread of processResult: My Thread
			 * event[code: -110, type: CREATE]
			 * Thread of processResult: main-EventThread
			 * 
			 * 上面这段程序使用了异步方式 inBackground() 来创建节点，前后两次调用，创建的节点名相同。
			 * 从两次返回的 event 中可以看出，第一次返回的响应码是 0，表明此次调用成功，即创建节点成功；
			 * 而第二次返回的响应码是 -110，表明该节点已经存在，无法重复创建。这些响应码和 ZooKeeper
			 * 原生的响应码是一致的。
			 * 
			 * 在前后两次调用 inBackground() 方法时传入的 Executor 参数。第一次传入了一个 ExecutorService，
			 * 这样一来，Curator 的异步事件处理逻辑就会交由该线程池去做。而第二次调用时，没有传入任何 Executor，因此
			 * 会使用 ZooKeeper 默认的 EventThread 来处理。
			 */
		} catch (Exception e) {
			if (client != null) {
				client.close();
			}
		}
	}
}