package curator.recipes;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.cache.PathChildrenCache;
import org.apache.curator.framework.recipes.cache.PathChildrenCache.StartMode;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheEvent;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheListener;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.data.Stat;

public class PathChildrenCacheSample {
	
	private static String path = "/zk-book";
	
	private static CuratorFramework client = CuratorFrameworkFactory.builder()
	.connectString("192.168.56.101:2181,192.168.56.101:2182,192.168.56.101:2183")
	.sessionTimeoutMs(5000)
	.connectionTimeoutMs(3000)
	.retryPolicy(new ExponentialBackoffRetry(1000, 3))
	.build();
	
	public static void main(String[] args) {
		client.start();
		
		try {
			/*
			 * 先检查 ZooKeeper 上该节点是否存在，如果存在就先删除。
			 */
			Stat stat = client.checkExists().forPath(path);
			if (stat != null) {
				client.delete().forPath(path);
			}
			
			/*
			 * PathChildrenCache 用于监听指定 ZooKeeper 数据节点的子节点变更情况。
			 * 
			 * PathChildrenCache 有如下几个构造方法的定义：
			 * -- public PathChildrenCache(CuratorFramework client, String path, boolean cacheData);
			 * -- public PathChildrenCache(CuratorFramework client, String path, boolean cacheData, ThreadFactory threadFactory);
			 * -- public PathChildrenCache(CuratorFramework client, String path, boolean cacheData, boolean dataIsCompressed, ThreadFactory threadFactory);
			 * -- public PathChildrenCache(CuratorFramework client, String path, boolean cacheData, boolean dataIsCompressed, final ExecutorService executorService);
			 * -- public PathChildrenCache(CuratorFramework client, String path, boolean cacheData, boolean dataIsCompressed, final CloseableExecutorService executorService);
			 * 
			 * PathChildrenCache 构造方法参数说明：
			 * client：Curator 客户端实例。
			 * path：数据节点的节点路径。
			 * dataIsCompressed：是否进行数据压缩。
			 * cacheData：用于配置是否把节点内容缓存起来，如果配置为 true ，那么客户端在接收到节点列表变更的同时，也能够获取到节点的数据内容；如果配置为 false，则无法获取到节点的数据内容。
			 * threadFactory：通过该工厂类构造一个专门的线程池，来处理事件通知。
			 * executorService：利用JDK提供的线程池，来处理事件通知。
			 * 
			 * PathChildrenCache 定义了事件处理的回调接口 PathChildrenCacheListener ，其定义如下：
			 * public interface pathChildrenCacheListener {
			 *     public void childEvent(CuratorFramework client, PathChildrenCacheEvent event) throws Exception;
			 * }
			 * 
			 * 当指定节点的子节点发生变化时，就会回调该方法。PathChildrenCacheEvent 类中定义了所有的事件类型，主要包括：
			 * CHILD_ADDED：新增子节点。
			 * CHILD_UPDATED：变更子节点。
			 * CHILD_REMOVED：删除子节点。
			 * 
			 * 在下面的程序中，对 "/zk-book" 节点进行了子节点变更事件的监听，一旦该节点 “新增/修改/删除”子节点，就会回调 PathChildrenCacheListener ，
			 * 并根据对应的事件类型进行相关的处理。同时，应注意到对节点 “/zk-book” 本身的变更，并没有通知到客户端。
			 * 
			 * 注意，Curator 也无法对二级子节点进行事件监听。也就是说，如果使用 PathChildrenCache 对 “/zk-book” 进行监听，那么当 “/zk-book/c1/c2” 
			 * 节点被 “创建/更新/删除” 的时候，是无法触发子节点变更事件的。
			 */
			PathChildrenCache cache = new PathChildrenCache(client, path, true);
			cache.start(StartMode.POST_INITIALIZED_EVENT);
			cache.getListenable().addListener(new PathChildrenCacheListener() {

				@Override
				public void childEvent(CuratorFramework client, PathChildrenCacheEvent event) throws Exception {
					switch (event.getType()) {
					
					case CHILD_ADDED:
						System.out.println("CHILD_ADDED, " + event.getData().getPath());
						break;
					case CHILD_UPDATED:
						System.out.println("CHILD_UPDATED, " + event.getData().getPath());
						break;
					case CHILD_REMOVED:
						System.out.println("CHILD_REMOVED, " + event.getData().getPath());
						break;
					default:
						break;
						
					}
				}
			});
			client.create().withMode(CreateMode.PERSISTENT).forPath(path);
			Thread.sleep(1000);
			
			client.create().withMode(CreateMode.PERSISTENT).forPath(path + "/c1");
			Thread.sleep(1000);
			
			client.setData().forPath(path + "/c1");
			Thread.sleep(1000);
			
			client.delete().forPath(path + "/c1");
			Thread.sleep(1000);
			
			client.delete().forPath(path);
			Thread.sleep(1000);
			
			cache.close();
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			if (client != null) {
				client.close();
			}
		}
	}
}