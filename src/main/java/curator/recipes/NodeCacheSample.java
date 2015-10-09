package curator.recipes;

import java.util.concurrent.TimeUnit;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.cache.ChildData;
import org.apache.curator.framework.recipes.cache.NodeCache;
import org.apache.curator.framework.recipes.cache.NodeCacheListener;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.zookeeper.CreateMode;

public class NodeCacheSample {
	
	private static String path = "/zk-book/nodecache";
	
	private static CuratorFramework client = CuratorFrameworkFactory.builder()
	.connectString("192.168.56.101:2181,192.168.56.101:2182,192.168.56.101:2183")
	.sessionTimeoutMs(5000)
	.connectionTimeoutMs(3000)
	.retryPolicy(new ExponentialBackoffRetry(1000, 3))
	.build();
	
	public static void main(String[] args) {
		client.start();
		
		try {
			client.create()
			.creatingParentsIfNeeded()
			.withMode(CreateMode.EPHEMERAL)
			.forPath(path, "init".getBytes());
			
			TimeUnit.SECONDS.sleep(2);
			
			/*
			 * ZooKeeper 原生支持通过注册 Watcher 来进行事件监听，但是其使用并不是特别方便，需要开发人员自己反复注册 Watcher，比较繁琐。
			 * Curator 引入了 Cache 来实现对 ZooKeeper 服务端事件的监听。Cache 是 Curator 中对事件监听的包装，其对事件的监听其实
			 * 可以近似看作是一个 “本地缓存试图” 和 “远程 ZooKeeper 视图” 的对比过程。同时，Curator 能够自动为开发人员处理反复注册监听，从
			 * 而大大简化了原生 API 开发的繁琐过程。
			 * 
			 * Cache 分为两类监听类型：（1）节点监听；（2）子节点监听。
			 * 
			 * NodeCache 类可用于监听指定 ZooKeeper 数据节点本身的改变，其构造方法有如下两个：
			 * --public NodeCache(CuratorFramework client, String path);
			 * --public NodeCache(CuratorFramework client, String path, boolean dataIsCompressed);
			 * 
			 * NodeCache 构造器参数说明：
			 * client：Curator 客户端实例。
			 * path：数据节点的节点路径。
			 * dataIsCompressed：是否进行数据压缩。
			 * 
			 * 同时，NodeCache 定义了事件处理的回调接口 NodeCacheListener，当数据节点的内容发生变化的时候，就会回调该方法。
			 * public interface NodeCacheListener {
			 *     public void nodeChanged() throws Exception;
			 * }
			 * 
			 * 在下边的代码中，首先构造了一个 NodeCache 实例，然后调用 NodeCache#start(boolean) 方法，该方法有个 boolean 类型的参数，
			 * 默认是 false，如果设置为 true，那么 NodeCache 在第一次启动的时候就会立刻从 ZooKeeper 上读取对应节点的数据内容，并保存在 Cache 中。
			 * 
			 * NodeCache 不仅可以用于监听数据节点的内容变更，也能监听指定节点是否存在。如果原本节点不存在，那么 Cache 就会在节点被创建后触发 NodeCacheListener 。
			 * 同时，如果该数据节点被删除时，那也会触发  NodeCacheListener 。
			 * 
			 * 注意，在调用 NodeCache#start(boolean) 方法时，如果设置为 false ，虽然 NodeCache 不会一启动就从 ZooKeeper 上读取对应节点的数据内容，
			 * 但如果注册了 NodeCacheListener ，就会在启动后触发该 NodeCacheListener 从而读出对应节点上的数据内容。
			 */
			final NodeCache cache = new NodeCache(client, path, false);
			cache.start(false);
			ChildData curChildData = cache.getCurrentData();
			if (curChildData != null) {
				System.out.println("Current Node data: " + new String(curChildData.getData()));				
			}
			
			cache.getListenable().addListener(new NodeCacheListener() {
				@Override
				public void nodeChanged() throws Exception {
					
					System.out.println("--- Listen ---");
					ChildData childData = cache.getCurrentData();
					if (childData != null) {						
						System.out.println("Node data update, new data: " + new String(childData.getData()));
					}
				}
				
			});
			
			TimeUnit.SECONDS.sleep(2);
			
			client.setData().forPath(path, "u".getBytes());
			Thread.sleep(1000);
			client.delete().deletingChildrenIfNeeded().forPath(path);
			Thread.sleep(2000);
			
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