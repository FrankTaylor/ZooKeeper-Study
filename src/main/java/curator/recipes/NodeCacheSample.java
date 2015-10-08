package curator.recipes;

import java.util.concurrent.TimeUnit;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
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
			
			TimeUnit.SECONDS.sleep(5);
			
			final NodeCache cache = new NodeCache(client, path, false);
			cache.start(true);
			cache.getListenable().addListener(new NodeCacheListener() {

				@Override
				public void nodeChanged() throws Exception {
					System.out.println("Node data update, new data: " + new String(cache.getCurrentData().getData()));
				}
				
			});
			
			TimeUnit.SECONDS.sleep(5);
			
			client.setData().forPath(path, "u".getBytes());
			Thread.sleep(1000);
			client.delete().deletingChildrenIfNeeded().forPath(path);
			Thread.sleep(Integer.MAX_VALUE);
			
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			if (client != null) {
				client.close();
			}
		}
		
		
	}
}