package curator.recipes;

import java.util.concurrent.TimeUnit;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.atomic.AtomicValue;
import org.apache.curator.framework.recipes.atomic.DistributedAtomicInteger;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.curator.retry.RetryNTimes;

public class RecipesDistAtomicInt {
	
	private static String path = "/curator_recipes_distatomicint_path";
	
	private static CuratorFramework client = CuratorFrameworkFactory.builder()
	.connectString("192.168.56.101:2181,192.168.56.101:2182,192.168.56.101:2183")
	.sessionTimeoutMs(5000)
	.connectionTimeoutMs(3000)
	.retryPolicy(new ExponentialBackoffRetry(1000, 3))
	.build();
	
	public static void main(String[] args) {
		client.start();
		
		/*
		 * 指定一个 ZooKeeper 数据节点作为计数器，多个应用实例在分布式锁的控制下，通过更新该数据节点的内容来实现计数功能。
		 */
		try {
			DistributedAtomicInteger atomicInteger = new DistributedAtomicInteger(client, path, new RetryNTimes(3, 1000));
			for (int i = 0; i < 100; i++) {
				AtomicValue<Integer> rc = atomicInteger.add(8);	
				
				if (rc.succeeded()) {				
					System.out.println("preValue: " + rc.preValue() + ", postValue: " + rc.postValue());
				}
				
				TimeUnit.SECONDS.sleep(2);
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}