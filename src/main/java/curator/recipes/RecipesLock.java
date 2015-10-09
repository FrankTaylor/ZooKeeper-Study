package curator.recipes;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.concurrent.CountDownLatch;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.locks.InterProcessMutex;
import org.apache.curator.retry.ExponentialBackoffRetry;

public class RecipesLock {
	
	private static String path = "/curator_recipes_lock_path";
	
	private static CuratorFramework client = CuratorFrameworkFactory.builder()
	.connectString("192.168.56.101:2181,192.168.56.101:2182,192.168.56.101:2183")
	.sessionTimeoutMs(5000)
	.connectionTimeoutMs(3000)
	.retryPolicy(new ExponentialBackoffRetry(1000, 3))
	.build();
	
	public static void main(String[] args) {
		client.start();
		
		/*
		 * 下面的程序将借助 Curator 来实现一个简单的分布式锁。其核心接口如下：
		 * public interface InterProcessLock
		 * - public void acquire() throws Exception;
		 * - public void release() throws Exception;
		 * 这两个接口分别用来实现分布式锁的获取与释放过程。
		 * 
		 * 当使用 InterProcessMutex#acquire() 方法时会在 path 下建立一个 “临时、顺序型的节点”。
		 * 经测试发现，当执行线程越多时，该程序的性能越差，所以该程序使用分布式锁的场景可能不对。
		 */
		try {
			final InterProcessMutex lock = new InterProcessMutex(client, path);
			final CountDownLatch down = new CountDownLatch(1);
			
			for (int i = 0; i < 300; i++) {
				new Thread(new Runnable() {
					@Override
					public void run() {
						try {
							down.await();
							lock.acquire();
						} catch (Exception e) {}
						SimpleDateFormat sdf = new SimpleDateFormat("HH:mm:ss | SSS");
						String orderNo = sdf.format(new Date());
						System.out.println("生成的订单号是：" + orderNo);
						
						try {
							lock.release();
						} catch (Exception e) {}
					}
				}, "ThreadName_" + i).start();
			}
			down.countDown();
			
		} catch (Exception e) {
			e.printStackTrace();
		} 
	}

}