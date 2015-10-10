package curator.recipes;

import java.util.concurrent.TimeUnit;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.barriers.DistributedBarrier;
import org.apache.curator.retry.ExponentialBackoffRetry;

public class RecipesBarrier {
	
	private static String path = "/curator_recipes_barrier_path";
	
	public static void main(String[] args) {
		
		/*
		 * 在下面的程序中，模拟了 5 个线程，通过调用 DistributedBarrier#setBarrier() 方法来完成 Barrier 的设置，
		 * 并通过调用 DistributedBarrier#waitOnBarrier() 方法来等待 Barrier 的释放。然后在主线程中，通过调用
		 * DistributedBarrier#removeBarrier() 方法来释放 Barrier ，同时触发所有等待该 Barrier 的5个线程同
		 * 时进行各自的业务逻辑。
		 */
		try {
			for (int i = 0; i < 5; i++) {
				new Thread(new Runnable() {
					@Override
					public void run() {
						try {
							CuratorFramework client = getClient();
							
							client.start();
							
							DistributedBarrier barrier = new DistributedBarrier(client, path);
							
							System.out.println(Thread.currentThread().getName() + " 号barrier设置");
							
							barrier.setBarrier();
							barrier.waitOnBarrier();
							
							System.out.println("启动...");
						} catch (Exception e) {
							e.printStackTrace();
						}
					}
				}).start();
			}
			
			TimeUnit.SECONDS.sleep(2);
			
			CuratorFramework client = getClient();
			client.start();
			new DistributedBarrier(client, path).removeBarrier();
			
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	private static CuratorFramework getClient() {
		CuratorFramework client = CuratorFrameworkFactory.builder()
		.connectString("192.168.56.101:2181,192.168.56.101:2182,192.168.56.101:2183")
		.sessionTimeoutMs(5000)
		.connectionTimeoutMs(3000)
		.retryPolicy(new ExponentialBackoffRetry(1000, 3))
		.build();
		
		return client;
	}
}