package curator.recipes;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.barriers.DistributedDoubleBarrier;
import org.apache.curator.retry.ExponentialBackoffRetry;

public class RecipesBarrier2 {
	
	private static String path = "/curator_recipes_barrier_path";
	
	public static void main(String[] args) {
		
		/*
		 * 该示例程序就是一个和 JDK 自带的 CyclicBarrier 非常类似的实现了，它们都指定了进入 Barrier 的成员数阀值（如，DistributedDoubleBarrier 构造器中的 5）。
		 * 每个 Barrier 的参与者都会在调用 DistributedDoubleBarrier#enter() 方法之后进行等待，此时处于准备进入状态。一旦准备进入 Barrier 的成员数达到 5 个后，
		 * 所有的参与者会被同时触发进入。之后调用 DistributedDoubleBarrier#leave() 方法则会再次等待，此时处于准备退出状态。一旦准备退出 Barrier 的成员数达到5个后，
		 * 所有的参与者同样会被同时触发退出。
		 * 
		 * 因此，使用 Curator 的 DistributedDoubleBarrier 能够很好地实现一个分布式 Barrier ，并控制其同时进入和退出。
		 */
		try {
			for (int i = 0; i < 5; i++) {
				new Thread(new Runnable() {
					@Override
					public void run() {
						try {
							CuratorFramework client = getClient();
							
							client.start();
							
							DistributedDoubleBarrier barrier = new DistributedDoubleBarrier(client, path, 5);
							
							Thread.sleep(Math.round(Math.random() * 3000));
							
							System.out.println(Thread.currentThread().getName() + " 号barrier设置");
							
							barrier.enter();
							
							System.out.println("启动...");
							
							Thread.sleep(Math.round(Math.random() * 3000));
							
							barrier.leave();
							
							System.out.println("退出");
						} catch (Exception e) {
							e.printStackTrace();
						}
					}
				}).start();
			}
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