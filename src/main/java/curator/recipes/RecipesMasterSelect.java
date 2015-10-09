package curator.recipes;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.leader.LeaderSelector;
import org.apache.curator.framework.recipes.leader.LeaderSelectorListenerAdapter;
import org.apache.curator.retry.ExponentialBackoffRetry;

public class RecipesMasterSelect {
	
	public static void main(String[] args) {
		
		ExecutorService exec = Executors.newFixedThreadPool(2);
		exec.execute(new MasterSelectTask("任务1"));
		exec.execute(new MasterSelectTask("任务2"));
		
		exec.shutdown();
		
		try {
			TimeUnit.SECONDS.sleep(120);
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		exec.shutdownNow();
	}

	private static class MasterSelectTask implements Runnable {
		
		private final String taskCode;
		
		public MasterSelectTask(String taskCode) {
			this.taskCode = taskCode;
		}
		
		private String path = "/curator_recipes_master_path";
		
		private CuratorFramework client = CuratorFrameworkFactory.builder()
		.connectString("192.168.56.101:2181,192.168.56.101:2182,192.168.56.101:2183")
		.sessionTimeoutMs(5000)
		.connectionTimeoutMs(3000)
		.retryPolicy(new ExponentialBackoffRetry(1000, 3))
		.build();
		
		@Override
		public void run() {
			client.start();
			
			/*
			 * LeaderSelector 类负责封装所有和 Master 选举相关的逻辑，包括所有和 ZooKeeper 服务器的交互过程。
			 * 其中 path 代表了一个 Master 选举的根节点，表明本次选举都是在该节点下进行的。
			 * 
			 * 在创建 LeaderSelector 实例的时候，还会传入一个监听器 LeaderSelectorListenerAdapter ，
			 * Curator 会在成功获取 Master 权利的时候回调该监听器，其定义如下：
			 * 
			 * public interface LeaderSelectorListener extends ConnectionStateListener {
			 *     public void takeLeadership(CuratorFramework client) throws Exception;
			 * }
			 * 
			 * public abstract class LeaderSelectorListenerAdapter implements LeaderSelectorListener {
			 *     @Override
			 *     public void stateChanged(CuratorFramework client, ConnectionState newState) {
			 *         if ( (newState == ConnectionState.SUSPENDED) || (newState == ConnectionState.LOST) ) {
			 *             throw new CancelLeadershipException();
			 *         }
			 *     }
			 * }
			 * 
			 * LeaderSelectorListener 接口中最主要的方法就是 takeLeadership，Curator 会在竞争到 Master 后自动调用该方法，
			 * 开发者可以在这个方法中实现自己的业务逻辑。
			 * 
			 * 需要注意的是，一旦执行完 takeLeadership 方法，Curator 就会立即释放 Master 权利，然后重新开始新一轮的 Master 选举。
			 * 
			 * 当你在 ZooKeeper 上执行 “ls /curator_recipes_master_path” 命令时，会发现像如下的子节点不断的创建出来：
			 * _c_50b68f2b-1c0c-56f9-ac0c-bf2f3c6d8879-lock-0000000080
			 * _c_980kof2b-678b-ko89-lkoc-kdherc98d32h-lock-0000000081
			 * 该子节点是 “临时、顺序型子节点”，其后缀是一个数字且不断增加。
			 */
			try {
				LeaderSelector selector = new LeaderSelector(client, path, new LeaderSelectorListenerAdapter() {

					@Override
					public void takeLeadership(CuratorFramework client) throws Exception {
						System.out.println("taskCode = " + taskCode + "，成为 Master 角色");
						TimeUnit.SECONDS.sleep(2);
						System.out.println("taskCode = " + taskCode + "，完成 Master 操作，释放 Master 权利");
					}
					
				});
				selector.autoRequeue();
				selector.start();
				
				TimeUnit.SECONDS.sleep(5);
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}
}