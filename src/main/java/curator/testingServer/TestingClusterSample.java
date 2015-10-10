package curator.testingServer;

import java.io.File;

import org.apache.curator.test.InstanceSpec;
import org.apache.curator.test.TestingCluster;
import org.apache.curator.test.TestingZooKeeperServer;

public class TestingClusterSample {
	
	private static String projectPath = new File("").getAbsolutePath();
	
	/*
	 * TestingCluster 是一个可以模拟 Zookeeper 集群环境的 Curator 工具类，能够便于开发人员在本地模拟由 n 台机器组成的集群环境。
	 * 
	 * 在下面的程序中，模拟了一个由 3 个 ZooKeeper 实例组成的集群，同时在运行期间，故意将 Leader 服务器 Kill 掉。此时其它两台机器将重新进行Leader 选举。
	 * 
	 * 在下面的程序中使用了 InstanceSpec 类描述 ZooKeeper 实例，还可以简单的使用如下方式产生 ZooKeeper 实例：
	 * TestingCluster cluster = new TestingCluster(3);
	 * 
	 * 在这种方式中，由于没有指定数据目录，那 Curator 会默认在以下路径中防止数据：
	 * C:\Users\admin\AppData\Local\Temp\1444462691004-0\version-2\snapshot.0
	 * 
	 * 经过测试这个方式工作极其不稳定，总抛出 javax.management.InstanceAlreadyExistsException: log4j:hiearchy=default 异常。
	 */
	
	public static void main(String[] args) {
		
		InstanceSpec zk1 = new InstanceSpec(new File(projectPath + "/zookeeper-data/cluster/2888/"), 2888, 3888, 5888, false, 1);
		InstanceSpec zk2 = new InstanceSpec(new File(projectPath + "/zookeeper-data/cluster/2889/"), 2889, 3889, 5889, false, 2);
		InstanceSpec zk3 = new InstanceSpec(new File(projectPath + "/zookeeper-data/cluster/2890/"), 2890, 3890, 5890, false, 3);
		
		TestingCluster cluster = new TestingCluster(zk1, zk2, zk3);
		
		try {
			cluster.start();
			
			Thread.sleep(5000);
			
			TestingZooKeeperServer leader = null;
			for (TestingZooKeeperServer zs : cluster.getServers()) {
				System.out.println(zs.getInstanceSpec().getServerId() + "-");
				System.out.println(zs.getQuorumPeer().getServerState() + "-");
				System.out.println(zs.getInstanceSpec().getDataDirectory().getAbsolutePath());
				
				if (zs.getQuorumPeer().getServerState().equals("leading")) {
					leader = zs;
				}
			}
			
			leader.kill();
			
			System.out.println("--After leader kill:");
			
			for (TestingZooKeeperServer zs : cluster.getServers()) {
				System.out.println(zs.getInstanceSpec().getServerId() + "-");
				System.out.println(zs.getQuorumPeer().getServerState() + "-");
				System.out.println(zs.getInstanceSpec().getDataDirectory().getAbsolutePath());
			}
			
			cluster.stop();
			
		} catch (Exception e) {
			e.printStackTrace();
		}
		
	}
}