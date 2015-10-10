package curator.testingServer;

import java.io.File;
import java.io.IOException;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.curator.test.TestingServer;

public class TestingServerSample {
	
	private static String projectPath = new File("").getAbsolutePath();
	
	public static void main(String[] args) {
		
		/*
		 * 为了便于开发人员进行 ZooKeeper 的开发与测试，Curator 提供了一种启动简易 ZooKeeper 服务的方法 —— TestingServer。该服务允许开发人员非常
		 * 方便地启动一个标准的 ZooKeeper 服务器，并以此来进行一系列的单元测试。
		 * 
		 * TestingServer 允许开发人员自定义 ZooKeeper 服务器对外服务的端口和 dataDir 路径。如果没有指定 dataDir ，那么 Curator 默认会在系统的临
		 * 时目录 java.io.tmpdir 中创建一个临时目录来作为数据存储目录。
		 */
		TestingServer server = null;
		CuratorFramework client = null;
		
		try {
			server = new TestingServer(2181, new File(projectPath + "/zookeeper-data"));
			
			System.out.println("Testing Server connectString: " + server.getConnectString());
			
			client = CuratorFrameworkFactory.builder()
			.connectString(server.getConnectString())
			.sessionTimeoutMs(5000)
			.connectionTimeoutMs(3000)
			.retryPolicy(new ExponentialBackoffRetry(1000, 3))
			.build();
			
			client.start();
			
			if (client.checkExists().forPath("/a") != null) {
				client.delete().forPath("/a");
			}
			
			if (client.checkExists().forPath("/b") != null) {
				client.delete().forPath("/b");
			}
			
			client.create().forPath("/a");
			client.create().forPath("/b");
			
			System.out.println(client.getChildren().forPath("/"));
			
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			if (client != null) {
				client.close();
			}
			
			if (server != null) {
				try {
					server.close();
				} catch (IOException e) {}
			}
		}
		
	}
}