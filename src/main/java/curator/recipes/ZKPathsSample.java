package curator.recipes;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.curator.utils.ZKPaths;
import org.apache.curator.utils.ZKPaths.PathAndNode;
import org.apache.zookeeper.ZooKeeper;

public class ZKPathsSample {
	
	private static String path = "/curator_zkpath_sample";
	
	public static void main(String[] args) {
		
		CuratorFramework client = getClient();
		client.start();
		
		try {
			
			System.out.println(ZKPaths.makePath(path, "sub"));                             // 构造节点路径   /curator_zkpath_sample/sub
			System.out.println(ZKPaths.getNodeFromPath(path + "/sub1/sub1-1/sub1-1-1"));   // 得到节点路径中叶子节点名称   sub1-1-1
			System.out.println(ZKPaths.fixForNamespace(path, "/sub2"));                    // 构造节点路径 /curator_zkpath_sample/sub2
			
			// --- 利用 ZKPaths 快速创建节点 ---
			String dir1 = ZKPaths.makePath(path, "child1");
			String dir2 = ZKPaths.fixForNamespace(path, "/child2");
			
			ZooKeeper zookeeper = client.getZookeeperClient().getZooKeeper();
			ZKPaths.mkdirs(zookeeper, dir1);
			ZKPaths.mkdirs(zookeeper, dir2);
			
			// --- 利用 ZKPaths 读出节点中的子节点 ---
			System.out.println(ZKPaths.getSortedChildren(zookeeper, path));                // [child1, child2]
			
			// --- 利用 PathAndNode 对节点路径进行解析 ---
			PathAndNode pn = ZKPaths.getPathAndNode(path + "/sub1/sub2/sub3");
			System.out.println(pn.getPath());                                              // /curator_zkpath_sample/sub1/sub2
			System.out.println(pn.getNode());                                              // sub3
			
			// --- 利用 ZKPaths 来快速删除节点及其子节点 ---
			ZKPaths.deleteChildren(client.getZookeeperClient().getZooKeeper(), path, true);
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			if (client != null) {
				client.close();
			}
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