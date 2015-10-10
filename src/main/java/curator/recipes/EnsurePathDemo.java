package curator.recipes;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;

public class EnsurePathDemo {
	
	private static String path = "/zk-book/c1";
	
	private static CuratorFramework client = CuratorFrameworkFactory.builder()
	.connectString("192.168.56.101:2181,192.168.56.101:2182,192.168.56.101:2183")
	.sessionTimeoutMs(5000)
	.connectionTimeoutMs(3000)
	.retryPolicy(new ExponentialBackoffRetry(1000, 3))
	.build();
	
	public static void main(String[] args) {
		
		/*
		 * 上层业务希望对一个数据节点进行一些操作，但是操作之前需要确保该节点存在。基于 ZooKeeper 提供的原始 API 接口，为解决上述场景问题，开发人员需要首先对该
		 * 节点进行一次判断，如果该节点不存在，那就需要先创建该节点。而与此同时，在分布式环境中，在 A 机器试图进行节点创建的过程中，由于并发操作的存在，另一台机器，
		 * 如 B 机器，也在同时创建这个节点，于是 A 机器创建的时候，可能会抛出诸如 “节点已存在” 的异常。因此开发人员还必须对这些异常进行单独的处理，逻辑通常是非常繁
		 * 琐的。
		 * 
		 * EnsurePath 正好可以用来搞定这些烦人的问题，它采取了静默的节点创建方式，其内部实现就是试图创建指定的节点，如果节点已经存在，那么就不进行任何操作，也不
		 * 对外抛出异常，否则正常创建数据节点。
		 * 
		 * 注意：在 2.9.0 版本中 EnsurePath 类已经被废弃了，作者是这么描述的：
		 * 
		 * @deprecated Since 2.9.0 - Prefer CuratorFramework.create().creatingParentContainersIfNeeded() or CuratorFramework.exists().creatingParentContainersIfNeeded()
		 */
	}
}