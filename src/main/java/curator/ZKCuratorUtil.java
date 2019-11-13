package curator;

import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.zookeeper.data.Stat;

import java.util.List;

public class ZKCuratorUtil {

    private final String CONNECTION_URL = "10.211.95.114:6830";

    /**
     * 设置参数，获得连接客户端
     */
    public CuratorFramework getClient() {
        RetryPolicy retryPolicy = new ExponentialBackoffRetry(1000, 5);
        CuratorFramework client = CuratorFrameworkFactory.builder()
                .connectString(CONNECTION_URL)
                .sessionTimeoutMs(5000)
                .connectionTimeoutMs(5000)
                .retryPolicy(retryPolicy)
                .build();
        return client;
    }

    public static void main(String[] args) throws Exception {
        ZKCuratorUtil util = new ZKCuratorUtil();
        CuratorFramework client = util.getClient();
        //start必须调用，否则报错
        client.start();//建立连接
        //读取 /
        byte[] pathRoot = client.getData().forPath("/");
        System.out.println("path / data: " + new String(pathRoot));
        //读取 /dsf/service
        byte[] pathDsf = client.getData().forPath("/dsf/service");
        System.out.println("path /dsf/service data: " + new String(pathDsf));
        //获得 / 的所有子节点
        List<String> lsRoot = client.getChildren().forPath("/");
        System.out.println("ls /: " + lsRoot.toString());

        //检查节点是否存在
        Stat exist = client.checkExists().forPath("/dsf/service");
        System.out.println("is /dsf/service exist: " + (exist == null ? "false" : "true"));
        Stat exist123 = client.checkExists().forPath("/dsf/service/123");
        System.out.println("is /dsf/service/123 exist: " + (exist123 == null ? "false" : "true"));

        //创建节点:节点创建模式默认为持久化节点，内容默认为空
        client.create().forPath("/dsf/service/123");
        Stat exist2 = client.checkExists().forPath("/dsf/service/123");
        System.out.println("is /dsf/service/123 exist: " + (exist2 == null ? "false" : "true"));
        //设置数据
        client.setData().forPath("/dsf/service/123", "666".getBytes());
        //读取
        byte[] pathCreate = client.getData().forPath("/dsf/service/123");
        System.out.println("pathCreate: get /dsf/service/123 data : " + new String(pathCreate));
        //删除节点
        client.delete().forPath("/dsf/service/123");
        Stat exist3 = client.checkExists().forPath("/dsf/service/123");
        System.out.println("is /dsf/service/123 exist: " + (exist3 == null ? "false" : "true"));
    }
}
