package curator;

import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Id;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

public class ZKCuratorAclDemo {

    private static final Logger LOGGER = LoggerFactory.getLogger(ZKCuratorAclDemo.class);

    private final static String CONNECTION_URL = "192.168.3.110:2181";

    /**
     * 设置参数，获得连接客户端
     */
    public static CuratorFramework getRunningConnect(String servers) {
        RetryPolicy retryPolicy = new ExponentialBackoffRetry(1000, 5);
        CuratorFramework client = CuratorFrameworkFactory.builder()
                .connectString(servers)
                .sessionTimeoutMs(5000)
                .connectionTimeoutMs(5000)
                .retryPolicy(retryPolicy)
                .build();
        //start必须调用，否则报错
        client.start();//建立连接
        return client;
    }

    /**
     * 没有acl的简单demo流程
     */
    public static void demoProcess1() throws Exception {
        CuratorFramework client = ZKCuratorAclDemo.getRunningConnect(CONNECTION_URL);

        //读取 /
        byte[] pathRoot = client.getData().forPath("/");
        System.out.println("path / data: " + new String(pathRoot));
        byte[] pathD = client.getData().forPath("/dubbo");
        System.out.println("path / data: " + new String(pathD));
        Stat stat = new Stat();
        byte[] pathDWithStat = client.getData().storingStatIn(stat).forPath("/dubbo");
        System.out.println("path /dubbo data with stat: " + new String(pathDWithStat));
        System.out.println("path /dubbo stat: " + stat.toString());

        client.close();
    }


    public static void main(String[] args) throws Exception {
        demoProcess1();

    }
}
