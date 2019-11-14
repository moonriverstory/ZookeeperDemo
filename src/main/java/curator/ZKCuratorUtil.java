package curator;

import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.api.ACLProvider;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Id;
import org.apache.zookeeper.data.Stat;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

public class ZKCuratorUtil {

    private final static String CONNECTION_URL = "10.211.95.114:6830";

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
     * 获得acl digest授权的连接
     * @param url
     * @param namespace
     * @param user
     * @param passwd
     * @return
     */
    public static CuratorFramework getDigestConnect(String url, String namespace, String user, String passwd) {
        //使用curator api，密码不需要加密，curator会在调用原生api的时候转为密文，省事=。=
        //创建权限管理器
        ACLProvider aclProvider = new ACLProvider() {
            private List<ACL> acl;

            @Override
            public List<ACL> getDefaultAcl() {
                if (acl == null) {
                    ArrayList<ACL> acl = ZooDefs.Ids.CREATOR_ALL_ACL; //初始化
                    acl.clear();
                    acl.add(new ACL(ZooDefs.Perms.ALL, new Id("auth", user + ":" + passwd)));//添加用户
                    this.acl = acl;
                }
                return acl;
            }

            @Override
            public List<ACL> getAclForPath(String path) {
                return acl;
            }
        };

        CuratorFramework curatorFramework =
                CuratorFrameworkFactory.builder()
                        .aclProvider(aclProvider)
                        .connectString(url)
                        .authorization("digest", (user + ":" + passwd).getBytes()) //设置scheme digest授权
                        .retryPolicy(new ExponentialBackoffRetry(1000, 5))  //重试策略
                        .build();
        curatorFramework.start();
        try {
            curatorFramework.blockUntilConnected(3, TimeUnit.SECONDS); //阻塞判断连接成功，超时认为失败。
            if (curatorFramework.getZookeeperClient().isConnected()) {
                return curatorFramework.usingNamespace(namespace); //返回连接，起始根目录为namespace。
            } else {
                curatorFramework.close();
                throw new RuntimeException("failed to connect to zookeeper service : " + url);
            }
        } catch (Exception e) {
            curatorFramework.close();
            throw new RuntimeException("failed to connect to zookeeper service : " + url + " error: " + e.getMessage());
        }
    }

    /**
     * 没有acl的简单demo流程
     */
    public static void demoProcess1() throws Exception {
        CuratorFramework client = ZKCuratorUtil.getRunningConnect(CONNECTION_URL);

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

        client.close();
    }
    /**
     2019-11-14 17:27:45,367 INFO  [main] [ZooKeeper.java:438] : Initiating client connection, connectString=10.211.95.114:6830 sessionTimeout=5000 watcher=org.apache.curator.ConnectionState@6df97b55
     2019-11-14 17:27:45,481 INFO  [main] [CuratorFrameworkImpl.java:356] : Default schema
     2019-11-14 17:27:45,481 INFO  [main-SendThread(10.211.95.114:6830)] [ClientCnxn.java:975] : Opening socket connection to server 10.211.95.114/10.211.95.114:6830. Will not attempt to authenticate using SASL (unknown error)
     2019-11-14 17:27:45,489 INFO  [main-SendThread(10.211.95.114:6830)] [ClientCnxn.java:852] : Socket connection established to 10.211.95.114/10.211.95.114:6830, initiating session
     2019-11-14 17:27:45,506 INFO  [main-SendThread(10.211.95.114:6830)] [ClientCnxn.java:1235] : Session establishment complete on server 10.211.95.114/10.211.95.114:6830, sessionid = 0x16e43fc56a437bf, negotiated timeout = 8000
     2019-11-14 17:27:45,519 INFO  [main-EventThread] [ConnectionStateManager.java:251] : State change: CONNECTED
     path / data:
     path /dsf/service data:
     ls /: [log_dir_event_notification, payment, test, usf, consumers, latest_producer_id_block, controller_epoch, Netrix, sysconf, isr_change_notification, ebus, admin, zookeeper, sdg, dsf2, dsdp, cluster, config, controller, wechat, dsflocks, soa, dsf, brokers]
     is /dsf/service exist: true
     is /dsf/service/123 exist: false
     is /dsf/service/123 exist: true
     pathCreate: get /dsf/service/123 data : 666
     is /dsf/service/123 exist: false
     2019-11-14 17:27:45,597 INFO  [Curator-Framework-0] [CuratorFrameworkImpl.java:955] : backgroundOperationsLoop exiting
     2019-11-14 17:27:45,610 INFO  [main] [ZooKeeper.java:684] : Session: 0x16e43fc56a437bf closed
     2019-11-14 17:27:45,611 INFO  [main-EventThread] [ClientCnxn.java:512] : EventThread shut down
     */

    /**
     * Acl模拟流程
     * @throws Exception
     */
    public static void demoProcess2() throws Exception {
        CuratorFramework client = ZKCuratorUtil.getDigestConnect(CONNECTION_URL,"","admin","12345");

        //2-------
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

        client.close();
    }

    public static void main(String[] args) throws Exception {
        demoProcess1();
        //demoProcess2();
    }
}
