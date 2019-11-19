package curator;

import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.api.ACLProvider;
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

public class ZKCuratorUtil {

    private static final Logger LOGGER = LoggerFactory.getLogger(ZKCuratorUtil.class);

    private final static String CONNECTION_URL = "106.14.5.254:2181";

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
     * 获得单一用户acl digest授权的连接
     *
     * @param url
     * @param namespace
     * @param user
     * @param passwd
     * @return
     */
    public static CuratorFramework getDigestConnect(String url, String namespace, String user, String passwd) {
        //使用curator api，密码不需要加密，curator会在调用原生api的时候转为密文，省事=。=
        //创建权限管理器
        CuratorFramework curatorFramework = CuratorFrameworkFactory.builder()
                .connectString(url)
                .namespace(namespace)
                .authorization("digest", (user + ":" + passwd).getBytes()) //设置scheme digest授权
                .retryPolicy(new ExponentialBackoffRetry(1000, 5))  //重试策略
                .build();
        curatorFramework.start();
        return curatorFramework;
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
     * Acl模拟流程1
     *
     * @throws Exception
     */
    public static void demoProcess2() throws Exception {
        CuratorFramework client = ZKCuratorUtil.getDigestConnect(CONNECTION_URL, "dsf", "admin", "12345");

        //2-------
        //读取 /
        byte[] pathRoot = client.getData().forPath("/");
        System.out.println("path / data: " + new String(pathRoot));
        //读取 /dsf/service
        byte[] pathDsf = client.getData().forPath("/service");
        System.out.println("path /dsf/service data: " + new String(pathDsf));
        //获得 / 的所有子节点
        List<String> lsRoot = client.getChildren().forPath("/");
        System.out.println("ls /: " + lsRoot.toString());

        //检查节点是否存在
        Stat exist = client.checkExists().forPath("/service");
        System.out.println("is /dsf/service exist: " + (exist == null ? "false" : "true"));
        Stat exist123 = client.checkExists().forPath("/service/123");
        System.out.println("is /dsf/service/123 exist: " + (exist123 == null ? "false" : "true"));

        //创建节点:节点创建模式默认为持久化节点，内容默认为空
        client.create().forPath("/service/123");
        Stat exist2 = client.checkExists().forPath("/service/123");
        System.out.println("is /dsf/service/123 exist: " + (exist2 == null ? "false" : "true"));
        //设置数据
        client.setData().forPath("/service/123", "666".getBytes());
        //读取
        byte[] pathCreate = client.getData().forPath("/service/123");
        System.out.println("pathCreate: get /dsf/service/123 data : " + new String(pathCreate));
        //删除节点
        client.delete().forPath("/service/123");
        Stat exist3 = client.checkExists().forPath("/service/123");
        System.out.println("is /dsf/service/123 exist: " + (exist3 == null ? "false" : "true"));

        client.close();
    }
    /**
     2019-11-14 17:46:23,965 INFO  [main] [ZooKeeper.java:438] : Initiating client connection, connectString=10.211.95.114:6830 sessionTimeout=60000 watcher=org.apache.curator.ConnectionState@54a097cc
     2019-11-14 17:46:24,100 INFO  [main] [CuratorFrameworkImpl.java:356] : Default schema
     2019-11-14 17:46:24,173 INFO  [main-SendThread(10.211.95.114:6830)] [ClientCnxn.java:975] : Opening socket connection to server 10.211.95.114/10.211.95.114:6830. Will not attempt to authenticate using SASL (unknown error)
     2019-11-14 17:46:24,184 INFO  [main-SendThread(10.211.95.114:6830)] [ClientCnxn.java:852] : Socket connection established to 10.211.95.114/10.211.95.114:6830, initiating session
     2019-11-14 17:46:24,197 INFO  [main-SendThread(10.211.95.114:6830)] [ClientCnxn.java:1235] : Session establishment complete on server 10.211.95.114/10.211.95.114:6830, sessionid = 0x16e43fc56a437d6, negotiated timeout = 60000
     2019-11-14 17:46:24,203 INFO  [main-EventThread] [ConnectionStateManager.java:251] : State change: CONNECTED
     path / data:
     path /dsf/service data:
     ls /: [service, instance]
     is /dsf/service exist: true
     is /dsf/service/123 exist: false
     is /dsf/service/123 exist: true
     pathCreate: get /dsf/service/123 data : 666
     is /dsf/service/123 exist: false
     2019-11-14 17:46:24,306 INFO  [Curator-Framework-0] [CuratorFrameworkImpl.java:955] : backgroundOperationsLoop exiting
     2019-11-14 17:46:24,316 INFO  [main] [ZooKeeper.java:684] : Session: 0x16e43fc56a437d6 closed
     2019-11-14 17:46:24,316 INFO  [main-EventThread] [ClientCnxn.java:512] : EventThread shut down
     */

    /**
     * 创建acl节点
     */
    public static void demoProcess3() throws Exception {
        CuratorFramework client = ZKCuratorUtil.getDigestConnect(CONNECTION_URL, "dsf", "admin", "12345");

        String nodePath = "/super/testAclNode/testOne";

        // 自定义权限列表
        List<ACL> acls = new ArrayList<ACL>();
        Id user1 = new Id("digest", "user1:123456a");
        Id user2 = new Id("digest", "user2:123456b");
        acls.add(new ACL(ZooDefs.Perms.ALL, user1));
        acls.add(new ACL(ZooDefs.Perms.READ, user2));

        // 创建节点，使用自定义权限列表来设置节点的acl权限
        byte[] nodeData = "child-data".getBytes();
        client.create().creatingParentsIfNeeded().withMode(CreateMode.PERSISTENT).withACL(acls).forPath(nodePath, nodeData);
        //withACL(acls):只有指定的新建节点使用这个acls，其余随之创建的父节点，使用session默认权限。
        //withACL(acls,true):传true，则所有新建节点都是用acls权限


        client.close();
        //看zk上的acl，奇怪不，只有直接创建的那个节点是 acls 的权限，而上层的父目录，是连接的acl
        //发现没有，如果没有设置acl，则节点的acl就是 world:anyone:cdrwa
        //节点的acl不会在session结束后消失
    }
    /**
     [zk: 10.211.95.114:6830(CONNECTED) 7] getAcl /
     'world,'anyone
     : cdrwa
     [zk: 10.211.95.114:6830(CONNECTED) 8] getAcl /dsf
     'world,'anyone
     : cdrwa
     [zk: 10.211.95.114:6830(CONNECTED) 9] getAcl /dsf/super
     'digest,'admin:RIcluWliVzL12y0nV2O1rx6dKLg=
     : cdrwa
     [zk: 10.211.95.114:6830(CONNECTED) 10] getAcl /dsf/super/testAclNode
     'digest,'admin:RIcluWliVzL12y0nV2O1rx6dKLg=
     : cdrwa
     [zk: 10.211.95.114:6830(CONNECTED) 11] getAcl /dsf/super/testAclNode/testOne
     'digest,'user1:123456a
     : cdrwa
     'digest,'user2:123456b
     : r
     'digest,'user2:123456b
     : cd
     */

    /**
     * 读取acl节点，没有权限的情况
     */
    public static void demoProcess4() throws Exception {
        CuratorFramework client = ZKCuratorUtil.getRunningConnect(CONNECTION_URL);
        try {
            //读取 /dsf/super
            byte[] pathAcl1 = client.getData().forPath("/dsf/super");
            System.out.println("path /dsf/super data: " + new String(pathAcl1));
        } catch (Exception e) {
            LOGGER.error("get path /dsf/super data error ", e);
        }
        try {
            //读取 /dsf/super/testAclNode/testOne
            byte[] pathAcl2 = client.getData().forPath("/dsf/super/testAclNode/testOne");
            System.out.println("path /dsf/super/testAclNode/testOne data: " + new String(pathAcl2));
        } catch (Exception e) {
            LOGGER.error("get path /dsf/super/testAclNode/testOne data error ", e);
        }
        //无权限会话，查询acl
        List<ACL> acl = client.getACL().forPath("/dsf/super/testAclNode/testOne");
        System.out.println("path /dsf/super/testAclNode/testOne acl: " + acl);
        //很尴尬，没有授权的会话，也可以查询节点的acl。。。
        //这尼玛还有个屁安全性了，我先读取，再给连接授权，不就能读了
    }

    /**
     * 2019-11-15 16:15:25,662 INFO  [main-SendThread(10.211.95.114:6830)] [ClientCnxn.java:852] : Socket connection established to 10.211.95.114/10.211.95.114:6830, initiating session
     * 2019-11-15 16:15:25,702 INFO  [main-SendThread(10.211.95.114:6830)] [ClientCnxn.java:1235] : Session establishment complete on server 10.211.95.114/10.211.95.114:6830, sessionid = 0x16e43fc56a43ab8, negotiated timeout = 8000
     * 2019-11-15 16:15:25,714 INFO  [main-EventThread] [ConnectionStateManager.java:251] : State change: CONNECTED
     * 2019-11-15 16:15:25,726 ERROR [main] [ZKCuratorUtil.java:261] : get path /dsf/super data error
     * org.apache.zookeeper.KeeperException$NoAuthException: KeeperErrorCode = NoAuth for /dsf/super
     * at org.apache.zookeeper.KeeperException.create(KeeperException.java:113)
     * at org.apache.zookeeper.KeeperException.create(KeeperException.java:51)
     * at org.apache.zookeeper.ZooKeeper.getData(ZooKeeper.java:1155)
     * at org.apache.curator.framework.imps.GetDataBuilderImpl$4.call(GetDataBuilderImpl.java:327)
     * at org.apache.curator.framework.imps.GetDataBuilderImpl$4.call(GetDataBuilderImpl.java:316)
     * at org.apache.curator.connection.StandardConnectionHandlingPolicy.callWithRetry(StandardConnectionHandlingPolicy.java:64)
     * at org.apache.curator.RetryLoop.callWithRetry(RetryLoop.java:100)
     * at org.apache.curator.framework.imps.GetDataBuilderImpl.pathInForeground(GetDataBuilderImpl.java:313)
     * at org.apache.curator.framework.imps.GetDataBuilderImpl.forPath(GetDataBuilderImpl.java:304)
     * at org.apache.curator.framework.imps.GetDataBuilderImpl.forPath(GetDataBuilderImpl.java:35)
     * at curator.ZKCuratorUtil.demoProcess4(ZKCuratorUtil.java:258)
     * at curator.ZKCuratorUtil.main(ZKCuratorUtil.java:291)
     * 2019-11-15 16:15:25,737 ERROR [main] [ZKCuratorUtil.java:268] : get path /dsf/super/testAclNode/testOne data error
     * org.apache.zookeeper.KeeperException$NoAuthException: KeeperErrorCode = NoAuth for /dsf/super/testAclNode/testOne
     * at org.apache.zookeeper.KeeperException.create(KeeperException.java:113)
     * at org.apache.zookeeper.KeeperException.create(KeeperException.java:51)
     * at org.apache.zookeeper.ZooKeeper.getData(ZooKeeper.java:1155)
     * at org.apache.curator.framework.imps.GetDataBuilderImpl$4.call(GetDataBuilderImpl.java:327)
     * at org.apache.curator.framework.imps.GetDataBuilderImpl$4.call(GetDataBuilderImpl.java:316)
     * at org.apache.curator.connection.StandardConnectionHandlingPolicy.callWithRetry(StandardConnectionHandlingPolicy.java:64)
     * at org.apache.curator.RetryLoop.callWithRetry(RetryLoop.java:100)
     * at org.apache.curator.framework.imps.GetDataBuilderImpl.pathInForeground(GetDataBuilderImpl.java:313)
     * at org.apache.curator.framework.imps.GetDataBuilderImpl.forPath(GetDataBuilderImpl.java:304)
     * at org.apache.curator.framework.imps.GetDataBuilderImpl.forPath(GetDataBuilderImpl.java:35)
     * at curator.ZKCuratorUtil.demoProcess4(ZKCuratorUtil.java:265)
     * at curator.ZKCuratorUtil.main(ZKCuratorUtil.java:291)
     * path /dsf/super/testAclNode/testOne acl: [31,s{'digest,'user1:123456a}
     * , 1,s{'digest,'user2:123456b}
     * , 12,s{'digest,'user2:123456b}
     * ]
     */

    public static void demoProcess5() throws Exception {
        CuratorFramework client = ZKCuratorUtil.getDigestConnect(CONNECTION_URL, "", "admin", "12345");
        //好神奇啊，用刚才查出来的用户名和密码建立授权链接，也无效Orz

        //查询acl
        List<ACL> acl = client.getACL().forPath("/dsf/super/testAclNode");
        System.out.println("path /dsf/super/testAclNode/testOne acl: " + acl);

        //使用acl权限读
        //byte[] pathAcl2 = client.getData().withACL(acls).forPath("/dsf/super/testAclNode/testOne");
        //这里不能使用withACL()方法，getData()返回的对象没有withACL()这个方法啊Orz

        try {
            //读取 /dsf/super/testAclNode/testOne
            byte[] pathAcl2 = client.getData().forPath("/dsf/super/testAclNode");
            System.out.println("path /dsf/super/testAclNode/testOne data: " + new String(pathAcl2));
        } catch (Exception e) {
            LOGGER.error("get path /dsf/super/testAclNode/testOne data error ", e);
        }
    }


    public static void main(String[] args) throws Exception {
        //demoProcess1();
        //demoProcess2();
        demoProcess3();
        //demoProcess4();
        //demoProcess5();
    }
}
