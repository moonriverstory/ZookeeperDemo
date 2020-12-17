package curator.watcher;

import curator.ZKCuratorAclDemo;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.cache.*;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class CuratorWatcherDemo {
    private static final Logger LOGGER = LoggerFactory.getLogger(CuratorWatcherDemo.class);
    private final static String CONNECTION_URL = "192.168.3.110:2181";

    /**
     * 简单watcher demo，只能触发一次
     * 如果想要多次触发，需要自己实现手动编写反复注册代码
     */
    private static void demo1() {
        CuratorFramework client = ZKCuratorAclDemo.getRunningConnect(CONNECTION_URL);
        // 判断节点是否存在,如果不存在则为空
        Stat statExist = null;
        try {
            statExist = client.checkExists().forPath("/access");
        } catch (Exception e) {
            LOGGER.error("checkExists error: ", e);
        }
        System.out.println(statExist);


        //注册Watcher事件
        try {
            //这段注释了，抛错，没有节点权限，不能添加watcher
            //NoAuthException: KeeperErrorCode = NoAuth for /dsf
            //client.getData().usingWatcher(new MyWatcher()).forPath("/dsf");
        } catch (Exception e) {
            LOGGER.error("usingWatcher MyWatcher error: ", e);
        }

        try {
            //可是，你这个watcher添加有什么用啊，又没有守护线程，都不知道触发watcher没有=。=
            client.getData().usingWatcher(new MyCuratorWatcher()).forPath("/access");
            //watcher事件，zk不保证客户端一定会收到，所以下次在执行，不能显示上一次触发的watcher事件=。=
        } catch (Exception e) {
            LOGGER.error("usingWatcher MyCuratorWatcher error: ", e);
        }
        //create /access/id1 '668'
        //Created /access/id1
        //这个只检测了set data的事件，创建子节点并没有触发事件，没检测到啊 =。=

        try {
            //注册第二个相同path的watcher，用原始watcher interface
            client.getData().usingWatcher(new MyWatcher()).forPath("/access");
            //从结果看，两个watcher都注册成功，并且被触发了
        } catch (Exception e) {
            LOGGER.error("usingWatcher MyWatcher error: ", e);
        }
        //可是这种方式只触发一次，反复操作，并没有重复提示，这不符合curator的初衷啊，说好的反复触发，不需要重复注册的呢？

        //没有把watcher设置到daemon线程上，所以要让watcher触发，则需要sleep当前main thread
        try {
            Thread.sleep(300000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        client.close();
    }
    /**
     * 触发 watcher，节点路径为： /access , eventType: NodeDataChanged , stat: SyncConnected
     * 触发 watcher，节点路径为：/access
     */

    /**
     * nodecache ： 监听某个特定节点，一次注册，反复触发
     */
    private static void demo2() {
        CuratorFramework client = ZKCuratorAclDemo.getRunningConnect(CONNECTION_URL);
        final String nodePath = "/access";
        // 为节点添加 watcher
        // NodeCache: 监听数据节点的变更，会触发事件
        final NodeCache nodeCache = new NodeCache(client, nodePath);

        // buildInitial : 初始化的时候获取 node 的值并且缓存
        try {
            nodeCache.start(true);
        } catch (Exception e) {
            LOGGER.error("nodeCache start error: ", e);
        }
        if (nodeCache.getCurrentData() != null) {
            System.out.println("节点初始化数据为：" + new String(nodeCache.getCurrentData().getData()));
        } else {
            System.out.println("节点初始化数据为空...");
        }

        nodeCache.getListenable().addListener(new NodeCacheListener() {
            @Override
            public void nodeChanged() throws Exception {
                if (nodeCache.getCurrentData() == null) {
                    System.out.println("空");
                    return;
                }
                String data = new String(nodeCache.getCurrentData().getData());
                System.out.println("节点路径：" + nodeCache.getCurrentData().getPath() + "数据：" + data);
            }
        });

        //暂停主线程，不退出
        try {
            Thread.sleep(300000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        client.close();
    }
    /**
     * 2019-11-19 16:45:12,776 INFO  [main-SendThread(106.14.5.254:2181)] [ClientCnxn.java:1235] : Session establishment complete on server 106.14.5.254/106.14.5.254:2181, sessionid = 0x16c18dbb136006d, negotiated timeout = 5000
     * 2019-11-19 16:45:12,783 INFO  [main-EventThread] [ConnectionStateManager.java:251] : State change: CONNECTED
     * 节点初始化数据为：3
     * 节点路径：/access数据：5
     * 节点路径：/access数据：6
     */


    /**
     * PathChildrenCache : 监听某个节点的子节点
     */
    private static void demo3() {
        CuratorFramework client = ZKCuratorAclDemo.getRunningConnect(CONNECTION_URL);
        final String nodePath = "/access";
        // 为子节点添加 watcher
        // PathChildrenCache: 监听数据节点的增删改，会触发事件
        String childNodePathCache = nodePath;
        // cacheData: 设置缓存节点的数据状态
        final PathChildrenCache childrenCache = new PathChildrenCache(client, childNodePathCache, true);
        /**
         * StartMode: 初始化方式
         * POST_INITIALIZED_EVENT：异步初始化，初始化之后会触发事件
         * NORMAL：异步初始化
         * BUILD_INITIAL_CACHE：同步初始化
         */
        try {
            childrenCache.start(PathChildrenCache.StartMode.POST_INITIALIZED_EVENT);
        } catch (Exception e) {
            LOGGER.error("childrenCache start error: ", e);
        }

        List<ChildData> childDataList = childrenCache.getCurrentData();
        System.out.println("当前数据节点的子节点数据列表：");
        for (ChildData cd : childDataList) {
            String childData = new String(cd.getData());
            System.out.println(childData);
        }

        childrenCache.getListenable().addListener(new PathChildrenCacheListener() {
            @Override
            public void childEvent(CuratorFramework client, PathChildrenCacheEvent event) throws Exception {
                if (event.getType().equals(PathChildrenCacheEvent.Type.INITIALIZED)) {
                    System.out.println("子节点初始化 ok...");
                } else if (event.getType().equals(PathChildrenCacheEvent.Type.CHILD_ADDED)) {
                    //String path = event.getData().getPath();
                    System.out.println("添加子节点:" + event.getData().getPath());
                    System.out.println("子节点数据:" + new String(event.getData().getData()));
                } else if (event.getType().equals(PathChildrenCacheEvent.Type.CHILD_REMOVED)) {
                    System.out.println("删除子节点:" + event.getData().getPath());
                } else if (event.getType().equals(PathChildrenCacheEvent.Type.CHILD_UPDATED)) {
                    System.out.println("修改子节点路径:" + event.getData().getPath());
                    System.out.println("修改子节点数据:" + new String(event.getData().getData()));
                }
            }
        });

        //暂停主线程，不退出
        try {
            Thread.sleep(300000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        client.close();
    }
    /**
     第一次运行demo3，还没有监视子节点，之前的创建节点事件居然保存了，我这次才触发的，还是每次运行都有？try~

     2019-11-19 17:32:35,506 INFO  [main-SendThread(106.14.5.254:2181)] [ClientCnxn.java:1235] : Session establishment complete on server 106.14.5.254/106.14.5.254:2181, sessionid = 0x16c18dbb136006e, negotiated timeout = 5000
     当前数据节点的子节点数据列表：
     2019-11-19 17:32:35,573 INFO  [main-EventThread] [ConnectionStateManager.java:251] : State change: CONNECTED
     添加子节点:/access/id2
     子节点数据:668
     添加子节点:/access/id1
     子节点数据:668
     子节点初始化 ok...
     */
    /**
     * 的确在持续监听子节点的创建和修改
     * <p>
     * 添加子节点:/access/id3
     * 子节点数据:666
     * 修改子节点路径:/access/id3
     * 修改子节点数据:667
     */


    private static void demo4() {
        CuratorFramework client = ZKCuratorAclDemo.getRunningConnect(CONNECTION_URL);
        final String nodePath = "/access";
        // 添加 watcher
        final TreeCache treeCache = new TreeCache(client, nodePath);

        try {
            treeCache.start();
        } catch (Exception e) {
            LOGGER.error("treeCache start error: ", e);
        }

        TreeCacheListener listener = (c, event) ->
                System.err.println("event type ：" + event.getType() + " | path ：" + (null != event.getData() ? event.getData().getPath() : null));

        treeCache.getListenable().addListener(listener);

        //暂停主线程，不退出
        try {
            Thread.sleep(300000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        client.close();
    }

    public static void main(String[] args) {
        demo1();
        //demo2();
        //demo3();
        //demo4();
    }
}
