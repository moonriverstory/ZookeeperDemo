package curator.distributeLock;

import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.locks.*;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.curator.utils.CloseableUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Collection;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

public class CuratorDistributeLockDemo {

    private static final Logger LOGGER = LoggerFactory.getLogger(CuratorDistributeLockDemo.class);

    // ZooKeeper 锁节点路径, 分布式锁的相关操作都是在这个节点上进行
    private final String lockPath = "/distributed-lock";

    // ZooKeeper 服务地址
    private String connectString;
    // Curator 客户端重试策略
    private RetryPolicy retry;
    // Curator 客户端对象
    private CuratorFramework client;
    // client2 用户模拟其他客户端
    private CuratorFramework client2;

    // 初始化资源
    public void init() throws Exception {
        // 设置 ZooKeeper 服务地址为本机的 2181 端口
        connectString = "106.14.5.254:2191,106.14.5.254:2192,106.14.5.254:2193,106.14.5.254:2194,106.14.5.254:2195";
        // 重试策略
        // 初始休眠时间为 1000ms, 最大重试次数为 3
        retry = new ExponentialBackoffRetry(1000, 3);
        // 创建一个客户端, 60000(ms)为 session 超时时间, 15000(ms)为链接超时时间
        client = CuratorFrameworkFactory.newClient(connectString, 60000, 15000, retry);
        client2 = CuratorFrameworkFactory.newClient(connectString, 60000, 15000, retry);
        // 创建会话
        client.start();
        client2.start();
    }
    /**
     分布式锁服务宕机, ZooKeeper 一般是以集群部署, 如果出现 ZooKeeper 宕机, 那么只要当前正常的服务器超过集群的半数, 依然可以正常提供服务持有锁资源服务器宕机,
     假如一台服务器获取锁之后就宕机了, 那么就会导致其他服务器无法再获取该锁. 就会造成 死锁 问题, 在 Curator 中, 锁的信息都是保存在临时节点上, 如果持有锁资源的服务器宕机,
     那么 ZooKeeper 就会移除它的信息, 这时其他服务器就能进行获取锁操作。

     当然了分布式锁还可以基于redis实现，其string类型的 setnx key value命令 结合expire命令。
     */

    // 释放资源
    public void close() {
        CloseableUtils.closeQuietly(client);
    }

    /**
     InterProcessSemaphoreMutex是一种不可重入的互斥锁，也就意味着即使是同一个线程也无法在持有锁的情况下再次获得锁，所以需要注意，不可重入的锁很容易在一些情况导致死锁。
     */
    public void sharedLockDemo() throws Exception {
        LOGGER.info("=====Demo1 sharedLockDemo start==========");
        // 创建共享锁
        final InterProcessLock lock = new InterProcessSemaphoreMutex(client, lockPath);
        // lock2 用于模拟其他客户端
        final InterProcessLock lock2 = new InterProcessSemaphoreMutex(client2, lockPath);

        new Thread(new Runnable() {
            @Override
            public void run() {
                // 获取锁对象
                try {
                    lock.acquire();
                    LOGGER.info("1获取锁===============");
                    // 测试锁重入
                    Thread.sleep(5 * 1000);
                    lock.release();
                    LOGGER.info("1释放锁===============");
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }).start();

        new Thread(new Runnable() {
            @Override
            public void run() {
                // 获取锁对象
                try {
                    lock2.acquire();
                    LOGGER.info("2获取锁===============");
                    Thread.sleep(5 * 1000);
                    lock2.release();
                    LOGGER.info("2释放锁===============");
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }).start();

        Thread.sleep(20 * 1000);
        LOGGER.info("=====Demo1 sharedLockDemo end==========");
    }

    /**
     原理:

     　　InterProcessMutex通过在zookeeper的某路径节点下创建临时序列节点来实现分布式锁，即每个线程（跨进程的线程）获取同一把锁前，都需要在同样的路径下创建一个节点，
     节点名字由uuid + 递增序列组成。而通过对比自身的序列数是否在所有子节点的第一位，来判断是否成功获取到了锁。当获取锁失败时，它会添加watcher来监听前一个节点的变动情况，
     然后进行等待状态。直到watcher的事件生效将自己唤醒，或者超时时间异常返回。
     */
    public void sharedReentrantLockDemo() throws Exception {
        LOGGER.info("=====Demo2 sharedReentrantLockDemo start==========");
        // 创建共享锁
        final InterProcessLock lock = new InterProcessMutex(client, lockPath);
        // lock2 用于模拟其他客户端
        final InterProcessLock lock2 = new InterProcessMutex(client2, lockPath);

        final CountDownLatch countDownLatch = new CountDownLatch(2);

        new Thread(new Runnable() {
            @Override
            public void run() {
                // 获取锁对象
                try {
                    lock.acquire();
                    LOGGER.info("1获取锁===============");
                    // 测试锁重入
                    lock.acquire();
                    LOGGER.info("1再次获取锁===============");
                    Thread.sleep(5 * 1000);
                    lock.release();
                    LOGGER.info("1释放锁===============");
                    lock.release();
                    LOGGER.info("1再次释放锁===============");

                    countDownLatch.countDown();
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }).start();

        new Thread(new Runnable() {
            @Override
            public void run() {
                // 获取锁对象
                try {
                    lock2.acquire();
                    LOGGER.info("2获取锁===============");
                    // 测试锁重入
                    lock2.acquire();
                    LOGGER.info("2再次获取锁===============");
                    Thread.sleep(5 * 1000);
                    lock2.release();
                    LOGGER.info("2释放锁===============");
                    lock2.release();
                    LOGGER.info("2再次释放锁===============");

                    countDownLatch.countDown();
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }).start();

        countDownLatch.await();
        LOGGER.info("=====Demo2 sharedReentrantLockDemo end==========");
    }

    public void sharedReentrantReadWriteLockDemo() throws Exception {
        LOGGER.info("=====Demo3 sharedReentrantReadWriteLockDemo start==========");
        // 创建共享可重入读写锁
        final InterProcessReadWriteLock locl1 = new InterProcessReadWriteLock(client, lockPath);
        // lock2 用于模拟其他客户端
        final InterProcessReadWriteLock lock2 = new InterProcessReadWriteLock(client2, lockPath);

        // 获取读写锁(使用 InterProcessMutex 实现, 所以是可以重入的)
        final InterProcessLock readLock = locl1.readLock();
        final InterProcessLock readLockw = lock2.readLock();

        final CountDownLatch countDownLatch = new CountDownLatch(2);

        new Thread(new Runnable() {
            @Override
            public void run() {
                // 获取锁对象
                try {
                    readLock.acquire();
                    LOGGER.info("1获取读锁===============");
                    // 测试锁重入
                    readLock.acquire();
                    LOGGER.info("1再次获取读锁===============");
                    Thread.sleep(5 * 1000);
                    readLock.release();
                    LOGGER.info("1释放读锁===============");
                    readLock.release();
                    LOGGER.info("1再次释放读锁===============");

                    countDownLatch.countDown();
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }).start();

        new Thread(new Runnable() {
            @Override
            public void run() {
                // 获取锁对象
                try {
                    Thread.sleep(500);
                    readLockw.acquire();
                    LOGGER.info("2获取读锁===============");
                    // 测试锁重入
                    readLockw.acquire();
                    LOGGER.info("2再次获取读锁==============");
                    Thread.sleep(5 * 1000);
                    readLockw.release();
                    LOGGER.info("2释放读锁===============");
                    readLockw.release();
                    LOGGER.info("2再次释放读锁===============");

                    countDownLatch.countDown();
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }).start();

        countDownLatch.await();
        LOGGER.info("=====Demo3 sharedReentrantReadWriteLockDemo end==========");
    }

    public void semaphoreDemo() throws Exception {
        LOGGER.info("=====Demo4 semaphoreDemo start==========");
        // 创建一个信号量, Curator 以公平锁的方式进行实现
        final InterProcessSemaphoreV2 semaphore = new InterProcessSemaphoreV2(client, lockPath, 3);

        final CountDownLatch countDownLatch = new CountDownLatch(2);

        new Thread(new Runnable() {
            @Override
            public void run() {
                // 获取锁对象
                try {
                    // 获取2个许可
                    Collection<Lease> acquire = semaphore.acquire(2);
                    LOGGER.info("1获取读信号量===============");
                    Thread.sleep(5 * 1000);
                    semaphore.returnAll(acquire);
                    LOGGER.info("1释放读信号量===============");

                    countDownLatch.countDown();
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }).start();

        new Thread(new Runnable() {
            @Override
            public void run() {
                // 获取锁对象
                try {
                    // 获取1个许可
                    Collection<Lease> acquire = semaphore.acquire(1);
                    LOGGER.info("2获取读信号量===============");
                    Thread.sleep(5 * 1000);
                    semaphore.returnAll(acquire);
                    LOGGER.info("2释放读信号量===============");

                    countDownLatch.countDown();
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }).start();

        countDownLatch.await();
        LOGGER.info("=====Demo4 semaphoreDemo end==========");
    }

    public void multiLockDemo() throws Exception {
        LOGGER.info("=====Demo5 multiLockDemo start==========");
        // 可重入锁
        final InterProcessLock interProcessLock1 = new InterProcessMutex(client, lockPath);
        // 不可重入锁
        final InterProcessLock interProcessLock2 = new InterProcessSemaphoreMutex(client2, lockPath);
        // 创建多重锁对象
        final InterProcessLock lock = new InterProcessMultiLock(Arrays.asList(interProcessLock1, interProcessLock2));

        final CountDownLatch countDownLatch = new CountDownLatch(1);

        new Thread(new Runnable() {
            @Override
            public void run() {
                // 获取锁对象
                try {
                    // 获取参数集合中的所有锁
                    lock.acquire();
                    // 因为存在一个不可重入锁, 所以整个 InterProcessMultiLock 不可重入
                    LOGGER.info(String.valueOf(lock.acquire(2, TimeUnit.SECONDS)));
                    // interProcessLock1 是可重入锁, 所以可以继续获取锁
                    LOGGER.info(String.valueOf(interProcessLock1.acquire(2, TimeUnit.SECONDS)));
                    // interProcessLock2 是不可重入锁, 所以获取锁失败
                    LOGGER.info(String.valueOf(interProcessLock2.acquire(2, TimeUnit.SECONDS)));

                    countDownLatch.countDown();
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }).start();

        countDownLatch.await();
        LOGGER.info("=====Demo5 multiLockDemo end==========");
    }

    public static void main(String[] args) {
        CuratorDistributeLockDemo demo = new CuratorDistributeLockDemo();
        try {
            demo.init();
        } catch (Exception e) {
            LOGGER.error("init demo error: ", e);
        }

        try {
            demo.sharedLockDemo();
        } catch (Exception e) {
            LOGGER.error("sharedLockDemo error: ", e);
        }

        try {
            demo.sharedReentrantLockDemo();
        } catch (Exception e) {
            LOGGER.error("sharedReentrantLockDemo error: ", e);
        }

        try {
            demo.sharedReentrantReadWriteLockDemo();
        } catch (Exception e) {
            LOGGER.error("sharedReentrantReadWriteLockDemo error: ", e);
        }

        try {
            demo.semaphoreDemo();
        } catch (Exception e) {
            LOGGER.error("semaphoreDemo error: ", e);
        }

        try {
            demo.multiLockDemo();
        } catch (Exception e) {
            LOGGER.error("multiLockDemo error: ", e);
        }

        demo.close();
    }
    /**
     2019-11-26 10:53:32,987 INFO  [main] [CuratorDistributeLockDemo.java:53] : =====Demo1 sharedLockDemo start==========
     2019-11-26 10:53:32,987 INFO  [main-SendThread(106.14.5.254:2192)] [ClientCnxn.java:975] : Opening socket connection to server 106.14.5.254/106.14.5.254:2192. Will not attempt to authenticate using SASL (unknown error)
     2019-11-26 10:53:33,008 INFO  [main-SendThread(106.14.5.254:2192)] [ClientCnxn.java:1235] : Session establishment complete on server 106.14.5.254/106.14.5.254:2192, sessionid = 0xc6e7dd549ea0001, negotiated timeout = 40000
     2019-11-26 10:53:33,009 INFO  [main-SendThread(106.14.5.254:2192)] [ClientCnxn.java:852] : Socket connection established to 106.14.5.254/106.14.5.254:2192, initiating session
     2019-11-26 10:53:33,016 INFO  [main-EventThread] [ConnectionStateManager.java:251] : State change: CONNECTED
     2019-11-26 10:53:33,029 INFO  [main-SendThread(106.14.5.254:2192)] [ClientCnxn.java:1235] : Session establishment complete on server 106.14.5.254/106.14.5.254:2192, sessionid = 0xc6e7dd549ea0002, negotiated timeout = 40000
     2019-11-26 10:53:33,030 INFO  [main-EventThread] [ConnectionStateManager.java:251] : State change: CONNECTED
     2019-11-26 10:53:33,124 INFO  [Thread-1] [CuratorDistributeLockDemo.java:65] : 1获取锁===============
     2019-11-26 10:53:38,150 INFO  [Thread-1] [CuratorDistributeLockDemo.java:69] : 1释放锁===============
     2019-11-26 10:53:38,188 INFO  [Thread-2] [CuratorDistributeLockDemo.java:82] : 2获取锁===============
     2019-11-26 10:53:43,207 INFO  [Thread-2] [CuratorDistributeLockDemo.java:85] : 2释放锁===============
     2019-11-26 10:53:53,014 INFO  [main] [CuratorDistributeLockDemo.java:93] : =====Demo1 sharedLockDemo end==========
     2019-11-26 10:53:53,014 INFO  [main] [CuratorDistributeLockDemo.java:97] : =====Demo2 sharedReentrantLockDemo start==========
     2019-11-26 10:53:53,044 INFO  [Thread-3] [CuratorDistributeLockDemo.java:111] : 1获取锁===============
     2019-11-26 10:53:53,044 INFO  [Thread-3] [CuratorDistributeLockDemo.java:114] : 1再次获取锁===============
     2019-11-26 10:53:58,045 INFO  [Thread-3] [CuratorDistributeLockDemo.java:117] : 1释放锁===============
     2019-11-26 10:53:58,062 INFO  [Thread-3] [CuratorDistributeLockDemo.java:119] : 1再次释放锁===============
     2019-11-26 10:53:58,078 INFO  [Thread-4] [CuratorDistributeLockDemo.java:134] : 2获取锁===============
     2019-11-26 10:53:58,078 INFO  [Thread-4] [CuratorDistributeLockDemo.java:137] : 2再次获取锁===============
     2019-11-26 10:54:03,079 INFO  [Thread-4] [CuratorDistributeLockDemo.java:140] : 2释放锁===============
     2019-11-26 10:54:03,097 INFO  [Thread-4] [CuratorDistributeLockDemo.java:142] : 2再次释放锁===============
     2019-11-26 10:54:03,098 INFO  [main] [CuratorDistributeLockDemo.java:152] : =====Demo2 sharedReentrantLockDemo end==========
     2019-11-26 10:54:03,098 INFO  [main] [CuratorDistributeLockDemo.java:156] : =====Demo3 sharedReentrantReadWriteLockDemo start==========
     2019-11-26 10:54:03,133 INFO  [Thread-5] [CuratorDistributeLockDemo.java:174] : 1获取读锁===============
     2019-11-26 10:54:03,133 INFO  [Thread-5] [CuratorDistributeLockDemo.java:177] : 1再次获取读锁===============
     2019-11-26 10:54:03,637 INFO  [Thread-6] [CuratorDistributeLockDemo.java:198] : 2获取读锁===============
     2019-11-26 10:54:03,637 INFO  [Thread-6] [CuratorDistributeLockDemo.java:201] : 2再次获取读锁==============
     2019-11-26 10:54:08,134 INFO  [Thread-5] [CuratorDistributeLockDemo.java:180] : 1释放读锁===============
     2019-11-26 10:54:08,154 INFO  [Thread-5] [CuratorDistributeLockDemo.java:182] : 1再次释放读锁===============
     2019-11-26 10:54:08,638 INFO  [Thread-6] [CuratorDistributeLockDemo.java:204] : 2释放读锁===============
     2019-11-26 10:54:08,655 INFO  [Thread-6] [CuratorDistributeLockDemo.java:206] : 2再次释放读锁===============
     2019-11-26 10:54:08,655 INFO  [main] [CuratorDistributeLockDemo.java:216] : =====Demo3 sharedReentrantReadWriteLockDemo end==========
     2019-11-26 10:54:08,655 INFO  [main] [CuratorDistributeLockDemo.java:220] : =====Demo4 semaphoreDemo start==========
     2019-11-26 10:54:08,815 INFO  [Thread-8] [CuratorDistributeLockDemo.java:252] : 2获取读信号量===============
     2019-11-26 10:54:08,870 INFO  [Thread-7] [CuratorDistributeLockDemo.java:233] : 1获取读信号量===============
     2019-11-26 10:54:13,832 INFO  [Thread-8] [CuratorDistributeLockDemo.java:255] : 2释放读信号量===============
     2019-11-26 10:54:13,901 INFO  [Thread-7] [CuratorDistributeLockDemo.java:236] : 1释放读信号量===============
     2019-11-26 10:54:13,901 INFO  [main] [CuratorDistributeLockDemo.java:265] : =====Demo4 semaphoreDemo end==========
     2019-11-26 10:54:13,901 INFO  [main] [CuratorDistributeLockDemo.java:269] : =====Demo5 multiLockDemo start==========
     2019-11-26 10:54:16,064 INFO  [Thread-9] [CuratorDistributeLockDemo.java:287] : false
     2019-11-26 10:54:16,064 INFO  [Thread-9] [CuratorDistributeLockDemo.java:289] : true
     2019-11-26 10:54:18,116 INFO  [Thread-9] [CuratorDistributeLockDemo.java:291] : false
     2019-11-26 10:54:18,117 INFO  [main] [CuratorDistributeLockDemo.java:301] : =====Demo5 multiLockDemo end==========
     */

}
