package distributedLock.lock;

import distributedLock.exception.LockException;
import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;

/**
 * 使用zookeeper和CountDownLatch写的分布式锁
 */
public class DistributedLock implements Lock, Watcher {

    private static final Logger LOGGER = LoggerFactory.getLogger(DistributedLock.class);

    /**
     * zk连接对象
     */
    private ZooKeeper zk;
    /**
     * 根
     */
    private String root = "/locks";
    /**
     * 竞争资源的标志
     */
    private String lockName;
    /**
     * 等待前一个锁
     */
    private String waitPrevNode;
    /**
     * 当前锁
     */
    private String currNode;
    /**
     * 等待前一个锁释放的门闩
     */
    private CountDownLatch waitLatch;
    /**
     * zk session超时时间
     */
    private int sessionTimeout = 30000;
    /**
     * 记录zk lock初始化错误
     */
    private List<Exception> initException = new ArrayList<Exception>();

    /**
     * 创建分布式锁,使用前请确认config配置的zookeeper服务可用
     *
     * @param config
     * @param lockName 竞争资源标志,lockName中不能包含单词lock
     */
    public DistributedLock(String config, String lockName) {
        this.lockName = lockName;
        // 创建一个与服务器的连接
        try {
            zk = new ZooKeeper(config, sessionTimeout, this);
            Stat stat = zk.exists(root, false);
            if (stat == null) {
                // 创建根节点
                zk.create(root, new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            }
        } catch (IOException e) {
            initException.add(e);
        } catch (KeeperException e) {
            initException.add(e);
        } catch (InterruptedException e) {
            initException.add(e);
        }
    }

    /**
     * zookeeper节点的监视器
     *
     * @param event
     */
    @Override
    public void process(WatchedEvent event) {
        if (this.waitLatch != null) {
            //如果前一个node被删除，代表锁释放
            if (event.getType().getIntValue() == Event.EventType.NodeDeleted.getIntValue()) {
                //值为1的门闩减1，取消等待
                this.waitLatch.countDown();
                LOGGER.debug("前一个锁：" + waitPrevNode + "释放，当前锁：" + currNode + "取消挂起");
            }
        }
    }

    /**
     * 加锁
     */
    @Override
    public void lock() {
        if (initException.size() > 0) {
            //zk初始化失败，不能加锁，抛异常
            throw new LockException(initException.get(0));
        }
        try {
            if (this.tryLock()) {
                LOGGER.debug(Thread.currentThread().getName() + " ,Thread ID: " + Thread.currentThread().getId() + " " + currNode + " 获得锁~");
                return;
            } else {
                //这里的waitTime很有意义，就是分布式锁队列等待的最大时间
                //如果超时，锁释放，整个队列余下的未执行完的线程都会继续运行
                //所以，超时问题很严重，这限制了分布式锁的队列长度。
                //大型应用，这里一定要更好的的设置等待队列=。=
                waitForLock(waitPrevNode, sessionTimeout);//挂起线程，等待锁，直到zk session超时
            }

        } catch (KeeperException e) {
            throw new LockException(e);
        } catch (InterruptedException e) {
            throw new LockException(e);
        }
    }

    /**
     * 解锁
     */
    @Override
    public void unlock() {
        try {
            LOGGER.debug("unlock " + currNode);
            zk.delete(currNode, -1);
            currNode = null;
            zk.close();
        } catch (InterruptedException e) {
            throw new LockException(e);
        } catch (KeeperException e) {
            throw new LockException(e);
        }
    }

    /**
     * 尝试加锁(CORE方法)
     *
     * @return
     */
    @Override
    public boolean tryLock() {
        try {
            String splitStr = "_lock_";
            if (lockName.contains(splitStr)) {
                throw new LockException("锁名有误, lockName can not contains 'lock'");
            }
            // 创建临时有序节点
            currNode = zk.create(root + "/" + lockName + splitStr, new byte[0],
                    ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);
            LOGGER.debug(currNode + " 已经创建");
            // 取所有子节点
            List<String> subNodes = zk.getChildren(root, false);
            // 取出所有lockName的锁
            List<String> lockObjects = new ArrayList<String>();
            for (String node : subNodes) {
                //截取结点名的lockName头
                String nodePrefix = node.split(splitStr)[0];
                if (nodePrefix.equals(lockName)) {
                    //是当前锁的watchers，把它加入lock list，作为等待队列
                    lockObjects.add(node);
                }
            }
            //锁node name排序
            Collections.sort(lockObjects);
            LOGGER.debug(Thread.currentThread().getName() + " ,Thread ID: " + Thread.currentThread().getId() + " 的锁是： " + currNode + " ，当前锁是： " + lockObjects.get(0));
            // 若当前节点为最小节点，则获取锁成功
            if (currNode.equals(root + "/" + lockObjects.get(0))) {
                //如果是最小的节点,则表示取得锁
                return true;
            }

            // 若不是最小节点，则找到自己的前一个节点
            //取得当前结点的名字
            String currNodeName = currNode.substring(currNode.lastIndexOf("/") + 1);
            //取得当前结点index小1的前一个结点，作为等待结点
            waitPrevNode = lockObjects.get(Collections.binarySearch(lockObjects, currNodeName) - 1);
        } catch (InterruptedException e) {
            throw new LockException(e);
        } catch (KeeperException e) {
            throw new LockException(e);
        }
        return false;
    }

    /**
     * 等待前一个锁释放
     *
     * @param prevNode
     * @param waitTime
     * @return
     * @throws InterruptedException
     * @throws KeeperException
     */
    private boolean waitForLock(String prevNode, long waitTime) throws InterruptedException, KeeperException {
        //判断比自己小一个数的节点是否存在,如果不存在则无需等待锁,同时注册监听
        Stat stat = zk.exists(root + "/" + prevNode, true);
        if (stat != null) {
            LOGGER.debug(Thread.currentThread().getName() + " ,Thread ID: " + Thread.currentThread().getId() + " waiting for " + root + "/" + prevNode);
            this.waitLatch = new CountDownLatch(1);
            //挂起当前线程，等待一定时间waitTime，或者前一个node锁释放
            this.waitLatch.await(waitTime, TimeUnit.MILLISECONDS);
            //清除阀门
            this.waitLatch = null;
        }
        return true;
    }


    /**
     * 尝试加锁，出错返回false
     *
     * @param timeout
     * @param unit
     * @return
     */
    @Override
    public boolean tryLock(long timeout, TimeUnit unit) {
        try {
            if (this.tryLock()) {
                return true;
            }
            return waitForLock(waitPrevNode, timeout);
        } catch (Exception e) {
            LOGGER.debug("tryLock fail: ", e);
            return false;
        }
    }

    @Override
    public void lockInterruptibly() throws InterruptedException {
        this.lock();
    }

    //没用到
    @Override
    public Condition newCondition() {
        return null;
    }


}