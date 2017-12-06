package distributedLock;

import distributedLock.lock.DistributedLock;
import distributedLock.ConcurrentTestTool.ConcurrentTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ZkTest {

    private static final Logger LOGGER = LoggerFactory.getLogger(ZkTest.class);

    private static final String ZK1_CONFIG = "106.14.5.254:2191";

    private static final String ZK2_CONFIG = "106.14.5.254:2192";

    public static void main(String[] args) {
        Runnable task1 = new Runnable() {

            @Override
            public void run() {
                DistributedLock lock = null;
                try {
                    lock = new DistributedLock(ZK1_CONFIG, "test1");
                    lock.lock();
                    Thread.sleep(3000);
                    LOGGER.info("task1: " + Thread.currentThread().getName() + " ,Thread ID: " + Thread.currentThread().getId() + " running");
                } catch (Exception e) {
                    e.printStackTrace();
                } finally {
                    if (lock != null) {
                        lock.unlock();
                    }
                }
            }
        };

        new Thread(task1).start();

        //主线程等待1s，用来等待zk1和zk2的同步
        try {
            Thread.sleep(1000);
        } catch (InterruptedException e1) {
            LOGGER.error("main sleep error: ", e1);
        }

        ConcurrentTask[] tasks = new ConcurrentTask[10];
        for (int i = 0; i < tasks.length; i++) {
            ConcurrentTask taskI = new ConcurrentTask() {

                @Override
                public void run() {
                    DistributedLock lock = null;
                    try {
                        lock = new DistributedLock(ZK2_CONFIG, "test2");
                        lock.lock();
                        LOGGER.info("taskI: " + Thread.currentThread().getName() + " ,Thread ID: " + Thread.currentThread().getId() + " running");
                    } catch (Exception e) {
                        e.printStackTrace();
                    } finally {
                        lock.unlock();
                    }
                }
            };
            tasks[i] = taskI;
        }

        new ConcurrentTestTool(tasks);
    }
}
