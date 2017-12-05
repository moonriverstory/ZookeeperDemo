package distributedLock;

import distributedLock.lock.DistributedLock;
import distributedLock.ConcurrentTestTool.ConcurrentTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ZkTest {

    private static final Logger LOGGER = LoggerFactory.getLogger(ZkTest.class);

    public static void main(String[] args) {
        Runnable task1 = new Runnable() {

            @Override
            public void run() {
                DistributedLock lock = null;
                try {
                    lock = new DistributedLock("106.14.5.254:2191", "test1");
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
        try {
            Thread.sleep(1000);
        } catch (InterruptedException e1) {
            e1.printStackTrace();
        }

        ConcurrentTask[] tasks = new ConcurrentTask[10];
        for (int i = 0; i < tasks.length; i++) {
            ConcurrentTask taskI = new ConcurrentTask() {

                @Override
                public void run() {
                    DistributedLock lock = null;
                    try {
                        lock = new DistributedLock("106.14.5.254:2192", "test2");
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
