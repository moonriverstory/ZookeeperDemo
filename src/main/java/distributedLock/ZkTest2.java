package distributedLock;

import distributedLock.ConcurrentTestTool.ConcurrentTask;
import distributedLock.lock.DistributedLock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class ZkTest2 {

    private static final Logger LOGGER = LoggerFactory.getLogger(ZkTest2.class);

    private static final String ZK1_CONFIG = "106.14.5.254:2191";

    private static final String ZK2_CONFIG = "106.14.5.254:2192";

    private static final String ZK3_CONFIG = "106.14.5.254:2193";

    public static void main(String[] args) {
        Runnable task1 = new Runnable() {
            @Override
            public void run() {
                DistributedLock lock = null;
                try {
                    lock = new DistributedLock(ZK1_CONFIG, "test1");
                    lock.lock();
                    Thread.sleep(1500);
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

        //主线程等待1s
        try {
            Thread.sleep(1000);
        } catch (InterruptedException e1) {
            LOGGER.error("main sleep error: ", e1);
        }

        Runnable task2 = new Runnable() {
            @Override
            public void run() {
                ConcurrentTask[] tasks = new ConcurrentTask[10];
                for (int i = 0; i < tasks.length; i++) {
                    ConcurrentTask taskI = new ConcurrentTask() {

                        @Override
                        public void run() {
                            DistributedLock lock = null;
                            try {
                                lock = new DistributedLock(ZK2_CONFIG, "test2");
                                lock.lock();
                                Thread.sleep(100);
                                LOGGER.info("task2: " + Thread.currentThread().getName() + " ,Thread ID: " + Thread.currentThread().getId() + " running");
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
        };
        new Thread(task2).start();

        Runnable task3 = new Runnable() {
            @Override
            public void run() {
                ConcurrentTask[] tasks = new ConcurrentTask[10];
                for (int i = 0; i < tasks.length; i++) {
                    ConcurrentTask taskI = new ConcurrentTask() {

                        @Override
                        public void run() {
                            DistributedLock lock = null;
                            try {
                                lock = new DistributedLock(ZK3_CONFIG, "test2");
                                lock.lock();
                                Thread.sleep(50);
                                LOGGER.info("task3: " + Thread.currentThread().getName() + " ,Thread ID: " + Thread.currentThread().getId() + " running");
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
        };
        new Thread(task3).start();


    }
}
