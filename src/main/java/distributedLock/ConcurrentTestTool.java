package distributedLock;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 使用CountDownLatch实现的并发测试工具
 */
public class ConcurrentTestTool {

    private static final Logger LOGGER = LoggerFactory.getLogger(ConcurrentTestTool.class);
    
    private CountDownLatch startSignal = new CountDownLatch(1);//开始阀门
    private CountDownLatch doneSignal = null;//结束阀门
    private CopyOnWriteArrayList<Long> list = new CopyOnWriteArrayList<Long>();
    private AtomicInteger err = new AtomicInteger();//原子递增
    private ConcurrentTask[] task = null;

    public ConcurrentTestTool(ConcurrentTask... task) {
        this.task = task;
        if (task == null) {
            LOGGER.info("task can not null");
            System.exit(1);
        }
        doneSignal = new CountDownLatch(task.length);
        start();
    }

    private void start() {
        //创建线程，并将所有线程等待在阀门处
        createThread();
        //打开阀门
        startSignal.countDown();//递减锁存器的计数，如果计数到达零，则释放所有等待的线程
        try {
            doneSignal.await();//等待所有线程都执行完毕
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        //计算执行时间
        getExeTime();
    }

    /**
     * 初始化所有线程，并在阀门处等待
     */
    private void createThread() {
        long len = doneSignal.getCount();
        for (int i = 0; i < len; i++) {
            final int j = i;
            new Thread(new Runnable() {
                @Override
                public void run() {
                    try {
                        startSignal.await();//使当前线程在锁存器倒计数至零之前一直等待
                        long start = System.currentTimeMillis();
                        task[j].run();
                        long end = (System.currentTimeMillis() - start);
                        list.add(end);
                    } catch (Exception e) {
                        err.getAndIncrement();//相当于err++
                    }
                    //运行完毕，计数减1
                    doneSignal.countDown();
                }
            }).start();
        }
    }

    /**
     * 计算平均响应时间
     */
    private void getExeTime() {
        int size = list.size();
        List<Long> _list = new ArrayList<Long>(size);
        _list.addAll(list);
        Collections.sort(_list);
        long min = _list.get(0);
        long max = _list.get(size - 1);
        long sum = 0L;
        for (Long t : _list) {
            sum += t;
        }
        long avg = sum / size;
        LOGGER.info("ExeTime min: " + min);
        LOGGER.info("ExeTime max: " + max);
        LOGGER.info("ExeTime avg: " + avg);
        LOGGER.info("Exe err: " + err.get());
    }

    public interface ConcurrentTask {
        void run();
    }
}


