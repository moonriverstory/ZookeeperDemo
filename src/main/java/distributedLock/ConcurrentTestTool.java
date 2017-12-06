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
    /**
     * 开始阀门
     */
    private CountDownLatch startSignal = new CountDownLatch(1);
    /**
     * 结束阀门
     */
    private CountDownLatch doneSignal = null;
    /**
     * 线程执行花费时间list
     */
    private CopyOnWriteArrayList<Long> exeTimeList = new CopyOnWriteArrayList<Long>();
    /**
     * 错误计数，原子递增
     */
    private AtomicInteger err = new AtomicInteger();
    /**
     * 要并行运行的tasks
     */
    private ConcurrentTask[] task = null;

    public ConcurrentTestTool(ConcurrentTask... task) {
        this.task = task;
        if (task == null) {
            LOGGER.error("task can not be null");
            System.exit(1);
        }
        //倒数，初始化结束门阀
        doneSignal = new CountDownLatch(task.length);
        start();
    }

    /**
     * 启动tasks
     */
    private void start() {
        //创建线程，并将所有线程等待在阀门处
        createThread();
        //打开阀门，开始执行所有tasks
        startSignal.countDown();
        try {
            //等待所有线程都执行完毕
            doneSignal.await();
        } catch (InterruptedException e) {
            LOGGER.error("start wait error: ", e);
        }
        //执行完毕，计算并打印执行时间
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
                        //使当前线程在锁存器倒计数至零之前一直等待
                        startSignal.await();
                        long start = System.currentTimeMillis();
                        task[j].run();
                        long end = (System.currentTimeMillis() - start);
                        exeTimeList.add(end);
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
        int size = exeTimeList.size();
        List<Long> _list = new ArrayList<Long>(size);
        _list.addAll(exeTimeList);
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

    //内部接口
    public interface ConcurrentTask {
        //在实现里面加 分布锁
        void run();
    }
}


