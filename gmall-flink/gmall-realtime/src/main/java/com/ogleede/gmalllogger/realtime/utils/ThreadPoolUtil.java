package com.ogleede.gmalllogger.realtime.utils;

import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * @author Ogleede
 * @Description
 * @create 2022-06-27-21:02
 */
public class ThreadPoolUtil {
    private volatile static  ThreadPoolExecutor threadPoolExecutor;

    private ThreadPoolUtil() {}

    public static ThreadPoolExecutor getThreadPool() {
        if(threadPoolExecutor == null) {

            synchronized (ThreadPoolUtil.class) {
                if(threadPoolExecutor == null) {
                    threadPoolExecutor = new ThreadPoolExecutor(
                            8,
                            16,
                            60,
                            TimeUnit.SECONDS,
                            new LinkedBlockingQueue<>()
                    );
                }
            }
        }
        return threadPoolExecutor;
    }
}
