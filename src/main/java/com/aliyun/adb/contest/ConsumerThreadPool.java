package com.aliyun.adb.contest;

import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class ConsumerThreadPool {
    public static final ThreadPoolExecutor CONSUMERPOOL = new ThreadPoolExecutor(Constant.WRITETHREAD, Constant.WRITETHREAD, 1, TimeUnit.SECONDS, new LinkedBlockingQueue<>());
}
