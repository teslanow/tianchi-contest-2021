package com.aliyun.adb.contest;

import sun.nio.ch.DirectBuffer;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicInteger;

import static com.aliyun.adb.contest.ConsumerThreadPool.CONSUMERPOOL;
import static com.aliyun.adb.contest.UnsafeUtil.UNSAFE;

public class Partion {
    int index;
    private ByteBuffer buffer;
    private long bufferAddress;
    public long[] hasWritensize; //已经写了的数据量大小
    private long curBlockSize; //对于最后一块当前已经写的量
    private Semaphore semaphore;
    private AtomicInteger leftBlock;
    private FileChannel[] channel;
    public Partion(int index, FileChannel[] channel) throws FileNotFoundException {
        this.index = index;
        this.buffer = ByteBuffer.allocateDirect(Constant.PARTITION_SIZE);
        this.bufferAddress = ((DirectBuffer)buffer).address();
        this.leftBlock = new AtomicInteger(Constant.THREADNUM);
        this.semaphore = new Semaphore(Constant.THREADNUM);
        this.channel = channel;
        this.hasWritensize = new long[Constant.COLNUM_EACHTABLE];
    }
    /*
    按序写加锁写
    * */
    synchronized public void writeSeq(long src, long size, int table)
    {
        UNSAFE.copyMemory(null, src, null, bufferAddress + curBlockSize, size);
        curBlockSize += size;
        if(leftBlock.addAndGet(-1) == 0)
        {
            submitToFlush((int)curBlockSize, table);
        }
    }
    private void submitToFlush(int size, int table)
    {
        CONSUMERPOOL.submit(new Runnable() {
            @Override
            public void run() {
                int curPos = 0;
                int leftSize = size;
                try {
                    while (leftSize > Constant.BYTEBUFFERSIZE)
                    {
                        buffer.position(curPos);
                        buffer.limit(curPos + Constant.BYTEBUFFERSIZE);
                        channel[table].write(buffer);
                        curPos += Constant.BYTEBUFFERSIZE;
                        leftSize -= Constant.BYTEBUFFERSIZE;
                    }
                    buffer.position(curPos);
                    buffer.position(leftSize);
                    channel[table].write(buffer);

                } catch (IOException e) {
                    e.printStackTrace();
                }
                hasWritensize[table] += size;
                curBlockSize = 0;
                leftBlock.set(Constant.THREADNUM);
                semaphore.release(Constant.THREADNUM);
            }
        });
    }
    public void acquire() throws InterruptedException {
        this.semaphore.acquire();
    }
}
