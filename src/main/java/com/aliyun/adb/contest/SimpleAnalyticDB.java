package com.aliyun.adb.contest;

import com.aliyun.adb.contest.spi.AnalyticDB;
import sun.misc.Unsafe;
import sun.nio.ch.DirectBuffer;

import java.io.*;
import java.lang.reflect.Field;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Arrays;
import java.util.concurrent.CountDownLatch;

public class SimpleAnalyticDB implements AnalyticDB {

    private static final int BOUNDARYSIZE = 130;
    private static final int THREADNUM = 4;
    private static final int DATALENGTH = 10000;
    private static final int BYTEBUFFERSIZE = 1024 * 64;
    private static final int EACHREADSIZE = 1024;
    private String curTableName;
    private Unsafe unsafe = null;
    private final int[][] blockSize = new int[2][BOUNDARYSIZE];
    private final int[][] beginOrder = new int[2][BOUNDARYSIZE];

    private static final CountDownLatch latch = new CountDownLatch(THREADNUM);
    private  String workDir;
    
    public SimpleAnalyticDB() {
    }

    @Override
    public void load(String tpchDataFileDir, String workspaceDir) throws Exception {
        workDir = workspaceDir;
        Field f = Unsafe.class.getDeclaredField("theUnsafe");
        f.setAccessible(true);
        unsafe = (Unsafe)f.get(null);
        File dir = new File(tpchDataFileDir);
        for (File dataFile : dir.listFiles()) {
            System.out.println("Start loading table " + dataFile.getName());
            curTableName = dataFile.getName();
            loadStore(dataFile);
        }
    }

    @Override
    public String quantile(String table, String column, double percentile) throws Exception {
        String ans;
        int rank = (int) Math.round(DATALENGTH * percentile);
        int index;
        int flag = column.equals("L_ORDERKEY") ? 0 : 1;
        int retIndex = Arrays.binarySearch(beginOrder[flag],rank);
        if (retIndex >= 0) {
            index = retIndex;
        }
        else {
            index = - retIndex - 2;
        }
        int rankDiff;
        rankDiff = rank - beginOrder[flag][index]+ 1;
        long[] data = new long[blockSize[flag][index]];
        int pos = 0;
        for (int i = 0; i < THREADNUM; i++){
            StringBuilder builder = new StringBuilder(workDir);
            String fileName = builder.append("/").append(table).append("-").append(column).append(i).append("-").append(index).toString();
            FileInputStream inFile = new FileInputStream(fileName);
            FileChannel channel = inFile.getChannel();
            long size = channel.size();
            MappedByteBuffer mappedByteBuffer = channel.map(FileChannel.MapMode.READ_ONLY, 0, size);
            mappedByteBuffer.order(ByteOrder.LITTLE_ENDIAN);
            for(int j = 0; j < size; j +=8 ) {
                data[pos++] = mappedByteBuffer.getLong(j);
            }
        }
        ans = MyFind.quickFind(data, 0, pos - 1, rankDiff).toString();
//        System.out.println("Query:" + table + ", " + column + ", " + percentile + " Answer:" + rank + ", " + ans);
        return ans;
        //return "0";
    }

    private void loadStore(File dataFile) throws Exception {
        long sss = System.currentTimeMillis();
        RandomAccessFile fis = new RandomAccessFile(dataFile, "r");
        FileChannel channel = fis.getChannel();
        long size = channel.size();
        byte[] st = new byte[21];
//        String line = new String(st, 0, 20);
//        System.out.println(line);
        for (int i = 0; i < BOUNDARYSIZE; i++){
            beginOrder[0][i] = 0;
            beginOrder[1][i] = 0;
        }
        long hasReadByte = 21;
        long sizePerBuffer = size / THREADNUM;
        byte[] bytes = new byte[42];
        ByteBuffer byteBuffer = ByteBuffer.wrap(bytes);
        for(int i = 0; i < THREADNUM; i++) {
            if(i == THREADNUM - 1) {
                new Thread(new ThreadTask(i,hasReadByte,(size - hasReadByte), channel)).start();
                break;
            }
            byteBuffer.clear();
            channel.read(byteBuffer, hasReadByte + sizePerBuffer - 42);
            int needMove = 0;
            for(int j = 41; j >= 0; j--) {
                if(bytes[j] != 10) {
                    needMove++;
                } else {
                    break;
                }
            }
            new Thread(new ThreadTask(i, hasReadByte,(sizePerBuffer-needMove), channel)).start();
            hasReadByte += (sizePerBuffer-needMove);
        }

        latch.await();
        long end = System.currentTimeMillis();
        System.out.println(end - sss);

        //flush
        int  lBry = 0, rBry = 0;
        for (int i = 0; i < BOUNDARYSIZE; i++){
            beginOrder[0][i] = lBry + 1;
            lBry += blockSize[0][i];
            beginOrder[1][i] = rBry + 1;
            rBry += blockSize[1][i];
        }
        System.out.println("" + ( beginOrder[0][BOUNDARYSIZE - 1] - 1 )  + " " + ( beginOrder[1][BOUNDARYSIZE - 1] - 1) );
    }


    class ThreadTask implements Runnable {
        long readStart;
        long trueSizeOfMmap;
        int threadNo;
        long directBufferBase;
        FileChannel fileChannel;
        ByteBuffer directBuffer;
        FileOutputStream[] leftChannel;
        FileOutputStream[] rightChannel;
        ByteBuffer[] leftBufs;
        ByteBuffer[] rightBufs;

        //初始化
        public ThreadTask(int threadNo, long readStart ,long trueSizeOfMmap, FileChannel fileChannel) throws Exception {
            this.threadNo = threadNo;
            this.readStart = readStart;
            this.trueSizeOfMmap = trueSizeOfMmap;
            this.fileChannel = fileChannel;
            this.directBuffer = ByteBuffer.allocateDirect(EACHREADSIZE);
            this.leftChannel = new FileOutputStream[BOUNDARYSIZE];
            this.rightChannel = new FileOutputStream[BOUNDARYSIZE];
            this.leftBufs = new ByteBuffer[BOUNDARYSIZE];
            this.rightBufs = new ByteBuffer[BOUNDARYSIZE];
            this.directBufferBase = ((DirectBuffer)directBuffer).address();
        }

        @Override
        public void run() {
            int leftReadNum = 0, rightReadNum = 0;
            try{
                String outLDir, outRDir;
                File LoutFile, RoutFile;
                RandomAccessFile Lrw, Rrw;
                StringBuilder builder;
                for (int i = 0; i < BOUNDARYSIZE; i++) {
                    builder = new StringBuilder(workDir);
                    outLDir = builder.append("/").append(curTableName).append("-").append("L_ORDERKEY").append(threadNo).append("-").append(i).toString();
                    builder = new StringBuilder(workDir);
                    outRDir = builder.append("/").append(curTableName).append("-").append("L_PARTKEY").append(threadNo).append("-").append(i).toString();
                    LoutFile = new File(outLDir);
                    RoutFile = new File(outRDir);
                    Lrw = new RandomAccessFile(LoutFile, "rw");
                    Rrw = new RandomAccessFile(RoutFile, "rw");
                    Lrw.setLength(8*600000); //length need change
                    Rrw.setLength(8*600000);
                    leftChannel[i] = new FileOutputStream(LoutFile);
                    rightChannel[i] = new FileOutputStream(RoutFile);
                    leftBufs[i] = ByteBuffer.allocate(BYTEBUFFERSIZE);
                    leftBufs[i].order(ByteOrder.LITTLE_ENDIAN);
                    rightBufs[i] = ByteBuffer.allocate(BYTEBUFFERSIZE);
                    rightBufs[i].order(ByteOrder.LITTLE_ENDIAN);
                }
                long nowRead = 0, realRead, yuzhi = trueSizeOfMmap - EACHREADSIZE;

                while(nowRead < yuzhi) {
                    realRead = EACHREADSIZE;
                    directBuffer.clear();
                    fileChannel.read(directBuffer, readStart + nowRead);
                    for(int i = (int)realRead-1; i >= 0; i--) {
                        if(unsafe.getByte(directBufferBase + i) != 10) {
                            realRead--;
                        } else {
                            break;
                        }
                    }
                    nowRead += realRead;
                    long val = 0;
                    int position;
                    byte t;
                    for(int index = 0; index < realRead; index++) {
                        t = unsafe.getByte(directBufferBase + index);
                        if((t & 16) == 0) {
                            if(t == 44) {
                                leftReadNum++;
                                int leftIndex = (int)(val >> 56);
                                leftBufs[leftIndex].putLong(val);
                                position = leftBufs[leftIndex].position();
                                if (position >= BYTEBUFFERSIZE) {
                                    leftChannel[leftIndex].write(leftBufs[leftIndex].array(), 0, position);
                                    leftBufs[leftIndex].clear();
                                }
                                val = 0;
                            }else {
                                rightReadNum++;
                                int rightIndex = (int)(val >> 56);
                                rightBufs[rightIndex].putLong(val);
                                position = rightBufs[rightIndex].position();
                                if (position >= BYTEBUFFERSIZE) {
                                    rightChannel[rightIndex].write(rightBufs[rightIndex].array(), 0, position);
                                    rightBufs[rightIndex].clear();
                                }
                                val = 0;
                            }
                        }
                        else {
                            val = val * 10 + (t - 48);
                        }
                    }
                }
                realRead = trueSizeOfMmap - nowRead;
                directBuffer.clear();
                fileChannel.read(directBuffer, readStart + nowRead);
                nowRead += realRead;
                long val = 0;
                int position;
                byte t;
                for(int index = 0; index < realRead; index++) {
                    t = unsafe.getByte(directBufferBase + index);
                    if((t & 16) == 0) {
                        if(t == 44) {
                            leftReadNum++;
                            int leftIndex = (int)(val >> 56);
                            leftBufs[leftIndex].putLong(val);
                            position = leftBufs[leftIndex].position();
                            if (position >= BYTEBUFFERSIZE) {
                                leftChannel[leftIndex].write(leftBufs[leftIndex].array(), 0, position);
                                leftBufs[leftIndex].clear();
                            }
                            val = 0;
                        }else {
                            rightReadNum++;
                            int rightIndex = (int)(val >> 56);
                            rightBufs[rightIndex].putLong(val);
                            position = rightBufs[rightIndex].position();
                            if (position >= BYTEBUFFERSIZE) {
                                rightChannel[rightIndex].write(rightBufs[rightIndex].array(), 0, position);
                                rightBufs[rightIndex].clear();
                            }
                            val = 0;
                        }
                    }
                    else {
                        val = val * 10 + (t - 48);
                    }
                }

                for(int i = 0; i < BOUNDARYSIZE; i++) {
                    leftChannel[i].write(leftBufs[i].array(),0 ,leftBufs[i].position());
                    rightChannel[i].write(rightBufs[i].array(),0 ,rightBufs[i].position());
                    synchronized (blockSize)
                    {
                        blockSize[0][i] += leftChannel[i].getChannel().size() >> 3;
                        blockSize[1][i] += rightChannel[i].getChannel().size() >> 3;
                    }


                }
            }catch (Exception e){
                e.printStackTrace();
            }
            System.out.println("thread " + threadNo + " " + leftReadNum + " " + rightReadNum);
            latch.countDown();
        }

    }
}