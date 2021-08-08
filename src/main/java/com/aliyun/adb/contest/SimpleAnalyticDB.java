package com.aliyun.adb.contest;

import com.aliyun.adb.contest.spi.AnalyticDB;
import sun.misc.Unsafe;
import sun.nio.ch.DirectBuffer;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.util.ArrayDeque;
import java.util.Arrays;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

public class SimpleAnalyticDB implements AnalyticDB {

    //提交需改
    private static final long DATAIN_EACHBLOCKTHREAD = 400000; //每个线程处理的每个块的数据量（字节单位）
    private static final int BOUNDARYSIZE = 1040;
    private static final int QUANTILE_DATA_SIZE = 16000000; //每次查询的data量，基本等于DATALENGTH / BOUNDARYSIZE * 8
    private static final int THREADNUM = 1;
    private static final int WRITETHREAD = 1;
    private static AtomicInteger endFlag = new AtomicInteger();
    private static final int ALLEND = (1 << THREADNUM) - 1;
    private static final long DATALENGTH = 1000000000;
    private static final long FILE_SIZE = 1000000;
    private static final int BYTEBUFFERSIZE = 64;
    private static final int EACHREADSIZE = 16;
    private static final int TABLENUM = 2;
    private static final int COLNUM_EACHTABLE = 2;
    private static final int SHIFTBITNUM = 53;
    private static final int CONCURRENT_QUANTILE_THREADNUM = 8;

    private int current_Quantile_threadNUM = 0;
    private final String[][] colName = new String[TABLENUM][COLNUM_EACHTABLE];
    private final String[] tabName = new String[TABLENUM];
    private final Unsafe unsafe;
    private final int[][][] blockSize = new int[TABLENUM][COLNUM_EACHTABLE][BOUNDARYSIZE];
    private final int[][][] beginOrder = new int[TABLENUM][COLNUM_EACHTABLE][BOUNDARYSIZE];
    private final long[] quantile_load_base = new long[CONCURRENT_QUANTILE_THREADNUM];
    private final ByteBuffer[] quantile_load_buffer = new ByteBuffer[CONCURRENT_QUANTILE_THREADNUM];
    private final long[] arrThreadId = new long[CONCURRENT_QUANTILE_THREADNUM];
    private static final CountDownLatch latch = new CountDownLatch(WRITETHREAD);

    //实验
    private final FileChannel[][] leftChannel = new FileChannel[TABLENUM][BOUNDARYSIZE];
    private final FileChannel[][] rightChannel = new FileChannel[TABLENUM][BOUNDARYSIZE];
    private  String workDir;

    public SimpleAnalyticDB() throws NoSuchFieldException, IllegalAccessException {
        this.unsafe = GetUnsafe.getUnsafe();
        this.quantile_load_buffer[0] = ByteBuffer.allocateDirect(QUANTILE_DATA_SIZE);
        this.quantile_load_base[0] = ((DirectBuffer)quantile_load_buffer[0]).address();
    }

    @Override
    public void load(String tpchDataFileDir, String workspaceDir) throws Exception {
        long ss = System.currentTimeMillis();
        workDir = workspaceDir;
        //判断工作区是否为空
//        if(new File(workspaceDir + "/index").exists())
//        {
//            for(int i = 1; i < CONCURRENT_QUANTILE_THREADNUM; i++)
//            {
//                quantile_load_buffer[i] = ByteBuffer.allocateDirect(QUANTILE_DATA_SIZE);
//                quantile_load_base[i] = ((DirectBuffer)quantile_load_buffer[i]).address();
//            }
//            System.out.println("sencond load");
//            RandomAccessFile file = new RandomAccessFile(new File(workDir + "/index"), "r");
//            FileChannel fileChannel = file.getChannel();
//            byte[] bytes = new byte[(int)fileChannel.size()];
//            ByteBuffer byteBuffer = ByteBuffer.wrap(bytes);
//            byteBuffer.clear();
//            fileChannel.read(byteBuffer);
//            byteBuffer.flip();
//            int curPos = 0;
//            String[] tmpString = new String[TABLENUM * COLNUM_EACHTABLE + TABLENUM];
//            for(int pre = 0, index = 0;;)
//            {
//                if(bytes[curPos] == 10)
//                {
//                    tmpString[index++] = new String(bytes, pre, curPos - pre, "UTF-8");
//                    if(index >= TABLENUM * COLNUM_EACHTABLE + TABLENUM)
//                    {
//                        curPos++;
//                        break;
//                    }
//                    pre = curPos + 1;
//                }
//                curPos++;
//            }
//            int index_name = 0;
//            byteBuffer.position(curPos);
//            for(int i = 0; i < TABLENUM; i++)
//            {
//                tabName[i] = tmpString[index_name++];
//                for(int j = 0; j < COLNUM_EACHTABLE; j++)
//                {
//                    colName[i][j] = tmpString[index_name++];
//                    for( int k = 0; k < BOUNDARYSIZE; k++)
//                    {
//                        beginOrder[i][j][k] = byteBuffer.getInt();
//                    }
//                }
//            }
//            return;
//        }
        File dir = new File(tpchDataFileDir);
        loadStore(dir.listFiles());
        long end = System.currentTimeMillis();
        System.out.println("load time is " + (end - ss));
    }

    @Override
    public String quantile(String table, String column, double percentile) throws Exception {
        String ans;
        long s1 = System.currentTimeMillis();
        int buffer_index = 0;
        if(current_Quantile_threadNUM < CONCURRENT_QUANTILE_THREADNUM)
        {
            synchronized (arrThreadId)
            {
                for(int i = 0; i < CONCURRENT_QUANTILE_THREADNUM; i++)
                {
                    if(arrThreadId[i] == Thread.currentThread().getId())
                        break;
                    else if(arrThreadId[i] == 0)
                    {
                        buffer_index = i;
                        arrThreadId[i] = Thread.currentThread().getId();
                        current_Quantile_threadNUM++;
                        break;
                    }
                }
            }
        }
        else
        {
            for(int i = 0; i < CONCURRENT_QUANTILE_THREADNUM; i++)
            {
                if(arrThreadId[i] == Thread.currentThread().getId())
                {
                    buffer_index = i;
                    break;
                }
            }
        }
        System.out.println("thread " + Thread.currentThread().getId() + " current_threadnum " + current_Quantile_threadNUM + " buffer_id " + buffer_index );
        int rank = (int) Math.round(DATALENGTH * percentile);
        int index;
        int flag_table, flag_colum;
        if(table.equals(tabName[0]))
        {
            flag_table = 0;
        }
        else
        {
            flag_table = 1;
        }
        if(column.equals(colName[flag_table][0]))
        {
            flag_colum = 0;
        }
        else
        {
            flag_colum = 1;
        }
        int[] curBeginOrder = beginOrder[flag_table][flag_colum];
        //System.out.println(Arrays.toString(curBeginOrder));
        int retIndex = Arrays.binarySearch(curBeginOrder,rank);
        if (retIndex >= 0) {
            index = retIndex;
        }
        else {
            index = - retIndex - 2;
        }
        int rankDiff;
        rankDiff = rank - curBeginOrder[index]+ 1;

        ByteBuffer byteBuffer = quantile_load_buffer[buffer_index];
        long byteBufferBase = quantile_load_base[buffer_index];
        StringBuilder builder = new StringBuilder(workDir);
        String fileName = builder.append("/").append(table).append("-").append(column).append("-").append(index).toString();
        FileInputStream inFile = new FileInputStream(fileName);
        FileChannel channel = inFile.getChannel();
        long left_size = channel.size();
        byteBuffer.clear();
        channel.read(byteBuffer);
        inFile.close();
//        System.out.println("BOUNDARAY " + BOUNDARYSIZE);
//        System.out.println(Arrays.toString(curBeginOrder));
//        System.out.println("" + ( byteBuffer.position() >> 3) );
//        System.out.println("real size " + (beginOrder[flag_table][flag_colum][index + 1] - beginOrder[flag_table][flag_colum][index]));
//        System.out.println("get size " + (left_size >> 3));
        ans = MyFind.quickFind(unsafe, byteBufferBase ,byteBufferBase + left_size - 8, ((long)rankDiff << 3)).toString();
        long e1 = System.currentTimeMillis();
        System.out.println("one quantile time is " + (e1 - s1) + " percentile " + percentile + "rank "+ rank + " index " + index  + " table " + tabName[flag_table] + " column " + colName[flag_table][flag_colum] + " " + ans);
        return "0";
//        return ans;
    }

    private void loadStore(File[] dataFileList) throws Exception {
        for(int j = 0; j < TABLENUM; j++)
        {
            for (int i = 0; i < BOUNDARYSIZE; i++){
                beginOrder[j][0][i] = 0;
                beginOrder[j][1][i] = 0;
            }
        }
        long[][] readStartEachThread = new long[THREADNUM][TABLENUM];
        long[][] trueSizeOfMmapEachThread = new long[THREADNUM][TABLENUM];
        FileChannel[] allFileChannel = new FileChannel[TABLENUM];
        byte[] bytes = new byte[42];
        ByteBuffer byteBuffer = ByteBuffer.wrap(bytes);
        for(int k = 0; k < dataFileList.length; k++)
        {
            File dataFile = dataFileList[k];
            RandomAccessFile fis = new RandomAccessFile(dataFile, "r");
            FileChannel channel = fis.getChannel();
            allFileChannel[k] = channel;
            long size = channel.size();
            long sizePerBuffer = size / THREADNUM;
            byteBuffer.clear();
            channel.read(byteBuffer);
            long hasReadByte = 0;
            for(int i = 0, pre = 0, j = 0; i < 42; i++)
            {
                byte t = bytes[i];
                hasReadByte++;
                if(t == 44)
                {
                    colName[k][j++] = new String(bytes, pre, i, "UTF-8");
                    pre = i + 1;
                }
                else if(bytes[i] == 10)
                {
                    colName[k][j++] = new String(bytes, pre, i - pre, "UTF-8");
                    break;
                }
            }
            tabName[k] = dataFile.getName();


            for(int i = 0; i < THREADNUM; i++)
            {
                if(i == THREADNUM - 1)
                {
                    readStartEachThread[i][k] = hasReadByte;
                    trueSizeOfMmapEachThread[i][k] = size - hasReadByte;
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
                readStartEachThread[i][k] = hasReadByte;
                trueSizeOfMmapEachThread[i][k] = sizePerBuffer - needMove;
                hasReadByte += (sizePerBuffer-needMove);
            }
        }
        //实验
        String outLDir, outRDir;
        File LoutFile, RoutFile;
        RandomAccessFile Lrw, Rrw;
        for(int j = 0; j < TABLENUM; j++)
        {
            for(int i = 0; i < BOUNDARYSIZE; i++)
            {
                outLDir = workDir + "/" + tabName[j] + "-" + colName[j][0] + "-"  +  i;
                outRDir = workDir + "/" + tabName[j] + "-" + colName[j][1] + "-" + i;
                LoutFile = new File(outLDir);
                RoutFile = new File(outRDir);
                Lrw = new RandomAccessFile(LoutFile, "rw");
                Rrw = new RandomAccessFile(RoutFile, "rw");
                Lrw.setLength(FILE_SIZE);
                Rrw.setLength(FILE_SIZE);
                leftChannel[j][i] = Lrw.getChannel();
                rightChannel[j][i] = Rrw.getChannel();
            }
        }
        ArrayDeque<ByteBuffer> fullQueue = new ArrayDeque<>(200000);
        ArrayDeque<ByteBuffer> emptyQueue = new ArrayDeque<>(200000);
        ArrayDeque<Tuple> fullQueueID = new ArrayDeque<>(200000);
        AtomicBoolean fullQueueSpinLock = new AtomicBoolean(false);
        AtomicBoolean emptyQueueSpinLock = new AtomicBoolean(false);
        for(int i = 0; i < THREADNUM; i++)
        {
            //new Thread(new ThreadTask(i, readStartEachThread[i], trueSizeOfMmapEachThread[i], allFileChannel)).start();
            new Thread(new ProducerThread(i, readStartEachThread[i],trueSizeOfMmapEachThread[i], allFileChannel, fullQueue, emptyQueue, fullQueueID, fullQueueSpinLock, emptyQueueSpinLock )).start();
        }
        for(int i = 0; i < WRITETHREAD; i++)
        {
            new Thread(new ConsumerThread(i, fullQueue, emptyQueue, fullQueueID,fullQueueSpinLock, emptyQueueSpinLock)).start();
        }

        latch.await();

        StringBuilder builder= new StringBuilder(workDir).append("/index");
        FileChannel fileChannel = new RandomAccessFile(new File(builder.toString()), "rw").getChannel();
        for(int i = 0; i < TABLENUM; i++)
        {
            fileChannel.write(ByteBuffer.wrap(tabName[i].getBytes(StandardCharsets.UTF_8)));
            fileChannel.write(ByteBuffer.wrap("\n".getBytes(StandardCharsets.UTF_8)));
            for(int j = 0; j < COLNUM_EACHTABLE; j++)
            {
                fileChannel.write(ByteBuffer.wrap(colName[i][j].getBytes(StandardCharsets.UTF_8)));
                fileChannel.write(ByteBuffer.wrap("\n".getBytes(StandardCharsets.UTF_8)));
            }
        }
        for(int j = 0; j < TABLENUM; j++)
        {
            int  lBry = 0, rBry = 0;
            for (int i = 0; i < BOUNDARYSIZE; i++){
                beginOrder[j][0][i] = lBry + 1;
                lBry += blockSize[j][0][i];
                beginOrder[j][1][i] = rBry + 1;
                rBry += blockSize[j][1][i];
            }
        }
        byte[] b_t = new byte[BOUNDARYSIZE * 4];
        ByteBuffer b_t_buffer = ByteBuffer.wrap(b_t);
        //b_t_buffer.order(ByteOrder.LITTLE_ENDIAN);
        for(int i = 0; i < TABLENUM; i++)
        {
            for(int j = 0; j < COLNUM_EACHTABLE; j++)
            {
                b_t_buffer.clear();
                for(int k = 0; k < BOUNDARYSIZE; k++)
                {
                    b_t_buffer.putInt(beginOrder[i][j][k]);
                }
                b_t_buffer.flip();
                fileChannel.write(b_t_buffer);
            }
        }
        System.out.println();
        System.out.println("table 0 " + ( beginOrder[0][0][BOUNDARYSIZE - 1] - 1 )  + " " + ( beginOrder[0][1][BOUNDARYSIZE - 1] - 1) );
        System.out.println("table 1 " + ( beginOrder[1][0][BOUNDARYSIZE - 1] - 1 )  + " " + ( beginOrder[1][1][BOUNDARYSIZE - 1] - 1) );
    }


    class ProducerThread implements Runnable{
        long[] readStart;
        long[] trueSizeOfMmap;
        int threadNo;
        long directBufferBase;
        FileChannel[] fileChannel;
        ByteBuffer directBuffer;
        ByteBuffer[] leftBufs;
        ByteBuffer[] rightBufs;
        ArrayDeque<ByteBuffer> fullQueue, emptyQueue;
        AtomicBoolean fullQueueSpinLock, emptyQueueSpinLock;
        ArrayDeque<Tuple> fullQueueID; //表明queue里的元素属于哪个表
        ProducerThread(int threadNo, long[] readStart , long[] trueSizeOfMmap, FileChannel[] fileChannel, ArrayDeque<ByteBuffer> fullQueue,
                       ArrayDeque<ByteBuffer> emptyQueue, ArrayDeque<Tuple> fullQueueID,AtomicBoolean fullQueueSpinLock, AtomicBoolean emptyQueueSpinLock)
        {
            this.threadNo = threadNo;
            this.readStart = readStart;
            this.trueSizeOfMmap = trueSizeOfMmap;
            this.fileChannel = fileChannel;
            this.fullQueue = fullQueue;
            this.emptyQueue = emptyQueue;
            this.fullQueueID = fullQueueID;
            this.fullQueueSpinLock = fullQueueSpinLock;
            this.emptyQueueSpinLock = emptyQueueSpinLock;
        }
        @Override
        public void run() {
            this.leftBufs = new ByteBuffer[BOUNDARYSIZE];
            this.rightBufs = new ByteBuffer[BOUNDARYSIZE];
            this.directBuffer = ByteBuffer.allocateDirect(EACHREADSIZE);
            this.directBufferBase = ((DirectBuffer)directBuffer).address();
            long writeTime = 0;
            for (int i = 0; i < BOUNDARYSIZE; i++) {
                leftBufs[i] = ByteBuffer.allocateDirect(BYTEBUFFERSIZE);
                leftBufs[i].order(ByteOrder.LITTLE_ENDIAN);
                rightBufs[i] = ByteBuffer.allocateDirect(BYTEBUFFERSIZE);
                rightBufs[i].order(ByteOrder.LITTLE_ENDIAN);
            }
            try{
                for(int k = 0; k < TABLENUM; k++)
                {
                    String curTableName = tabName[k];
                    long nowRead = 0, realRead, yuzhi = trueSizeOfMmap[k] - EACHREADSIZE;
                    long curReadStart = readStart[k];
                    while(nowRead < yuzhi) {
                        realRead = EACHREADSIZE;
                        directBuffer.clear();
                        fileChannel[k].read(directBuffer, curReadStart + nowRead);
                        for(int i = (int)realRead-1; i >= 0; i--) {
                            if(unsafe.getByte(directBufferBase + i) != 10) {
                                realRead--;
                            } else {
                                break;
                            }
                        }
                        nowRead += realRead;
                        long val = 0;
                        long position;
                        byte t;
                        long curPos = directBufferBase;
                        long endPos = directBufferBase + realRead;
                        for(; curPos < endPos; curPos++) {
                            t = unsafe.getByte(curPos);
                            if((t & 16) == 0) {
                                if(t == 44) {
                                    int leftIndex = (int)(val >> SHIFTBITNUM);
                                    if(leftBufs[leftIndex] == null)
                                    {
                                        while(true)
                                        {
                                            while (!emptyQueueSpinLock.compareAndSet(false, true)){}
                                            if(!emptyQueue.isEmpty())
                                            {
                                                leftBufs[leftIndex] = emptyQueue.removeFirst();
                                                emptyQueueSpinLock.set(false);
                                                break;
                                            }
                                            else {
                                                emptyQueueSpinLock.set(false);
                                            }
                                        }
                                    }
                                    ByteBuffer byteBuffer = leftBufs[leftIndex];
                                    byteBuffer.putLong(val);
                                    position = byteBuffer.position();
                                    if (position >= BYTEBUFFERSIZE) {
                                        //填入
                                        while (!fullQueueSpinLock.compareAndSet(false, true)){}
                                        fullQueue.offer(byteBuffer);
                                        fullQueueID.offer(new Tuple(k, 0, leftIndex));
                                        leftBufs[leftIndex] = null;
                                        fullQueueSpinLock.set(false);
                                    }
                                    val = 0;
                                }else {
                                    int rightIndex = (int)(val >> SHIFTBITNUM);
                                    if(rightBufs[rightIndex] == null)
                                    {
                                        while(true)
                                        {
                                            while (!emptyQueueSpinLock.compareAndSet(false, true)){}
                                            if(!emptyQueue.isEmpty())
                                            {
                                                rightBufs[rightIndex] = emptyQueue.removeFirst();
                                                emptyQueueSpinLock.set(false);
                                                break;
                                            }
                                            else {
                                                emptyQueueSpinLock.set(false);
                                            }
                                        }
                                    }
                                    ByteBuffer byteBuffer = rightBufs[rightIndex];
                                    byteBuffer.putLong(val);
                                    position = byteBuffer.position();
                                    if (position >= BYTEBUFFERSIZE) {
                                        while (!fullQueueSpinLock.compareAndSet(false, true)){}
                                        fullQueue.offer(byteBuffer);
                                        fullQueueID.offer(new Tuple(k, 1, rightIndex));
                                        rightBufs[rightIndex] = null;
                                        fullQueueSpinLock.set(false);
                                    }
                                    val = 0;
                                }
                            }
                            else {
                                val = val * 10 + (t - 48);
                            }
                        }
                    }
                    realRead = trueSizeOfMmap[k] - nowRead;
                    directBuffer.clear();
                    fileChannel[k].read(directBuffer, curReadStart + nowRead);
                    long val = 0;
                    long position = 0;
                    byte t;
                    long curPos = directBufferBase;
                    long endPos = directBufferBase + realRead;
                    for(; curPos < endPos; curPos++) {
                        t = unsafe.getByte(curPos);
                        if((t & 16) == 0) {
                            if(t == 44) {
                                int leftIndex = (int)(val >> SHIFTBITNUM);
                                if(leftBufs[leftIndex] == null)
                                {
                                    while(true)
                                    {
                                        while (!emptyQueueSpinLock.compareAndSet(false, true)){}
                                        if(!emptyQueue.isEmpty())
                                        {
                                            leftBufs[leftIndex] = emptyQueue.removeFirst();
                                            emptyQueueSpinLock.set(false);
                                            break;
                                        }
                                        else {
                                            emptyQueueSpinLock.set(false);
                                        }
                                    }
                                }
                                ByteBuffer byteBuffer = leftBufs[leftIndex];
                                byteBuffer.putLong(val);
                                position = byteBuffer.position();
                                if (position >= BYTEBUFFERSIZE) {
                                    //填入
                                    while (!fullQueueSpinLock.compareAndSet(false, true)){}
                                    fullQueue.offer(byteBuffer);
                                    fullQueueID.offer(new Tuple(k, 0, leftIndex));
                                    leftBufs[leftIndex] = null;
                                    fullQueueSpinLock.set(false);
                                }
                                val = 0;
                            }else {
                                int rightIndex = (int)(val >> SHIFTBITNUM);
                                if(rightBufs[rightIndex] == null)
                                {
                                    while(true)
                                    {
                                        while (!emptyQueueSpinLock.compareAndSet(false, true)){}
                                        if(!emptyQueue.isEmpty())
                                        {
                                            rightBufs[rightIndex] = emptyQueue.removeFirst();
                                            emptyQueueSpinLock.set(false);
                                            break;
                                        }
                                        else {
                                            emptyQueueSpinLock.set(false);
                                        }
                                    }
                                }
                                ByteBuffer byteBuffer = rightBufs[rightIndex];
                                byteBuffer.putLong(val);
                                position = byteBuffer.position();
                                if (position >= BYTEBUFFERSIZE) {
                                    while (!fullQueueSpinLock.compareAndSet(false, true)){}
                                    fullQueue.offer(byteBuffer);
                                    fullQueueID.offer(new Tuple(k, 1, rightIndex));
                                    rightBufs[rightIndex] = null;
                                    fullQueueSpinLock.set(false);
                                }
                                val = 0;
                            }
                        }
                        else {
                            val = val * 10 + (t - 48);
                        }
                    }
                    while (!fullQueueSpinLock.compareAndSet(false, true)){}
                    for(int i = 0; i < BOUNDARYSIZE; i++) {
                        ByteBuffer byteBuffer = leftBufs[i];
                        if(byteBuffer.position() == 0)
                            continue;
                        else
                        {
                            fullQueue.offer(byteBuffer);
                            fullQueueID.offer(new Tuple(k, 0, i));
                            leftBufs[i] = null;
                        }
                    }
                    for(int i = 0; i < BOUNDARYSIZE; i++)
                    {
                        ByteBuffer byteBuffer = rightBufs[i];
                        if(byteBuffer.position() == 0)
                            continue;
                        else
                        {
                            fullQueue.offer(byteBuffer);
                            fullQueueID.offer(new Tuple(k, 1, i));
                            rightBufs[i] = null;
                        }

                    }
                    fullQueueSpinLock.set(false);
                    //收回资源

                }
            }catch (Exception e){
                e.printStackTrace();
            }
            endFlag.getAndAdd(1 << threadNo);
            System.out.println("Thread " + threadNo + " write time " + writeTime);
        }
    }
    class ConsumerThread implements Runnable{
        ArrayDeque<ByteBuffer> fullQueue, emptyQueue;
        ArrayDeque<Tuple> fullQueueID;
        AtomicBoolean fullQueueSpinLock, emptyQueueSpinLock;
        long[][] leftStart = new long[TABLENUM][BOUNDARYSIZE];
        long[][] rightStart = new long[TABLENUM][BOUNDARYSIZE];
        int threadNo;
        ConsumerThread(int threadNo, ArrayDeque<ByteBuffer> fullQueue, ArrayDeque<ByteBuffer> emptyQueue, ArrayDeque<Tuple> fullQueueID,AtomicBoolean fullQueueSpinLock, AtomicBoolean emptyQueueSpinLock)
        {
            this.threadNo = threadNo;
            this.fullQueue = fullQueue;
            this.emptyQueue = emptyQueue;
            this.fullQueueID = fullQueueID;
            this.fullQueueSpinLock = fullQueueSpinLock;
            this.emptyQueueSpinLock = emptyQueueSpinLock;
        }
        @Override
        public void run() {
            long base =  threadNo * DATAIN_EACHBLOCKTHREAD;
            for(int i = 0; i < TABLENUM; i++)
            {
                leftStart[i] = new long[BOUNDARYSIZE];
                rightStart[i] = new long[BOUNDARYSIZE];
                for(int j = 0; j < BOUNDARYSIZE; j++)
                {
                    leftStart[i][j] = rightStart[i][j] = base;
                }
            }
            try {
                while (true)
                {
                    while (!fullQueueSpinLock.compareAndSet(false, true)){}
                    if(fullQueue.isEmpty())
                    {
                        fullQueueSpinLock.set(false);
                        if(endFlag.get() == ALLEND)
                        {
                            synchronized (blockSize)
                            {
                                for(int i = 0; i < TABLENUM; i++)
                                {
                                    for(int k = 0; k < BOUNDARYSIZE; k++)
                                    {
                                        blockSize[i][0][k] += (int)((leftStart[i][k] - base) >> 3);
                                        blockSize[i][1][k] += (int)((rightStart[i][k] - base) >> 3);
                                    }
                                }
                            }
                            latch.countDown();
                            return;
                        }
                    }
                    else
                    {
                        ByteBuffer byteBuffer = fullQueue.removeFirst();
                        Tuple id = fullQueueID.removeFirst();
                        fullQueueSpinLock.set(false);
                        //val2 means col id
                        FileChannel fileChannel;
                        long start;
                        if(id.val2 == 0)
                        {
                            fileChannel = leftChannel[id.val1][id.val3];
                            start = leftStart[id.val1][id.val3];
                            leftStart[id.val1][id.val3] += byteBuffer.position();
                        }
                        else {
                            fileChannel = rightChannel[id.val1][id.val3];
                            start = rightStart[id.val1][id.val3];
                            rightStart[id.val1][id.val3] += byteBuffer.position();
                        }
                        byteBuffer.flip();
                        fileChannel.write(byteBuffer, start);
                        byteBuffer.clear();
                        while (!emptyQueueSpinLock.compareAndSet(false, true)){}
                        emptyQueue.offer(byteBuffer);
                        emptyQueueSpinLock.set(false);
                    }
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }
}