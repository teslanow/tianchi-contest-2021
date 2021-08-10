package com.aliyun.adb.contest;

import com.aliyun.adb.contest.spi.AnalyticDB;
import sun.misc.Unsafe;
import sun.nio.ch.DirectBuffer;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.text.SimpleDateFormat;
import java.util.ArrayDeque;
import java.util.Arrays;
import java.util.Date;
import java.util.Queue;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.SimpleFormatter;

public class SimpleAnalyticDB implements AnalyticDB {

    //提交需改
    private static final long DATAIN_EACHBLOCKTHREAD = 400000; //每个线程处理的每个块的数据量（字节单位）
    private static final int BOUNDARYSIZE = 1040;
    private static final int QUANTILE_DATA_SIZE = 16000000; //每次查询的data量，基本等于DATALENGTH / BOUNDARYSIZE * 8
    private static final int THREADNUM = 5;
    private static final int WRITETHREAD = 10;
    private static AtomicInteger endFlag = new AtomicInteger();
    private static final int ALLEND = (1 << THREADNUM) - 1;
    private static final long DATALENGTH = 1000000000;
    private static final long FILE_SIZE = 1000000;
    private static final int BYTEBUFFERSIZE = 64 * 1024;
    private static final int EACHREADSIZE = 16 * 1024 * 1024;
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
    private AtomicBoolean[][] leftChannelSpinLock = new AtomicBoolean[TABLENUM][BOUNDARYSIZE];
    private AtomicBoolean[][] rightChannelSpinLock = new AtomicBoolean[TABLENUM][BOUNDARYSIZE];
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
                leftChannelSpinLock[j][i] = new AtomicBoolean(false);
                rightChannel[j][i] = Rrw.getChannel();
                rightChannelSpinLock[j][i] = new AtomicBoolean(false);
            }
        }
        LinkedBlockingDeque<Tuple> fullQueue = new LinkedBlockingDeque<>(200000);
        LinkedBlockingDeque<Tuple> emptyQueue = new LinkedBlockingDeque<>(200000);
        for(int i = 0; i < THREADNUM; i++)
        {
            new Thread(new ProducerThread(i, readStartEachThread[i],trueSizeOfMmapEachThread[i], allFileChannel, fullQueue, emptyQueue )).start();
        }
        for(int i = 0; i < WRITETHREAD; i++)
        {
            new Thread(new ConsumerThread(i, fullQueue, emptyQueue)).start();
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
        Tuple[][][] allBufs = new Tuple[TABLENUM][COLNUM_EACHTABLE][BOUNDARYSIZE];
        Tuple[] leftBufs;
        Tuple[] rightBufs;
        LinkedBlockingDeque<Tuple> fullQueue, emptyQueue;
        ProducerThread(int threadNo, long[] readStart , long[] trueSizeOfMmap, FileChannel[] fileChannel, LinkedBlockingDeque<Tuple> fullQueue,
                       LinkedBlockingDeque<Tuple> emptyQueue)
        {
            this.threadNo = threadNo;
            this.readStart = readStart;
            this.trueSizeOfMmap = trueSizeOfMmap;
            this.fileChannel = fileChannel;
            this.fullQueue = fullQueue;
            this.emptyQueue = emptyQueue;
        }
        @Override
        public void run() {

            this.directBuffer = ByteBuffer.allocateDirect(EACHREADSIZE);
            this.directBufferBase = ((DirectBuffer)directBuffer).address();
            for(int i = 0; i < TABLENUM; i++)
            {
                for(int j = 0; j < COLNUM_EACHTABLE; j++)
                {
                    for(int k = 0; k < BOUNDARYSIZE; k++)
                    {
                        Tuple t = new Tuple(0, 0, 0, null);
                        allBufs[i][j][k] = t;
                        t.val4 = ByteBuffer.allocateDirect(BYTEBUFFERSIZE);
                        t.val4.order(ByteOrder.LITTLE_ENDIAN);
                    }
                }
            }
            try{
                for(int k = 0; k < TABLENUM; k++)
                {
                    leftBufs = allBufs[k][0];
                    rightBufs = allBufs[k][1];
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
                                        leftBufs[leftIndex] = emptyQueue.take();
                                    }
                                    Tuple tuple = leftBufs[leftIndex];
                                    ByteBuffer byteBuffer = tuple.val4;
                                    byteBuffer.putLong(val);
                                    position = byteBuffer.position();
                                    if (position >= BYTEBUFFERSIZE) {
                                        //填入
                                        tuple.setAll(k, 0, leftIndex);
                                        fullQueue.put(tuple);
                                        leftBufs[leftIndex] = null;
                                    }
                                    val = 0;
                                }else {
                                    int rightIndex = (int)(val >> SHIFTBITNUM);
                                    if(rightBufs[rightIndex] == null)
                                    {
                                        rightBufs[rightIndex] = emptyQueue.take();
                                    }
                                    Tuple tuple = rightBufs[rightIndex];
                                    ByteBuffer byteBuffer = tuple.val4;
                                    byteBuffer.putLong(val);
                                    position = byteBuffer.position();
                                    if (position >= BYTEBUFFERSIZE) {
                                        tuple.setAll(k, 1, rightIndex);
                                        fullQueue.put(tuple);
                                        rightBufs[rightIndex] = null;
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
                                    leftBufs[leftIndex] = emptyQueue.take();
                                }
                                Tuple tuple = leftBufs[leftIndex];
                                ByteBuffer byteBuffer = tuple.val4;
                                byteBuffer.putLong(val);
                                position = byteBuffer.position();
                                if (position >= BYTEBUFFERSIZE) {
                                    //填入
                                    tuple.setAll(k, 0, leftIndex);
                                    fullQueue.put(tuple);
                                    leftBufs[leftIndex] = null;
                                }
                                val = 0;
                            }else {
                                int rightIndex = (int)(val >> SHIFTBITNUM);
                                if(rightBufs[rightIndex] == null)
                                {
                                    rightBufs[rightIndex] = emptyQueue.take();
                                }
                                Tuple tuple = rightBufs[rightIndex];
                                ByteBuffer byteBuffer = tuple.val4;
                                byteBuffer.putLong(val);
                                position = byteBuffer.position();
                                if (position >= BYTEBUFFERSIZE) {
                                    tuple.setAll(k, 1, rightIndex);
                                    fullQueue.put(tuple);
                                    rightBufs[rightIndex] = null;
                                }
                                val = 0;
                            }
                        }
                        else {
                            val = val * 10 + (t - 48);
                        }
                    }
                    for(int i = 0; i < BOUNDARYSIZE; i++) {
                        Tuple tuple = leftBufs[i];
                        if(tuple == null)
                            continue;
                        ByteBuffer byteBuffer = tuple.val4;
                        if(byteBuffer.position() == 0)
                            continue;
                        else
                        {
                            tuple.setAll(k, 0, i);
                            fullQueue.put(tuple);
                            leftBufs[i] = null;
                        }
                    }
                    for(int i = 0; i < BOUNDARYSIZE; i++)
                    {
                        Tuple tuple = rightBufs[i];
                        if(tuple == null)
                            continue;
                        ByteBuffer byteBuffer = tuple.val4;
                        if(byteBuffer.position() == 0)
                            continue;
                        else
                        {
                            tuple.setAll(k, 1, i);
                            fullQueue.put(tuple);
                            rightBufs[i] = null;
                        }

                    }
                    //System.out.println("Thread " + threadNo + " finish " + new SimpleDateFormat("HH:mm:ss").format(new Date(System.currentTimeMillis())));
                }
                endFlag.getAndAdd(1 << threadNo);
                if(endFlag.get() == ALLEND)
                {
                    for(int i = 0; i < WRITETHREAD;i++)
                    {
                        fullQueue.put(new Tuple(0, 0, 0, null));
                    }
                }

            }catch (Exception e){
                e.printStackTrace();
            }


        }
    }
    class ConsumerThread implements Runnable{
        LinkedBlockingDeque<Tuple> fullQueue, emptyQueue;
        long[][] leftStart = new long[TABLENUM][BOUNDARYSIZE];
        long[][] rightStart = new long[TABLENUM][BOUNDARYSIZE];
        int threadNo;
        ConsumerThread(int threadNo, LinkedBlockingDeque<Tuple> fullQueue, LinkedBlockingDeque<Tuple> emptyQueue)
        {
            this.threadNo = threadNo;
            this.fullQueue = fullQueue;
            this.emptyQueue = emptyQueue;
        }
        @Override
        public void run() {
            long writeTime = 0;
            long times = 0;
            for(int i = 0; i < TABLENUM; i++)
            {
                leftStart[i] = new long[BOUNDARYSIZE];
                rightStart[i] = new long[BOUNDARYSIZE];
            }
            try {
                while (true)
                {
                    Tuple id = fullQueue.take();
                    if(id.val4 == null)
                    {
                        synchronized (blockSize)
                        {
                            for(int i = 0; i < TABLENUM; i++)
                            {
                                for(int k = 0; k < BOUNDARYSIZE; k++)
                                {
                                    blockSize[i][0][k] += (int)((leftStart[i][k]) >> 3);
                                    blockSize[i][1][k] += (int)((rightStart[i][k]) >> 3);
                                }
                            }
                        }
                        System.out.println("Consumer Thread " + threadNo + " Write time " + writeTime + " total times " + times);
                        latch.countDown();
                        return;
                    }
                    else
                    {
                        times++;
                        ByteBuffer byteBuffer = id.val4;
                        //val2 means col id
                        FileChannel fileChannel;
                        AtomicBoolean atomicBoolean;
                        if(id.val2 == 0)
                        {
                            fileChannel = leftChannel[id.val1][id.val3];
                            leftStart[id.val1][id.val3] += byteBuffer.position();
                            atomicBoolean = leftChannelSpinLock[id.val1][id.val3];
                        }
                        else {
                            fileChannel = rightChannel[id.val1][id.val3];
                            rightStart[id.val1][id.val3] += byteBuffer.position();
                            atomicBoolean = rightChannelSpinLock[id.val1][id.val3];
                        }
                        byteBuffer.flip();
                        long s = System.currentTimeMillis();
                        //fileChannel.write(byteBuffer, start);
                        while (!atomicBoolean.compareAndSet(false, true)){}
                        fileChannel.write(byteBuffer);
                        atomicBoolean.set(false);
                        long e = System.currentTimeMillis();
                        writeTime += (e - s);
                        byteBuffer.clear();
                        emptyQueue.put(id);
                    }
                }
            } catch (IOException | InterruptedException e) {
                e.printStackTrace();
            }
        }
    }
}