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
import java.util.Iterator;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;


public class SimpleAnalyticDB implements AnalyticDB {

//    private static final int BOUNDARYSIZE = 130;
//    private static final int QUANTILE_DATA_SIZE = 32000000; //每次查询的data量，基本等于DATALENGTH / BOUNDARYSIZE * 8
//    private static final int THREADNUM = 16;
//    private static final long DATALENGTH = 500000000;
//    private static final int BYTEBUFFERSIZE = 1024 * 128;
//    private static final int EACHREADSIZE = 1024 * 1024 * 16;
//    //private static final int EACHREADSIZE = 1024;
//    private static final int TABLENUM = 2;
//    private static final int COLNUM_EACHTABLE = 2;
//    private static final int SHIFTBITNUM = 56;
//    private static final int CONCURRENT_QUANTILE_THREADNUM = 8;

//    private static final int BOUNDARYSIZE = 130;
//    private static final int QUANTILE_DATA_SIZE = 800; //每次查询的data量，基本等于DATALENGTH / BOUNDARYSIZE * 8
//    private static final int THREADNUM = 1;
//    private static final long DATALENGTH = 10000;
//    private static final int BYTEBUFFERSIZE = 1024 * 128;
//    private static final int EACHREADSIZE = 1024 ;
//    //private static final int EACHREADSIZE = 1024;
//    private static final int TABLENUM = 2;
//    private static final int COLNUM_EACHTABLE = 2;
//    private static final int SHIFTBITNUM = 56;
//    private static final int CONCURRENT_QUANTILE_THREADNUM = 8;

    //提交需改
    private static final int BIG_BOUNDARYSIZE = 520;
    private static final int QUANTILE_DATA_SIZE = 16000000; //每次查询的data量，基本等于DATALENGTH / BOUNDARYSIZE * 8
    private static final int THREADNUM = 64;
    private static final long DATALENGTH = 1000000000;

    private static final int EACHREADSIZE = 16 * 1024 * 1024;
    private static final int TABLENUM = 2;
    private static final int COLNUM_EACHTABLE = 2;
    private static final int BIG_SHIFTBITNUM = 54;
    private static final int SMALL_REALSHIFTBITNUM = 53;
    private static final int CONCURRENT_QUANTILE_THREADNUM = 8;
    private static final int SMALL_BOUNDARYSIZE = 1040;
    private static final int DIFF_BITS = (BIG_SHIFTBITNUM - SMALL_REALSHIFTBITNUM);
    private static final int BOUND_INTERVAL = (1 << DIFF_BITS);
    private static final int BIG_BYTEBUFFERSIZE = 64 * 1024;
    private static final int SMALL_BYTEBUFFERSIZE = BIG_BYTEBUFFERSIZE / BOUND_INTERVAL;

    private int current_Quantile_threadNUM = 0;
    private String[][] colName = new String[TABLENUM][COLNUM_EACHTABLE];
    private String[] tabName = new String[TABLENUM];
    private String curTableName;
    private Unsafe unsafe;
    private final int[][][] blockSize = new int[TABLENUM][COLNUM_EACHTABLE][SMALL_BOUNDARYSIZE];
    private final int[][][] beginOrder = new int[TABLENUM][COLNUM_EACHTABLE][SMALL_BOUNDARYSIZE];
    private long[] quantile_load_base = new long[CONCURRENT_QUANTILE_THREADNUM];
    private ByteBuffer[] quantile_load_buffer = new ByteBuffer[CONCURRENT_QUANTILE_THREADNUM];
    private long arrThreadId[] = new long[CONCURRENT_QUANTILE_THREADNUM];
    private static final CountDownLatch latch = new CountDownLatch(THREADNUM);

    //实验
    private FileChannel[][] leftChannel = new FileChannel[TABLENUM][BIG_BOUNDARYSIZE];
    private FileChannel[][] rightChannel = new FileChannel[TABLENUM][BIG_BOUNDARYSIZE];
    private AtomicBoolean[][] leftChannelSpinLock = new AtomicBoolean[TABLENUM][BIG_BYTEBUFFERSIZE];
    private AtomicBoolean[][] rightChannelSpinLock = new AtomicBoolean[TABLENUM][BIG_BOUNDARYSIZE];
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
        //System.out.println("thread " + Thread.currentThread().getId() + " current_threadnum " + current_Quantile_threadNUM + " buffer_id " + buffer_index );
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
//        System.out.println("one quantile time is " + (e1 - s1) + " percentile " + percentile + "rank "+ rank + " index " + index  + " table " + tabName[flag_table] + " column " + colName[flag_table][flag_colum]);
        //return "0";
        return ans;
    }

    private void loadStore(File[] dataFileList) throws Exception {
        for(int j = 0; j < TABLENUM; j++)
        {
            for (int i = 0; i < SMALL_BOUNDARYSIZE; i++){
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
            for(int i = 0; i < BIG_BOUNDARYSIZE; i++)
            {
                outLDir = workDir + "/" + tabName[j] + "-" + colName[j][0] + "-"  +  i;
                outRDir = workDir + "/" + tabName[j] + "-" + colName[j][1] + "-" + i;
                LoutFile = new File(outLDir);
                RoutFile = new File(outRDir);
                Lrw = new RandomAccessFile(LoutFile, "rw");
                Rrw = new RandomAccessFile(RoutFile, "rw");
                leftChannel[j][i] = Lrw.getChannel();
                rightChannel[j][i] = Rrw.getChannel();
                leftChannelSpinLock[j][i] = new AtomicBoolean(false);
                rightChannelSpinLock[j][i] = new AtomicBoolean(false);
            }
        }

        for(int i = 0; i < THREADNUM; i++)
        {
            new Thread(new ThreadTask(i, readStartEachThread[i], trueSizeOfMmapEachThread[i], allFileChannel)).start();
        }
        latch.await();

        StringBuilder builder= new StringBuilder(workDir + "/index");
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
            for (int i = 0; i < SMALL_BOUNDARYSIZE; i++){
                beginOrder[j][0][i] = lBry + 1;
                lBry += blockSize[j][0][i];
                beginOrder[j][1][i] = rBry + 1;
                rBry += blockSize[j][1][i];
            }
        }
        byte[] b_t = new byte[SMALL_BOUNDARYSIZE * 4];
        ByteBuffer b_t_buffer = ByteBuffer.wrap(b_t);
        //b_t_buffer.order(ByteOrder.LITTLE_ENDIAN);
        for(int i = 0; i < TABLENUM; i++)
        {
            for(int j = 0; j < COLNUM_EACHTABLE; j++)
            {
                b_t_buffer.clear();
                for(int k = 0; k < SMALL_BOUNDARYSIZE; k++)
                {
                    b_t_buffer.putInt(beginOrder[i][j][k]);
                }
                b_t_buffer.flip();
                fileChannel.write(b_t_buffer);
            }
        }
        System.out.println();
        System.out.println("table 0 " + ( beginOrder[0][0][SMALL_BOUNDARYSIZE - 1] - 1 )  + " " + ( beginOrder[0][1][SMALL_BOUNDARYSIZE - 1] - 1) );
        System.out.println("table 1 " + ( beginOrder[1][0][SMALL_BOUNDARYSIZE - 1] - 1 )  + " " + ( beginOrder[1][1][SMALL_BOUNDARYSIZE - 1] - 1) );
    }


    class ThreadTask implements Runnable {
        long[] readStart;
        long[] trueSizeOfMmap;
        int threadNo;
        long directBufferBase;
        FileChannel[] fileChannel;
        ByteBuffer directBuffer;
        ByteBuffer[] leftSmallBufs, leftBigBufs;
        ByteBuffer[] rightSmallBufs, rightBigBufs;
        ArrayDeque<Integer>[] leftSizeInBigFile = new ArrayDeque[SMALL_BOUNDARYSIZE];
        ArrayDeque<Integer>[] rightSizeInBigFile = new ArrayDeque[SMALL_BOUNDARYSIZE];

        //初始化
        public ThreadTask(int threadNo, long[] readStart ,long[] trueSizeOfMmap, FileChannel[] fileChannel) throws Exception {
            this.threadNo = threadNo;
            this.readStart = readStart;
            this.trueSizeOfMmap = trueSizeOfMmap;
            this.fileChannel = fileChannel;
            this.directBuffer = ByteBuffer.allocateDirect(EACHREADSIZE);
            this.leftSmallBufs = new ByteBuffer[SMALL_BOUNDARYSIZE];
            this.leftBigBufs = new ByteBuffer[BIG_BOUNDARYSIZE];
            this.rightSmallBufs = new ByteBuffer[SMALL_BOUNDARYSIZE];
            this.rightBigBufs = new ByteBuffer[BIG_BOUNDARYSIZE];
            this.directBufferBase = ((DirectBuffer)directBuffer).address();
        }

        @Override
        public void run() {
            for(int i = 0; i < SMALL_BOUNDARYSIZE; i++)
            {
                leftSizeInBigFile[i] = new ArrayDeque<>(100);
                rightSizeInBigFile[i] = new ArrayDeque<>(100);
            }
            for (int i = 0; i < BIG_BOUNDARYSIZE; i++) {
                leftBigBufs[i] = ByteBuffer.allocateDirect(BIG_BYTEBUFFERSIZE);
                leftBigBufs[i].order(ByteOrder.LITTLE_ENDIAN);
                rightBigBufs[i] = ByteBuffer.allocateDirect(BIG_BYTEBUFFERSIZE);
                rightBigBufs[i].order(ByteOrder.LITTLE_ENDIAN);
            }
            for(int i = 0; i < BIG_BOUNDARYSIZE;i++ )
            {
                int base = (i << DIFF_BITS);
                int slice_base = 0;
                ByteBuffer buffer = leftBigBufs[i];
                for(int j = base; j < base + BOUND_INTERVAL; j++)
                {
                    buffer.limit(slice_base + SMALL_BYTEBUFFERSIZE);
                    buffer.position(slice_base);
                    leftSmallBufs[j] = buffer.slice();
                }
            }
            for(int i = 0; i < BIG_BOUNDARYSIZE;i++ )
            {
                int base = (i << DIFF_BITS);
                int slice_base = 0;
                ByteBuffer buffer = rightBigBufs[i];
                for(int j = base; j < base + BOUND_INTERVAL; j++)
                {
                    buffer.limit(slice_base + SMALL_BYTEBUFFERSIZE);
                    buffer.position(slice_base);
                    rightSmallBufs[j] = buffer.slice();
                }
            }
            try{
                for(int k = 0; k < TABLENUM; k++)
                {
                    curTableName = tabName[k];
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
                                    int leftIndex = (int)(val >> SMALL_REALSHIFTBITNUM);
                                    ByteBuffer byteBuffer = leftSmallBufs[leftIndex];
                                    byteBuffer.putLong(val);
                                    position = byteBuffer.position();
                                    if (position == SMALL_BYTEBUFFERSIZE) {
                                        int big_index = leftIndex >> DIFF_BITS;
                                        ByteBuffer bigBuffer = leftBigBufs[big_index];
                                        bigBuffer.limit(BIG_BYTEBUFFERSIZE);
                                        bigBuffer.position(BIG_BYTEBUFFERSIZE);
                                        bigBuffer.flip();
                                        FileChannel channel = leftChannel[k][big_index];
                                        AtomicBoolean atomicBoolean = leftChannelSpinLock[k][big_index];
                                        while (!atomicBoolean.compareAndSet(false, true)){}
                                        channel.write(bigBuffer);
                                        atomicBoolean.set(false);
                                        for(int i = (big_index << DIFF_BITS); i < (big_index << DIFF_BITS) + BOUND_INTERVAL; i++)
                                        {
                                            leftSizeInBigFile[i].addLast(leftSmallBufs[i].position());
                                            leftSmallBufs[i].clear();
                                        }
                                    }
                                    val = 0;
                                }else {
                                    int rightIndex = (int)(val >> SMALL_REALSHIFTBITNUM);
                                    ByteBuffer byteBuffer = rightSmallBufs[rightIndex];
                                    byteBuffer.putLong(val);
                                    position = byteBuffer.position();
                                    if (position == SMALL_BYTEBUFFERSIZE) {
                                        int big_index = rightIndex >> DIFF_BITS;
                                        ByteBuffer bigBuffer = rightBigBufs[big_index];
                                        bigBuffer.limit(BIG_BYTEBUFFERSIZE);
                                        bigBuffer.position(BIG_BYTEBUFFERSIZE);
                                        bigBuffer.flip();
                                        FileChannel channel = rightChannel[k][big_index];
                                        AtomicBoolean atomicBoolean = rightChannelSpinLock[k][big_index];
                                        while (!atomicBoolean.compareAndSet(false, true)){}
                                        channel.write(bigBuffer);
                                        atomicBoolean.set(false);
                                        for(int i = (big_index << DIFF_BITS) ; i < (big_index << DIFF_BITS) + BOUND_INTERVAL; i++)
                                        {
                                            rightSizeInBigFile[i].addLast(rightSmallBufs[i].position());
                                            rightSmallBufs[i].clear();
                                        }
                                    }
                                    val = 0;
                                }
                            }
                            else {
                                val = (val << 1) + (val << 3) + (t - 48);
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
                                int leftIndex = (int)(val >> SMALL_REALSHIFTBITNUM);
                                ByteBuffer byteBuffer = leftSmallBufs[leftIndex];
                                byteBuffer.putLong(val);
                                position = byteBuffer.position();
                                if (position == SMALL_BYTEBUFFERSIZE) {
                                    int big_index = leftIndex >> DIFF_BITS;
                                    ByteBuffer bigBuffer = leftBigBufs[big_index];
                                    bigBuffer.limit(BIG_BYTEBUFFERSIZE);
                                    bigBuffer.position(BIG_BYTEBUFFERSIZE);
                                    bigBuffer.flip();
                                    FileChannel channel = leftChannel[k][big_index];
                                    AtomicBoolean atomicBoolean = leftChannelSpinLock[k][big_index];
                                    while (!atomicBoolean.compareAndSet(false, true)){}
                                    channel.write(bigBuffer);
                                    atomicBoolean.set(false);
                                    for(int i = (big_index << DIFF_BITS); i < (big_index << DIFF_BITS) + BOUND_INTERVAL; i++)
                                    {
                                        leftSizeInBigFile[i].addLast(leftSmallBufs[i].position());
                                        leftSmallBufs[i].clear();
                                    }
                                }
                                val = 0;
                            }else {
                                int rightIndex = (int)(val >> SMALL_REALSHIFTBITNUM);
                                ByteBuffer byteBuffer = rightSmallBufs[rightIndex];
                                byteBuffer.putLong(val);
                                position = byteBuffer.position();
                                if (position == SMALL_BYTEBUFFERSIZE) {
                                    int big_index = rightIndex >> DIFF_BITS;
                                    ByteBuffer bigBuffer = rightBigBufs[big_index];
                                    bigBuffer.limit(BIG_BYTEBUFFERSIZE);
                                    bigBuffer.position(BIG_BYTEBUFFERSIZE);
                                    bigBuffer.flip();
                                    FileChannel channel = rightChannel[k][big_index];
                                    AtomicBoolean atomicBoolean = rightChannelSpinLock[k][big_index];
                                    while (!atomicBoolean.compareAndSet(false, true)){}
                                    channel.write(bigBuffer);
                                    atomicBoolean.set(false);
                                    for(int i = (big_index << DIFF_BITS) ; i < (big_index << DIFF_BITS) + BOUND_INTERVAL; i++)
                                    {
                                        rightSizeInBigFile[i].addLast(rightSmallBufs[i].position());
                                        rightSmallBufs[i].clear();
                                    }
                                }
                                val = 0;
                            }
                        }
                        else {
                            val = (val << 1) + (val << 3) + (t - 48);
                        }
                    }

                    //System.out.println("Size " + size);

                    for(int i = 0; i < BIG_BOUNDARYSIZE; i++) {
                        int base = i << DIFF_BITS;
                        ByteBuffer bigBuffer = leftBigBufs[i];
                        bigBuffer.limit(BIG_BYTEBUFFERSIZE);
                        bigBuffer.position(BIG_BYTEBUFFERSIZE);
                        bigBuffer.flip();
                        FileChannel channel = leftChannel[k][i];
                        AtomicBoolean atomicBoolean = leftChannelSpinLock[k][i];
                        while (!atomicBoolean.compareAndSet(false, true)){}
                        channel.write(bigBuffer);
                        atomicBoolean.set(false);
                        for(int j = base; j < BOUND_INTERVAL + base; j++)
                        {
                            leftSizeInBigFile[j].add(leftSmallBufs[j].position());
                            leftSmallBufs[j].clear();
                        }
                    }
                    for(int i = 0; i < BIG_BOUNDARYSIZE; i++)
                    {
                        int base = i << DIFF_BITS;
                        ByteBuffer bigBuffer = rightBigBufs[i];
                        bigBuffer.limit(BIG_BYTEBUFFERSIZE);
                        bigBuffer.position(BIG_BYTEBUFFERSIZE);
                        bigBuffer.flip();
                        FileChannel channel = rightChannel[k][i];
                        AtomicBoolean atomicBoolean = rightChannelSpinLock[k][i];
                        while (!atomicBoolean.compareAndSet(false, true)){}
                        channel.write(bigBuffer);
                        atomicBoolean.set(false);
                        for(int j = base; j < BOUND_INTERVAL + base; j++)
                        {
                            rightSizeInBigFile[j].add(rightSmallBufs[j].position());
                            rightSmallBufs[j].clear();
                        }
                    }
                }
            }catch (Exception e){
                e.printStackTrace();
            }
            latch.countDown();

        }

    }
}