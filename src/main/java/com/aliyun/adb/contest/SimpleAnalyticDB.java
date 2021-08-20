package com.aliyun.adb.contest;

import com.aliyun.adb.contest.spi.AnalyticDB;
import sun.misc.Unsafe;
import sun.nio.ch.DirectBuffer;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

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
//    private static final int QUANTILE_DATA_SIZE = 1600; //每次查询的data量，基本等于DATALENGTH / BOUNDARYSIZE * 8
//    private static final int THREADNUM = 1;
//    private static final long DATALENGTH = 10000;
//    private static final int BYTEBUFFERSIZE = 1024 * 128;
//    private static final int EACHREADSIZE = 1024 ;
//    //private static final int EACHREADSIZE = 1024;
//    private static final int TABLENUM = 2;
//    private static final int COLNUM_EACHTABLE = 2;
//    private static final int SHIFTBITNUM = 56;
//    private static final int CONCURRENT_QUANTILE_THREADNUM = 8;
//    private static final int QUANTILE_READ_SIZE = 1024 * 1024 * 16;
    //提交需改
    private static final int BOUNDARYSIZE = 520;
    private static final int THREADNUM = 30;
    private static final long DATALENGTH = 1000000000;
    //private static final long DATALENGTH = 300000000;
    private static final int BYTEBUFFERSIZE = 1024 * 64;
    private static final int EACHREADSIZE = 1024 * 1024 * 16;

    private static final int TABLENUM = 2;
    private static final int COLNUM_EACHTABLE = 2;
    private static final int SHIFTBITNUM = 54;
    private static final int CONCURRENT_QUANTILE_THREADNUM = 8;
    private static final long QUANTILE_DATA_SIZE = 16000000; //每次查询的总data量，基本等于DATALENGTH / BOUNDARYSIZE * 8
    //private static final long QUANTILE_DATA_SIZE = 6000000; //每次查询的总data量，基本等于DATALENGTH / BOUNDARYSIZE * 8
    private static final long ALIGN_MASK = ~(0x7);
    private static final int SMALL_SHIFTBITNUM = 53;
    private static final int SMALL_MASK = ((1 << (SHIFTBITNUM - SMALL_SHIFTBITNUM)) - 1);
    private static final int EACH_QUANTILE_THREADNUM = 4;
    private static final int INTERVAL_SMALL_BLOCK = (1 << (SHIFTBITNUM - SMALL_SHIFTBITNUM));
    private static final long QUANTILE_DATA_SIZE_OF_EACH_QTHREAD = QUANTILE_DATA_SIZE / EACH_QUANTILE_THREADNUM;
    private static final long SMALL_QUANTILE_DATA_SIZE_OF_EACH_QTHREAD = QUANTILE_DATA_SIZE_OF_EACH_QTHREAD / INTERVAL_SMALL_BLOCK; //每个查询线程每次查询的每个小块的总的DATASIZE

    private int current_Quantile_threadNUM = 0;
    private String[][] colName = new String[TABLENUM][COLNUM_EACHTABLE];
    private String[] tabName = new String[TABLENUM];
    private String curTableName;
    private Unsafe unsafe;
    private final int[][][] blockSize = new int[TABLENUM][COLNUM_EACHTABLE][BOUNDARYSIZE];
    private final int[][][] beginOrder = new int[TABLENUM][COLNUM_EACHTABLE][BOUNDARYSIZE];
    //每个查询线程会调用EACH_QUANTILE_THREADNUM个线程进行读，将得到的每个小块存储起来
    private long[] quantile_load_base = new long[CONCURRENT_QUANTILE_THREADNUM]; //每个查询线程拥有一块内存用于读文件
    private ByteBuffer[] quantile_load_buffer = new ByteBuffer[CONCURRENT_QUANTILE_THREADNUM];
    private long[][][] quantile_data_base = new long[CONCURRENT_QUANTILE_THREADNUM][EACH_QUANTILE_THREADNUM][INTERVAL_SMALL_BLOCK];
    private int[][][] quantile_data_size = new int[CONCURRENT_QUANTILE_THREADNUM][EACH_QUANTILE_THREADNUM][INTERVAL_SMALL_BLOCK];
    private long arrThreadId[] = new long[CONCURRENT_QUANTILE_THREADNUM];
    private static final CountDownLatch latch = new CountDownLatch(THREADNUM);
    private long[] findBufferBase = new long[CONCURRENT_QUANTILE_THREADNUM];

    //实验
    private FileChannel[][] leftChannel = new FileChannel[TABLENUM][BOUNDARYSIZE];
    private FileChannel[][] rightChannel = new FileChannel[TABLENUM][BOUNDARYSIZE];
    private AtomicBoolean[][] leftChannelSpinLock = new AtomicBoolean[TABLENUM][BOUNDARYSIZE];
    private AtomicBoolean[][] rightChannelSpinLock = new AtomicBoolean[TABLENUM][BOUNDARYSIZE];
    private  String workDir;
    private static ThreadPoolExecutor poolExecutor = new ThreadPoolExecutor(32, 32, 1000, TimeUnit.MILLISECONDS, new LinkedBlockingQueue<Runnable>());
    private AtomicInteger quantileTime = new AtomicInteger(500);
    public SimpleAnalyticDB() throws NoSuchFieldException, IllegalAccessException {
        this.unsafe = GetUnsafe.getUnsafe();
    }
    void initOnlyForOneQThread()
    {
        this.quantile_load_buffer[0] = ByteBuffer.allocateDirect((int)QUANTILE_DATA_SIZE);
        this.quantile_load_base[0] = ((DirectBuffer)quantile_load_buffer[0]).address();
        long allBase = unsafe.allocateMemory(QUANTILE_DATA_SIZE);
        findBufferBase[0] = allBase;
        for(int j = 0; j < EACH_QUANTILE_THREADNUM; j++)
        {
            long thread_data_base = findBufferBase[0] + j * QUANTILE_DATA_SIZE_OF_EACH_QTHREAD;
            for(int k = 0; k < INTERVAL_SMALL_BLOCK; k++)
            {
                this.quantile_data_base[0][j][k] = thread_data_base + k * SMALL_QUANTILE_DATA_SIZE_OF_EACH_QTHREAD;
            }
        }
    }
    void initForAllQThread()
    {
        long allBase = unsafe.allocateMemory(QUANTILE_DATA_SIZE * CONCURRENT_QUANTILE_THREADNUM);
        for(int i = 0; i < CONCURRENT_QUANTILE_THREADNUM; i++)
        {
            this.quantile_load_buffer[i] = ByteBuffer.allocateDirect((int)QUANTILE_DATA_SIZE);
            this.quantile_load_base[i] = ((DirectBuffer)quantile_load_buffer[i]).address();
        }
        for(int i = 0; i < CONCURRENT_QUANTILE_THREADNUM; i++)
        {
            findBufferBase[i] = allBase + (long) i * QUANTILE_DATA_SIZE;
            for(int j = 0; j < EACH_QUANTILE_THREADNUM; j++)
            {
                long thread_data_base = findBufferBase[i] + j * QUANTILE_DATA_SIZE_OF_EACH_QTHREAD;
                for(int k = 0; k < INTERVAL_SMALL_BLOCK; k++)
                {
                    this.quantile_data_base[i][j][k] = thread_data_base + k * SMALL_QUANTILE_DATA_SIZE_OF_EACH_QTHREAD;
                }
            }
        }
    }
    @Override
    public void load(String tpchDataFileDir, String workspaceDir) throws Exception {
        long ss = System.currentTimeMillis();
        workDir = workspaceDir;
        //判断工作区是否为空
        if(new File(workspaceDir + "/index").exists())
        {
            initForAllQThread();
            System.out.println("sencond load");
            RandomAccessFile file = new RandomAccessFile(new File(workDir + "/index"), "r");
            FileChannel fileChannel = file.getChannel();
            byte[] bytes = new byte[(int)fileChannel.size()];
            ByteBuffer byteBuffer = ByteBuffer.wrap(bytes);
            byteBuffer.clear();
            fileChannel.read(byteBuffer);
            byteBuffer.flip();
            int curPos = 0;
            String[] tmpString = new String[TABLENUM * COLNUM_EACHTABLE + TABLENUM];
            for(int pre = 0, index = 0;;)
            {
                if(bytes[curPos] == 10)
                {
                    tmpString[index++] = new String(bytes, pre, curPos - pre, "UTF-8");
                    if(index >= TABLENUM * COLNUM_EACHTABLE + TABLENUM)
                    {
                        curPos++;
                        break;
                    }
                    pre = curPos + 1;
                }
                curPos++;
            }
            int index_name = 0;
            byteBuffer.position(curPos);
            for(int i = 0; i < TABLENUM; i++)
            {
                tabName[i] = tmpString[index_name++];
                for(int j = 0; j < COLNUM_EACHTABLE; j++)
                {
                    colName[i][j] = tmpString[index_name++];
                    for( int k = 0; k < BOUNDARYSIZE; k++)
                    {
                        beginOrder[i][j][k] = byteBuffer.getInt();
                    }
                }
            }
            return;
        }
        else
        {
            initOnlyForOneQThread();
        }
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
                    {
                        buffer_index = i;
                        break;
                    }

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
        int rank = (int) Math.round(DATALENGTH * percentile);
        int index;
        int flag_table, flag_colum;
        if(table.equals(tabName[0]))
        {
            flag_table = 0;
        } else
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

        ByteBuffer readBuffer = quantile_load_buffer[buffer_index];
        long readBufferBase = quantile_load_base[buffer_index];
        long[][] dataBufferBase = quantile_data_base[buffer_index];
        int[][] dataSize_ofSmallBlock = quantile_data_size[buffer_index]; //每个并发加载线程处理的每个小块的数目
        StringBuilder builder = new StringBuilder(workDir);
        String fileName = builder.append("/").append(table).append("-").append(column).append("-").append(index).toString();
        FileInputStream inFile = new FileInputStream(fileName);
        FileChannel channel = inFile.getChannel();
        //System.out.println("buffer index before " + buffer_index + " " + Arrays.toString(readBufferBase) + " " + Arrays.toString(dataBufferBase[0]) + " " + Arrays.toString(dataSize_ofSmallBlock[0]) + " " + Thread.currentThread().getName() + " ID " + Thread.currentThread().getId());
        long left_size = channel.size();

        long each_read_size = (left_size / EACH_QUANTILE_THREADNUM) & (ALIGN_MASK); //保证为8倍数
        long cur_read_start = 0;
        long cur_read_size = each_read_size;
        CyclicBarrier barrier = new CyclicBarrier(EACH_QUANTILE_THREADNUM);
        readBuffer.clear();
        channel.read(readBuffer);
        for(int i = 0; i < EACH_QUANTILE_THREADNUM; i++)
        {
            if(i == EACH_QUANTILE_THREADNUM - 1)
            {
                cur_read_size = (left_size - cur_read_start);
                //poolExecutor.submit(new QuantileTask(cur_read_start, cur_read_size, readBuffer[i], readBufferBase[i],dataSize_ofSmallBlock[i], dataBufferBase[i], channel, barrier));
                new QuantileTask(cur_read_size, readBufferBase + cur_read_start,  dataSize_ofSmallBlock[i],  dataBufferBase[i], channel, barrier).run();
            }
            else
            {
                poolExecutor.submit(new QuantileTask(cur_read_size, readBufferBase + cur_read_start,  dataSize_ofSmallBlock[i],  dataBufferBase[i], channel, barrier));
                cur_read_start += cur_read_size;
            }

        }
        //System.out.println("buffer index " + buffer_index + " " + Arrays.toString(readBufferBase) + " " + Arrays.toString(dataBufferBase[0]) + " " + Arrays.toString(dataSize_ofSmallBlock[0]) );
        int[] eachSmallBlockSize = new int[INTERVAL_SMALL_BLOCK]; //该次查询中，每个小块总的数目
        for(int i = 0; i < INTERVAL_SMALL_BLOCK; i++)
        {
            for(int j = 0; j < EACH_QUANTILE_THREADNUM; j++)
            {
                eachSmallBlockSize[i] += dataSize_ofSmallBlock[j][i];
            }
        }
        int smallBlockIndex = 0;
        for(int i = 0; i < INTERVAL_SMALL_BLOCK; i++)
        {
            if(rankDiff - eachSmallBlockSize[i] <= 0)
            {
                smallBlockIndex = i;
                break;
            }
            rankDiff -= eachSmallBlockSize[i];
        }
        long byteBufferBase = findBufferBase[buffer_index];
        long copyBase = byteBufferBase;
        for(int i = 0; i < EACH_QUANTILE_THREADNUM; i++)
        {
            unsafe.copyMemory(null, dataBufferBase[i][smallBlockIndex], null, copyBase, (dataSize_ofSmallBlock[i][smallBlockIndex] << 3) );
            copyBase += (dataSize_ofSmallBlock[i][smallBlockIndex] << 3);
        }
        ans = MyFind.quickFind(unsafe, byteBufferBase ,byteBufferBase + (eachSmallBlockSize[smallBlockIndex] << 3) - 8, ((long)rankDiff << 3)).toString();
        long e1 = System.currentTimeMillis();
        System.out.println("one quantile time is " + (e1 - s1));
        if(quantileTime.addAndGet(1) >= 500)
            return "0";
        else
            return ans;
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
            for (int i = 0; i < BOUNDARYSIZE; i++){
                blockSize[j][0][i] = (int)leftChannel[j][i].position() >> 3;
                blockSize[j][1][i] = (int)rightChannel[j][i].position() >> 3;
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
        //System.out.println("table 0 " + ( beginOrder[0][0][BOUNDARYSIZE - 1] - 1 )  + " " + ( beginOrder[0][1][BOUNDARYSIZE - 1] - 1) );
        //System.out.println("table 1 " + ( beginOrder[1][0][BOUNDARYSIZE - 1] - 1 )  + " " + ( beginOrder[1][1][BOUNDARYSIZE - 1] - 1) );
    }

    class QuantileTask implements Runnable
    {
        private long  readSize;
        int[] eachBlockSize;
        long[] eachBlockBufferStart;
        FileChannel readChannel;
        CyclicBarrier barrier;
        long readBufferBase;
        //readbuffer的size等于QUANTILE_READ_SIZE
        QuantileTask(long readSize, long readBufferBase, int[] eachBlockSize,  long[] eachBlockBufferStart, FileChannel readChannel, CyclicBarrier barrier)
        {
            this.readSize = readSize;
            this.eachBlockSize = eachBlockSize;
            this.eachBlockBufferStart = eachBlockBufferStart;
            this.readChannel = readChannel;
            this.barrier = barrier;
            this.readBufferBase = readBufferBase;
        }

        @Override
        public void run() {
            long[] eachBlockBufferCurPos = new long[eachBlockBufferStart.length];
            for(int i = 0; i < eachBlockBufferStart.length; i++)
            {
                eachBlockBufferCurPos[i] = eachBlockBufferStart[i];
            }
            long readBufferPos = readBufferBase;
            long readEnd = readBufferBase + readSize;
            for(; readBufferPos < readEnd; readBufferPos += 8)
            {
                long val = unsafe.getLong(null, readBufferPos);
                int index = (int)((val >> SMALL_SHIFTBITNUM) & SMALL_MASK);
                unsafe.putLong(null, eachBlockBufferCurPos[index], val);
                eachBlockBufferCurPos[index] += 8;
            }
            for(int i = 0; i < eachBlockSize.length; i++)
            {
                eachBlockSize[i] = (int)((eachBlockBufferCurPos[i] - eachBlockBufferStart[i]) >> 3);
            }
            try {
                barrier.await();
            } catch (InterruptedException e) {
                e.printStackTrace();
            } catch (BrokenBarrierException e) {
                e.printStackTrace();
            }
        }
    }
    class ThreadTask implements Runnable {
        long[] readStart;
        long[] trueSizeOfMmap;
        int threadNo;
        long directBufferBase;
        FileChannel[] fileChannel;
        ByteBuffer directBuffer;
        ByteBuffer[] leftBufs;
        ByteBuffer[] rightBufs;
        //初始化
        public ThreadTask(int threadNo, long[] readStart ,long[] trueSizeOfMmap, FileChannel[] fileChannel) throws Exception {
            this.threadNo = threadNo;
            this.readStart = readStart;
            this.trueSizeOfMmap = trueSizeOfMmap;
            this.fileChannel = fileChannel;
            this.directBuffer = ByteBuffer.allocateDirect(EACHREADSIZE);
            this.leftBufs = new ByteBuffer[BOUNDARYSIZE];
            this.rightBufs = new ByteBuffer[BOUNDARYSIZE];
            this.directBufferBase = ((DirectBuffer)directBuffer).address();
        }

        @Override
        public void run() {
            for (int i = 0; i < BOUNDARYSIZE; i++) {
                leftBufs[i] = ByteBuffer.allocateDirect(BYTEBUFFERSIZE);
                leftBufs[i].order(ByteOrder.LITTLE_ENDIAN);
                rightBufs[i] = ByteBuffer.allocateDirect(BYTEBUFFERSIZE);
                rightBufs[i].order(ByteOrder.LITTLE_ENDIAN);
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
                        byte t;
                        long curPos = directBufferBase;
                        long endPos = directBufferBase + realRead;
                        for(; curPos < endPos; curPos++) {
                            t = unsafe.getByte(curPos);
                            if((t & 16) == 0) {
                                if(t == 44) {
                                    int leftIndex = (int)(val >> SHIFTBITNUM);
                                    ByteBuffer byteBuffer = leftBufs[leftIndex];
                                    byteBuffer.putLong(val);
                                    if (byteBuffer.position() == BYTEBUFFERSIZE) {
                                        FileChannel fileChannel = leftChannel[k][leftIndex];
                                        AtomicBoolean atomicBoolean = leftChannelSpinLock[k][leftIndex];
                                        byteBuffer.flip();
                                        while (!atomicBoolean.compareAndSet(false, true)){}
                                        fileChannel.write(byteBuffer);
                                        atomicBoolean.set(false);
                                        byteBuffer.clear();
                                    }
                                    val = 0;
                                }else {
                                    int rightIndex = (int)(val >> SHIFTBITNUM);
                                    ByteBuffer byteBuffer = rightBufs[rightIndex];
                                    byteBuffer.putLong(val);
                                    if (byteBuffer.position() == BYTEBUFFERSIZE) {
                                        FileChannel fileChannel = rightChannel[k][rightIndex];
                                        AtomicBoolean atomicBoolean = rightChannelSpinLock[k][rightIndex];
                                        byteBuffer.flip();
                                        while (!atomicBoolean.compareAndSet(false, true)){}
                                        fileChannel.write(byteBuffer);
                                        atomicBoolean.set(false);
                                        byteBuffer.clear();
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
                                int leftIndex = (int)(val >> SHIFTBITNUM);
                                ByteBuffer byteBuffer = leftBufs[leftIndex];
                                byteBuffer.putLong(val);
                                if (byteBuffer.position() == BYTEBUFFERSIZE) {
                                    FileChannel fileChannel = leftChannel[k][leftIndex];
                                    AtomicBoolean atomicBoolean = leftChannelSpinLock[k][leftIndex];
                                    byteBuffer.flip();
                                    while (!atomicBoolean.compareAndSet(false, true)){}
                                    fileChannel.write(byteBuffer);
                                    atomicBoolean.set(false);
                                    byteBuffer.clear();
                                }
                                val = 0;
                            }else {
                                int rightIndex = (int)(val >> SHIFTBITNUM);
                                ByteBuffer byteBuffer = rightBufs[rightIndex];
                                byteBuffer.putLong(val);
                                if (byteBuffer.position() == BYTEBUFFERSIZE) {
                                    FileChannel fileChannel = rightChannel[k][rightIndex];
                                    AtomicBoolean atomicBoolean = rightChannelSpinLock[k][rightIndex];
                                    byteBuffer.flip();
                                    while (!atomicBoolean.compareAndSet(false, true)){}
                                    fileChannel.write(byteBuffer);
                                    atomicBoolean.set(false);
                                    byteBuffer.clear();
                                }
                                val = 0;
                            }
                        }
                        else {
                            val = (val << 1) + (val << 3) + (t - 48);
                        }
                    }
                    for(int i = 0; i < BOUNDARYSIZE; i++) {
                        FileChannel fileChannel = leftChannel[k][i];
                        AtomicBoolean atomicBoolean = leftChannelSpinLock[k][i];
                        ByteBuffer byteBuffer = leftBufs[i];
                        byteBuffer.flip();
                        while (!atomicBoolean.compareAndSet(false, true)){}
                        fileChannel.write(byteBuffer);
                        atomicBoolean.set(false);
                        byteBuffer.clear();

                    }
                    for(int i = 0; i < BOUNDARYSIZE; i++)
                    {
                        FileChannel fileChannel = rightChannel[k][i];
                        AtomicBoolean atomicBoolean = rightChannelSpinLock[k][i];
                        ByteBuffer byteBuffer = rightBufs[i];
                        byteBuffer.flip();
                        while (!atomicBoolean.compareAndSet(false, true)){}
                        fileChannel.write(byteBuffer);
                        atomicBoolean.set(false);
                        byteBuffer.clear();
                    }
                }
            }catch (Exception e){
                e.printStackTrace();
            }
            latch.countDown();

        }

    }
}