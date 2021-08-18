package com.aliyun.adb.contest;

import com.aliyun.adb.contest.spi.AnalyticDB;
import sun.misc.Unsafe;
import sun.nio.ch.DirectBuffer;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

public class SimpleAnalyticDB implements AnalyticDB {

    //提交需改
    private static final long DATAIN_EACHBLOCKTHREAD = 400000; //每个线程处理的每个块的数据量（字节单位）
    private static final int BOUNDARYSIZE = 1040;
    private static final int QUANTILE_DATA_SIZE = 16000000; //每次查询的data量，基本等于DATALENGTH / BOUNDARYSIZE * 8
    private static final int THREADNUM = 16;
    private static final int WRITETHREAD = 16;
    private static AtomicInteger endFlag = new AtomicInteger();
    private static final int ALLEND = (1 << THREADNUM) - 1;
    private static final long DATALENGTH = 1000000000;
    private static final long FILE_SIZE = 1000000;
    private static final int BYTEBUFFERSIZE = 4 * 1024;
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
    private AtomicBoolean[][] leftChannelSpinLock = new AtomicBoolean[TABLENUM][BOUNDARYSIZE];
    private AtomicBoolean[][] rightChannelSpinLock = new AtomicBoolean[TABLENUM][BOUNDARYSIZE];
    private MappedByteBuffer[][] leftWriteBuffer = new MappedByteBuffer[TABLENUM][BOUNDARYSIZE];
    private MappedByteBuffer[][] rightWriteBuffer = new MappedByteBuffer[TABLENUM][BOUNDARYSIZE];
    private long[][] leftWriteAddress = new long[TABLENUM][BOUNDARYSIZE];
    private long[][] rightWriteAddress = new long[TABLENUM][BOUNDARYSIZE];
    private long[][] leftWriteBeginAddress = new long[TABLENUM][BOUNDARYSIZE];
    private long[][] rightWriteBeginAddress = new long[TABLENUM][BOUNDARYSIZE];
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
                leftWriteBuffer[j][i] = Lrw.getChannel().map(FileChannel.MapMode.READ_WRITE, 0, QUANTILE_DATA_SIZE);
                leftWriteBeginAddress[j][i] = leftWriteAddress[j][i] = ((DirectBuffer)leftWriteBuffer[j][i]).address();
                rightWriteBuffer[j][i] = Rrw.getChannel().map(FileChannel.MapMode.READ_WRITE, 0, QUANTILE_DATA_SIZE);
                rightWriteBeginAddress[j][i] = rightWriteAddress[j][i] = ((DirectBuffer)rightWriteBuffer[j][i]).address();
                leftChannelSpinLock[j][i] = new AtomicBoolean(false);
                rightChannelSpinLock[j][i] = new AtomicBoolean(false);
            }
        }
        LinkedBlockingDeque<Tuple5> fullQueue = new LinkedBlockingDeque<>();
        LinkedBlockingDeque<Tuple5> emptyQueue = new LinkedBlockingDeque<>();
        long eachThreadSize =  COLNUM_EACHTABLE * BOUNDARYSIZE * BYTEBUFFERSIZE;
        for(int i = 0; i < 2; i++)
        {
            long base = unsafe.allocateMemory(1024 * 1024 * 1024);
            if (base == 0)
            {
                System.out.println("not allocated ");
                return;
            }
            for(int j = 0; j < 262144; j++)
            {
                emptyQueue.put(new Tuple5(0, 0, 0, base, 0));
                base += BYTEBUFFERSIZE;
            }
        }
        for(int i = 0; i < THREADNUM; i++)
        {
            long base = unsafe.allocateMemory(eachThreadSize);
            if (base == 0)
            {
                System.out.println("not allocated ");
                return;
            }
            new Thread(new ProducerThread(i, readStartEachThread[i],trueSizeOfMmapEachThread[i], allFileChannel, fullQueue, emptyQueue, base )).start();
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
        for(int i = 0; i < TABLENUM; i++)
        {
            for(int j = 0; j < BOUNDARYSIZE; j++)
            {
                leftWriteBuffer[i][j].force();
                blockSize[i][0][j] = (int)((leftWriteAddress[i][j] - leftWriteBeginAddress[i][j]) >> 3);
                rightWriteBuffer[i][j].force();
                blockSize[i][1][j] = (int)((rightWriteAddress[i][j] - rightWriteBeginAddress[i][j]) >> 3);
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
        Tuple5[][] allBufs = new Tuple5[COLNUM_EACHTABLE][BOUNDARYSIZE];
        Tuple5[] leftBufs;
        Tuple5[] rightBufs;
        LinkedBlockingDeque<Tuple5> fullQueue, emptyQueue;
        long bufferstart;
        ProducerThread(int threadNo, long[] readStart , long[] trueSizeOfMmap, FileChannel[] fileChannel, LinkedBlockingDeque<Tuple5> fullQueue,
                       LinkedBlockingDeque<Tuple5> emptyQueue, long bufferstart)
        {
            this.threadNo = threadNo;
            this.readStart = readStart;
            this.trueSizeOfMmap = trueSizeOfMmap;
            this.fileChannel = fileChannel;
            this.fullQueue = fullQueue;
            this.emptyQueue = emptyQueue;
            this.bufferstart = bufferstart;
        }
        @Override
        public void run() {
            this.directBuffer = ByteBuffer.allocateDirect(EACHREADSIZE);
            this.directBufferBase = ((DirectBuffer)directBuffer).address();
            for(int j = 0; j < COLNUM_EACHTABLE; j++)
            {
                for(int k = 0; k < BOUNDARYSIZE; k++)
                {
                    Tuple5 t = new Tuple5(0, 0, 0, bufferstart, 0);
                    bufferstart += BYTEBUFFERSIZE;
                    allBufs[j][k] = t;
                }
            }
            leftBufs = allBufs[0];
            rightBufs = allBufs[1];
            try{
                for(int k = 0; k < TABLENUM; k++)
                {
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
                                    if(leftBufs[leftIndex] == null)
                                    {
                                        leftBufs[leftIndex] = emptyQueue.take();
                                    }
                                    Tuple5 tuple = leftBufs[leftIndex];
                                    long address = tuple.address;
                                    long size = tuple.size;
                                    //System.out.println("size " + size + " address " + address);
                                    unsafe.putLong(null, address + size, val);
                                    tuple.size += 8;
                                    if (tuple.size == BYTEBUFFERSIZE) {
                                        //填入
                                        tuple.setIndex(k, 0, leftIndex);
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
                                    Tuple5 tuple = rightBufs[rightIndex];
                                    long address = tuple.address;
                                    long size = tuple.size;
                                    //System.out.println("size " + size + " address " + address);
                                    unsafe.putLong(null, address + size, val);
                                    tuple.size += 8;
                                    if (tuple.size == BYTEBUFFERSIZE) {
                                        //填入
                                        tuple.setIndex(k, 1, rightIndex);
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
                                Tuple5 tuple = leftBufs[leftIndex];
                                long address = tuple.address;
                                long size = tuple.size;
                                //System.out.println("size " + size + " address " + address);
                                unsafe.putLong(null, address + size, val);
                                tuple.size += 8;
                                if (tuple.size == BYTEBUFFERSIZE) {
                                    //填入
                                    tuple.setIndex(k, 0, leftIndex);
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
                                Tuple5 tuple = rightBufs[rightIndex];
                                long address = tuple.address;
                                long size = tuple.size;
                                //System.out.println("size " + size + " address " + address);
                                unsafe.putLong(null, address + size, val);
                                tuple.size += 8;
                                if (tuple.size == BYTEBUFFERSIZE) {
                                    //填入
                                    tuple.setIndex(k, 1, rightIndex);
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
                        Tuple5 tuple = leftBufs[i];
                        if(tuple == null)
                            continue;
                        long size = tuple.size;
                        if(size == 0)
                            continue;
                        else
                        {
                            tuple.setIndex(k, 0, i);
                            fullQueue.put(tuple);
                            leftBufs[i] = null;
                        }
                    }
                    for(int i = 0; i < BOUNDARYSIZE; i++)
                    {
                        Tuple5 tuple = rightBufs[i];
                        if(tuple == null)
                            continue;
                        long size = tuple.size;
                        if(size == 0)
                            continue;
                        else
                        {
                            tuple.setIndex(k, 1, i);
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
                        fullQueue.put(new Tuple5(0, 0, 0, -1, 0));
                    }
                }

            }catch (Exception e){
                e.printStackTrace();
            }


        }
    }
    class ConsumerThread implements Runnable{
        LinkedBlockingDeque<Tuple5> fullQueue, emptyQueue;
        int threadNo;
        ConsumerThread(int threadNo, LinkedBlockingDeque<Tuple5> fullQueue, LinkedBlockingDeque<Tuple5> emptyQueue)
        {
            this.threadNo = threadNo;
            this.fullQueue = fullQueue;
            this.emptyQueue = emptyQueue;
        }
        @Override
        public void run() {
            long writeTime = 0;
            try {
                while (true)
                {
                    Tuple5 id = fullQueue.take();

                    if(id.address == -1)
                    {
                        System.out.println("Consumer Thread " + threadNo + " Write time " + writeTime + " total times ");
                        latch.countDown();
                        return;
                    }
                    else
                    {
                        AtomicBoolean atomicBoolean;
                        if(id.col_index == 0)
                        {
                            atomicBoolean = leftChannelSpinLock[id.table_index][id.bound_index];

                            while (!atomicBoolean.compareAndSet(false, true)){}
                            long s = System.currentTimeMillis();
                            unsafe.copyMemory(null, id.address, null, leftWriteAddress[id.table_index][id.bound_index], id.size);
                            leftWriteAddress[id.table_index][id.bound_index] += id.size;
                            atomicBoolean.set(false);
                            id.size = 0;
                            long e = System.currentTimeMillis();
                            writeTime += (e - s);
                            emptyQueue.put(id);
                        }
                        else {
                            atomicBoolean = rightChannelSpinLock[id.table_index][id.bound_index];

                            while (!atomicBoolean.compareAndSet(false, true)){}
                            long s = System.currentTimeMillis();
                            unsafe.copyMemory(null, id.address, null, rightWriteAddress[id.table_index][id.bound_index], id.size);
                            rightWriteAddress[id.table_index][id.bound_index] += id.size;
                            atomicBoolean.set(false);
                            id.size = 0;
                            long e = System.currentTimeMillis();
                            writeTime += (e - s);
                            emptyQueue.put(id);
                        }

                    }
                }
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }
}