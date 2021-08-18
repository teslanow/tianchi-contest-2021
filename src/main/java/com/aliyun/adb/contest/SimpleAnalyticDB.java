package com.aliyun.adb.contest;

import com.aliyun.adb.contest.spi.AnalyticDB;
import sun.misc.Unsafe;
import sun.nio.ch.DirectBuffer;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;


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
    private static final int BOUNDARYSIZE = 520;
    private static final int QUANTILE_DATA_SIZE = 16000000; //每次查询的data量，基本等于DATALENGTH / BOUNDARYSIZE * 8
    private static final int THREADNUM = 20;
    private static final long DATALENGTH = 1000000000;
    private static final int BYTEBUFFERSIZE = 1024 * 64;
    private static final int EACHREADSIZE = 1024 * 1024 * 16;
    private static final int TABLENUM = 2;
    private static final int COLNUM_EACHTABLE = 2;
    private static final int SHIFTBITNUM = 54;
    private static final int CONCURRENT_QUANTILE_THREADNUM = 8;

    private int current_Quantile_threadNUM = 0;
    private String[][] colName = new String[TABLENUM][COLNUM_EACHTABLE];
    private String[] tabName = new String[TABLENUM];
    private String curTableName;
    private Unsafe unsafe;
    private final int[][][] blockSize = new int[TABLENUM][COLNUM_EACHTABLE][BOUNDARYSIZE];
    private final int[][][] beginOrder = new int[TABLENUM][COLNUM_EACHTABLE][BOUNDARYSIZE];
    private long[] quantile_load_base = new long[CONCURRENT_QUANTILE_THREADNUM];
    private ByteBuffer[] quantile_load_buffer = new ByteBuffer[CONCURRENT_QUANTILE_THREADNUM];
    private long arrThreadId[] = new long[CONCURRENT_QUANTILE_THREADNUM];
    private static final CountDownLatch latch = new CountDownLatch(THREADNUM);
    //实验
    private long[][] allLeftWriteBeginAddress = new long[TABLENUM][BOUNDARYSIZE];
    private long[][] allRightWriteBeginAddress = new long[TABLENUM][BOUNDARYSIZE];
    private long[][] allLeftWriteAddress = new long[TABLENUM][BOUNDARYSIZE];
    private long[][] allRightWriteAddress = new long[TABLENUM][BOUNDARYSIZE];
    private MappedByteBuffer[][] allLeftWriteMapBuffer = new MappedByteBuffer[TABLENUM][BOUNDARYSIZE];
    private MappedByteBuffer[][] allRightWriteMapBuffer = new MappedByteBuffer[TABLENUM][BOUNDARYSIZE];
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
        ans = MyFind.quickFind(unsafe, byteBufferBase ,byteBufferBase + left_size - 8, ((long)rankDiff << 3)).toString();
        long e1 = System.currentTimeMillis();
//        System.out.println("one quantile time is " + (e1 - s1) + " percentile " + percentile + "rank "+ rank + " index " + index  + " table " + tabName[flag_table] + " column " + colName[flag_table][flag_colum]);
        //return "0";
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
                allLeftWriteMapBuffer[j][i] = Lrw.getChannel().map(FileChannel.MapMode.READ_WRITE, 0, QUANTILE_DATA_SIZE);
                allLeftWriteBeginAddress[j][i] = allLeftWriteAddress[j][i] = ((DirectBuffer)allLeftWriteMapBuffer[j][i]).address();
                allRightWriteMapBuffer[j][i] = Rrw.getChannel().map(FileChannel.MapMode.READ_WRITE, 0, QUANTILE_DATA_SIZE);
                allRightWriteBeginAddress[j][i] = allRightWriteAddress[j][i] = ((DirectBuffer)allRightWriteMapBuffer[j][i]).address();
                leftChannelSpinLock[j][i] = new AtomicBoolean();
                rightChannelSpinLock[j][i] = new AtomicBoolean();
            }
        }

        for(int i = 0; i < THREADNUM; i++)
        {
            new Thread(new ThreadTask(i, readStartEachThread[i], trueSizeOfMmapEachThread[i], allFileChannel)).start();
        }
        latch.await();
        for(int i = 0; i < TABLENUM; i++)
        {
            for(int j = 0; j < BOUNDARYSIZE; j++)
            {
                allLeftWriteMapBuffer[i][j].force();
            }
        }
        for(int i = 0; i < TABLENUM; i++)
        {
            for(int j = 0; j < BOUNDARYSIZE; j++)
            {
                allRightWriteMapBuffer[i][j].force();
            }
        }
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
                blockSize[j][0][i] = (int)(allLeftWriteAddress[j][i] - allLeftWriteBeginAddress[j][i]) >> 3;
                blockSize[j][1][i] = (int)(allRightWriteAddress[j][i] - allRightWriteBeginAddress[j][i]) >> 3;
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


    class ThreadTask implements Runnable {
        long[] readStart;
        long[] trueSizeOfMmap;
        int threadNo;
        long directBufferBase;
        ByteBuffer directBuffer;
        long[] leftWriteFileAddress, rightWriteFileAddress;
        long[] leftBegin, rightBegin;
        long[] leftSize, rightSize;
        FileChannel[] readChannel;
        //初始化
        public ThreadTask(int threadNo, long[] readStart ,long[] trueSizeOfMmap, FileChannel[] readChannel) throws Exception {
            this.threadNo = threadNo;
            this.readStart = readStart;
            this.trueSizeOfMmap = trueSizeOfMmap;
            this.readChannel = readChannel;
        }

        @Override
        public void run() {
            this.leftBegin = new long[BOUNDARYSIZE];
            this.rightBegin = new long[BOUNDARYSIZE];
            this.leftSize = new long[BOUNDARYSIZE];
            this.rightSize = new long[BOUNDARYSIZE];
            this.directBuffer = ByteBuffer.allocateDirect(EACHREADSIZE);
            this.directBufferBase = ((DirectBuffer)directBuffer).address();
            long allBegin = unsafe.allocateMemory(2 * BOUNDARYSIZE * BYTEBUFFERSIZE);
            for (int i = 0; i < BOUNDARYSIZE; i++) {
                leftBegin[i] = allBegin;
                allBegin += BYTEBUFFERSIZE;
            }
            for (int i = 0; i < BOUNDARYSIZE; i++) {
                rightBegin[i] = allBegin;
                allBegin += BYTEBUFFERSIZE;
            }
            AtomicBoolean[] curLeftSpinLock, curRightSpinLock;
            try{
                for(int k = 0; k < TABLENUM; k++)
                {
                    leftWriteFileAddress = allLeftWriteAddress[k];
                    rightWriteFileAddress = allRightWriteAddress[k];
                    curLeftSpinLock = leftChannelSpinLock[k];
                    curRightSpinLock = leftChannelSpinLock[k];
                    curTableName = tabName[k];
                    long nowRead = 0, realRead, yuzhi = trueSizeOfMmap[k] - EACHREADSIZE;
                    long curReadStart = readStart[k];
                    while(nowRead < yuzhi) {
                        realRead = EACHREADSIZE;
                        directBuffer.clear();
                        readChannel[k].read(directBuffer, curReadStart + nowRead);
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
                                    long beginAddress = leftBegin[leftIndex];
                                    long size = leftSize[leftIndex];
                                    unsafe.putLong(null, beginAddress + size, val);
                                    size += 8;
                                    if (size == BYTEBUFFERSIZE) {
                                        AtomicBoolean spinLock = curLeftSpinLock[leftIndex];
                                        while (!spinLock.compareAndSet(false, true)){}
                                        unsafe.copyMemory(null, beginAddress, null, leftWriteFileAddress[leftIndex], BYTEBUFFERSIZE);
                                        leftWriteFileAddress[leftIndex] += BYTEBUFFERSIZE;
                                        spinLock.set(false);
                                        size = 0;
                                    }
                                    leftSize[leftIndex] = size;
                                    val = 0;
                                }else {
                                    int rightIndex = (int)(val >> SHIFTBITNUM);
                                    long beginAddress = rightBegin[rightIndex];
                                    long size = rightSize[rightIndex];
                                    unsafe.putLong(null, beginAddress + size, val);
                                    size += 8;
                                    if (size == BYTEBUFFERSIZE) {
                                        AtomicBoolean spinLock = curRightSpinLock[rightIndex];
                                        while (!spinLock.compareAndSet(false, true)){}
                                        unsafe.copyMemory(null, beginAddress, null, rightWriteFileAddress[rightIndex], BYTEBUFFERSIZE);
                                        rightWriteFileAddress[rightIndex] += BYTEBUFFERSIZE;
                                        spinLock.set(false);
                                        size = 0;
                                    }
                                    rightSize[rightIndex] = size;
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
                    readChannel[k].read(directBuffer, curReadStart + nowRead);
                    long val = 0;
                    byte t;
                    long curPos = directBufferBase;
                    long endPos = directBufferBase + realRead;
                    for(; curPos < endPos; curPos++) {
                        t = unsafe.getByte(curPos);
                        if((t & 16) == 0) {
                            if(t == 44) {
                                int leftIndex = (int)(val >> SHIFTBITNUM);
                                long beginAddress = leftBegin[leftIndex];
                                long size = leftSize[leftIndex];
                                unsafe.putLong(null, beginAddress + size, val);
                                size += 8;
                                if (size == BYTEBUFFERSIZE) {
                                    AtomicBoolean spinLock = curLeftSpinLock[leftIndex];
                                    while (!spinLock.compareAndSet(false, true)){}
                                    unsafe.copyMemory(null, beginAddress, null, leftWriteFileAddress[leftIndex], BYTEBUFFERSIZE);
                                    leftWriteFileAddress[leftIndex] += BYTEBUFFERSIZE;
                                    spinLock.set(false);
                                    size = 0;
                                }
                                leftSize[leftIndex] = size;
                                val = 0;
                            }else {
                                int rightIndex = (int)(val >> SHIFTBITNUM);
                                long beginAddress = rightBegin[rightIndex];
                                long size = rightSize[rightIndex];
                                unsafe.putLong(null, beginAddress + size, val);
                                size += 8;
                                if (size == BYTEBUFFERSIZE) {
                                    AtomicBoolean spinLock = curRightSpinLock[rightIndex];
                                    while (!spinLock.compareAndSet(false, true)){}
                                    unsafe.copyMemory(null, beginAddress, null, rightWriteFileAddress[rightIndex], BYTEBUFFERSIZE);
                                    rightWriteFileAddress[rightIndex] += BYTEBUFFERSIZE;
                                    spinLock.set(false);
                                    size = 0;
                                }
                                rightSize[rightIndex] = size;
                                val = 0;
                            }
                        }
                        else {
                            val = (val << 1) + (val << 3) + (t - 48);
                        }
                    }
                    for(int i = 0; i < BOUNDARYSIZE; i++) {
                        AtomicBoolean spinLock = curLeftSpinLock[i];
                        while (!spinLock.compareAndSet(false, true)){}
                        unsafe.copyMemory(null, leftBegin[i], null, leftWriteFileAddress[i], leftSize[i]);
                        leftWriteFileAddress[i] += leftSize[i];
                        spinLock.set(false);
                        leftSize[i] = 0;
                    }
                    for(int i = 0; i < BOUNDARYSIZE; i++)
                    {
                        AtomicBoolean spinLock = curRightSpinLock[i];
                        while (!spinLock.compareAndSet(false, true)){}
                        unsafe.copyMemory(null, rightBegin[i], null, rightWriteFileAddress[i], rightSize[i]);
                        rightWriteFileAddress[i] += rightSize[i];
                        spinLock.set(false);
                        rightSize[i] = 0;
                    }
                }
            }catch (Exception e){
                e.printStackTrace();
            }
            latch.countDown();

        }

    }
}