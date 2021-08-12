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
import java.util.Arrays;
import java.util.Date;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

public class SimpleAnalyticDB implements AnalyticDB {
    private Random random = new Random();
    private static final int PRODUCERTHREAD = 4;
    private static final int ALLREADEREND = (1 << PRODUCERTHREAD) - 1;
    private static final int AVALIABLE_BUFFERNUM = 10;
    private AtomicInteger producerFlag = new AtomicInteger();
    //提交需改
    private static final int BOUNDARYSIZE = 1040;
    private static final int QUANTILE_DATA_SIZE = 16000000; //每次查询的data量，基本等于DATALENGTH / BOUNDARYSIZE * 8
    private static final int THREADNUM = 20;
    private static final long DATALENGTH = 1000000000;
    private static final long FILE_SIZE = 1000000;
    private static final int BYTEBUFFERSIZE = 1024 * 64;
    private static final int EACHREADSIZE = 1024 * 1024 * 16;
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
    private static final CountDownLatch latch = new CountDownLatch(THREADNUM);

    //实验
    private final FileChannel[][][] allChannel = new FileChannel[TABLENUM][COLNUM_EACHTABLE][BOUNDARYSIZE];
    private final AtomicBoolean[][][] allChannelSpinLock = new AtomicBoolean[TABLENUM][COLNUM_EACHTABLE][BOUNDARYSIZE];
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
//        return "0";
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
        long[][] readStartEachThread = new long[PRODUCERTHREAD][TABLENUM];
        long[][] trueSizeOfMmapEachThread = new long[PRODUCERTHREAD][TABLENUM];
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
            long sizePerBuffer = size / PRODUCERTHREAD;
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


            for(int i = 0; i < PRODUCERTHREAD; i++)
            {
                if(i == PRODUCERTHREAD - 1)
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
        String outDir;
        File outFile;
        RandomAccessFile randFile;
        for(int i = 0; i < TABLENUM; i++)
        {
            for(int j = 0; j < COLNUM_EACHTABLE; j++)
            {
                for(int k = 0; k < BOUNDARYSIZE; k++)
                {
                    outDir = workDir + "/" + tabName[i] + "-" + colName[i][j] + "-"  +  k;
                    outFile = new File(outDir);
                    randFile = new RandomAccessFile(outFile, "rw");
                    randFile.setLength(FILE_SIZE);
                    allChannel[i][j][k] = randFile.getChannel();
                    allChannelSpinLock[i][j][k] = new AtomicBoolean(false);
                }
            }
        }
        LinkedBlockingDeque<Tuple_3> fullQueue = new LinkedBlockingDeque<>(20000);
        LinkedBlockingDeque<Tuple_3> emptyQueue = new LinkedBlockingDeque<>(20000);
        for(int i = 0; i < PRODUCERTHREAD; i++)
        {
            new Thread(new ProducerTask(i, readStartEachThread[i], trueSizeOfMmapEachThread[i], allFileChannel, fullQueue, emptyQueue)).start();
        }
        for(int i = 0; i < THREADNUM; i++)
        {
            new Thread(new ConsumerTask(i, fullQueue, emptyQueue)).start();
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
        for(int i = 0; i < TABLENUM ; i++)
        {
            for(int j = 0; j < COLNUM_EACHTABLE; j++)
            {
                for(int k = 0; k < BOUNDARYSIZE; k++)
                {
                    blockSize[i][j][k] = (int)(allChannel[i][j][k].position() >> 3);
                }
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


    class ConsumerTask implements Runnable{
        LinkedBlockingDeque<Tuple_3> fullQueue, emptyQueue;
        ByteBuffer[][][] allBufs = new ByteBuffer[TABLENUM][COLNUM_EACHTABLE][BOUNDARYSIZE];
        int threadNo;
        ConsumerTask(int threadNo, LinkedBlockingDeque<Tuple_3> fullQueue, LinkedBlockingDeque<Tuple_3> emptyQueue  )
        {
            this.threadNo = threadNo;
            this.fullQueue = fullQueue;
            this.emptyQueue = emptyQueue;
        }
        @Override
        public void run() {

            for(int i = 0; i < TABLENUM; i++)
            {
                for(int j = 0; j < COLNUM_EACHTABLE; j++)
                {
                    for(int k = 0; k < BOUNDARYSIZE; k++)
                    {
                        allBufs[i][j][k] = ByteBuffer.allocateDirect(BYTEBUFFERSIZE);
                        allBufs[i][j][k].order(ByteOrder.LITTLE_ENDIAN);
                    }
                }
            }
            Tuple_3 ava_tuple = null, used_tuple = null;
            try {
                FileChannel ff = new RandomAccessFile(new File("" + threadNo), "rw" ).getChannel();
                while (true)
                {
                    used_tuple = ava_tuple;
                    if(used_tuple != null)
                        emptyQueue.put(used_tuple);
                    ava_tuple = fullQueue.take();
                    //System.out.println("Consumer thread " + threadNo  +  "get at " + new SimpleDateFormat("mm:ss").format(new Date(System.currentTimeMillis())));
                    if(ava_tuple.val3 == null) {
                        System.out.println("Consumer thread " + threadNo  +  "come here  at " + new SimpleDateFormat("mm:ss").format(new Date(System.currentTimeMillis())));
                        for (int i = 0; i < TABLENUM; i++) {
                            for (int j = 0; j < COLNUM_EACHTABLE; j++) {
                                int start_index = threadNo * 52;
                                for (int k = 0; k < BOUNDARYSIZE; k++) {
                                    ByteBuffer b = allBufs[i][j][start_index];
                                    b.flip();
                                    ff.write(b);
                                }
                            }
                        }
                        latch.countDown();
                        return;
                    }
                    //System.out.println("Consumer thread " + threadNo  +  "start to run at  " + new SimpleDateFormat("mm:ss").format(new Date(System.currentTimeMillis())));
                    long val = 0;
                    long position;
                    byte t;
                    long curPos = ava_tuple.val2;
                    long endPos = curPos + ava_tuple.val3.limit();
                    int table_i = ava_tuple.val1;
                    int col_i = 0;
                    for(; curPos < endPos; curPos++) {
                        t = unsafe.getByte(curPos);
                        if((t & 16) == 0) {
                            int block_i = (int)(val >> SHIFTBITNUM);
                            ByteBuffer byteBuffer = allBufs[table_i][col_i][block_i];
                            byteBuffer.putLong(val);
                            position = byteBuffer.position();
                            if(position >= BYTEBUFFERSIZE)
                            {
                                FileChannel fileChannel = allChannel[table_i][col_i][block_i];
                                AtomicBoolean atomicBoolean = allChannelSpinLock[table_i][col_i][block_i];
                                byteBuffer.flip();
                                while (!atomicBoolean.compareAndSet(false, true)){}
                                fileChannel.write(byteBuffer);
                                atomicBoolean.set(false);
                                byteBuffer.clear();
                                col_i ^= 0x1;
                            }
                            val = 0;
                        }
                        else {
                            val = (val << 3) + (val << 1) + (t - 48);
                        }
                    }
                }
            } catch (InterruptedException e) {
                e.printStackTrace();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }
    class ProducerTask implements Runnable
    {
        int threadNo;
        long[] readStart;
        long[] trueSizeOfMmap;
        FileChannel[] fileChannel;
        LinkedBlockingDeque<Tuple_3> fullQueue;
        LinkedBlockingDeque<Tuple_3> emptyQueue;
        ProducerTask(int threadNo, long[] readStart , long[] trueSizeOfMmap, FileChannel[] fileChannel, LinkedBlockingDeque<Tuple_3> fullQueue, LinkedBlockingDeque<Tuple_3> emptyQueue)
        {
            this.readStart = readStart;
            this.trueSizeOfMmap = trueSizeOfMmap;
            this.fileChannel = fileChannel;
            this.threadNo = threadNo;
            this.fullQueue = fullQueue;
            this.emptyQueue = emptyQueue;
        }
        @Override
        public void run() {
            try {
                for(int i = 0; i < AVALIABLE_BUFFERNUM ; i++)
                {
                    ByteBuffer directBuffer = ByteBuffer.allocateDirect(EACHREADSIZE);
                    long directBufferBase = ((DirectBuffer)directBuffer).address();
                    emptyQueue.put(new Tuple_3(0, directBufferBase, directBuffer));
                }
                for(int k = 0; k < TABLENUM; k++)
                {
                    long nowRead = 0, realRead, yuzhi = trueSizeOfMmap[k] - EACHREADSIZE;
                    long curReadStart = readStart[k];
                    while(nowRead < yuzhi) {
                        realRead = EACHREADSIZE;
                        Tuple_3 tuple_3 = emptyQueue.take();
                        //System.out.println("Thread " + threadNo + " read at " + new SimpleDateFormat("mm:ss").format(new Date(System.currentTimeMillis())));
                        ByteBuffer directBuffer = tuple_3.val3;
                        long directBufferBase = tuple_3.val2;
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
                        directBuffer.limit((int)realRead);
                        tuple_3.val1 = k;
                        fullQueue.put(tuple_3);
                    }
                    realRead = trueSizeOfMmap[k] - nowRead;
                    Tuple_3 tuple_3 = emptyQueue.take();
                    ByteBuffer directBuffer = tuple_3.val3;
                    directBuffer.clear();
                    fileChannel[k].read(directBuffer, curReadStart + nowRead);
                    directBuffer.limit((int)realRead);
                    tuple_3.val1 = k;
                    fullQueue.put(tuple_3);
                }
                if(producerFlag.addAndGet(1 << threadNo) == ALLREADEREND)
                {
                    for(int i = 0; i < THREADNUM; i++)
                    {
                        fullQueue.put(new Tuple_3(0, 0, null));
                    }
                }
            } catch (InterruptedException e) {
                e.printStackTrace();
            } catch (IOException e) {
                e.printStackTrace();
            }


        }
    }

}