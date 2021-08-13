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
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

public class SimpleAnalyticDB implements AnalyticDB {

    //提交需改
    private AtomicInteger producerFlag = new AtomicInteger(0);
    private CyclicBarrier cyclicBarrier = new CyclicBarrier(Constant.PRODUCER_NUM);
    private int current_Quantile_threadNUM = 0;
    private String[][] colName = new String[Constant.TABLENUM][Constant.COLNUM_EACHTABLE];
    private String[] tabName = new String[Constant.TABLENUM];
    private String curTableName;
    private Unsafe unsafe;
    private final int[][][] blockSize = new int[Constant.TABLENUM][Constant.COLNUM_EACHTABLE][Constant.BOUNDARYSIZE];
    private final int[][][] beginOrder = new int[Constant.TABLENUM][Constant.COLNUM_EACHTABLE][Constant.BOUNDARYSIZE];
    private long[] quantile_load_base = new long[Constant.CONCURRENT_QUANTILE_THREADNUM];
    private ByteBuffer[] quantile_load_buffer = new ByteBuffer[Constant.CONCURRENT_QUANTILE_THREADNUM];
    private long arrThreadId[] = new long[Constant.CONCURRENT_QUANTILE_THREADNUM];
    private static final CountDownLatch latch = new CountDownLatch(Constant.THREADNUM);

    //实验
    private FileChannel[][] leftChannel = new FileChannel[Constant.TABLENUM][Constant.BOUNDARYSIZE];
    private FileChannel[][] rightChannel = new FileChannel[Constant.TABLENUM][Constant.BOUNDARYSIZE];
    private AtomicBoolean[][] leftChannelSpinLock = new AtomicBoolean[Constant.TABLENUM][Constant.BOUNDARYSIZE];
    private AtomicBoolean[][] rightChannelSpinLock = new AtomicBoolean[Constant.TABLENUM][Constant.BOUNDARYSIZE];
    private  String workDir;

    public SimpleAnalyticDB() throws NoSuchFieldException, IllegalAccessException {
        this.unsafe = GetUnsafe.getUnsafe();
        this.quantile_load_buffer[0] = ByteBuffer.allocateDirect(Constant.QUANTILE_DATA_SIZE);
        this.quantile_load_base[0] = ((DirectBuffer)quantile_load_buffer[0]).address();
    }

    @Override
    public void load(String tpchDataFileDir, String workspaceDir) throws Exception {
        long ss = System.currentTimeMillis();
        workDir = workspaceDir;
        //判断工作区是否为空
//        if(new File(workspaceDir + "/index").exists())
//        {
//            for(int i = 1; i < Constant.CONCURRENT_QUANTILE_THREADNUM; i++)
//            {
//                quantile_load_buffer[i] = ByteBuffer.allocateDirect(Constant.QUANTILE_DATA_SIZE);
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
//            String[] tmpString = new String[Constant.TABLENUM * Constant.COLNUM_EACHTABLE + Constant.TABLENUM];
//            for(int pre = 0, index = 0;;)
//            {
//                if(bytes[curPos] == 10)
//                {
//                    tmpString[index++] = new String(bytes, pre, curPos - pre, "UTF-8");
//                    if(index >= Constant.TABLENUM * Constant.COLNUM_EACHTABLE + Constant.TABLENUM)
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
//            for(int i = 0; i < Constant.TABLENUM; i++)
//            {
//                tabName[i] = tmpString[index_name++];
//                for(int j = 0; j < Constant.COLNUM_EACHTABLE; j++)
//                {
//                    colName[i][j] = tmpString[index_name++];
//                    for( int k = 0; k < Constant.BOUNDARYSIZE; k++)
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
        if(current_Quantile_threadNUM < Constant.CONCURRENT_QUANTILE_THREADNUM)
        {
            synchronized (arrThreadId)
            {
                for(int i = 0; i < Constant.CONCURRENT_QUANTILE_THREADNUM; i++)
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
            for(int i = 0; i < Constant.CONCURRENT_QUANTILE_THREADNUM; i++)
            {
                if(arrThreadId[i] == Thread.currentThread().getId())
                {
                    buffer_index = i;
                    break;
                }
            }
        }
        //System.out.println("thread " + Thread.currentThread().getId() + " current_threadnum " + current_Quantile_threadNUM + " buffer_id " + buffer_index );
        int rank = (int) Math.round(Constant.DATALENGTH * percentile);
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
//        System.out.println("BOUNDARAY " + Constant.BOUNDARYSIZE);
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
        for(int j = 0; j < Constant.TABLENUM; j++)
        {
            for (int i = 0; i < Constant.BOUNDARYSIZE; i++){
                beginOrder[j][0][i] = 0;
                beginOrder[j][1][i] = 0;
            }
        }
        long[][] readStartEachThread = new long[Constant.PRODUCER_NUM][Constant.TABLENUM];
        long[][] trueSizeOfMmapEachThread = new long[Constant.PRODUCER_NUM][Constant.TABLENUM];
        FileChannel[] allFileChannel = new FileChannel[Constant.TABLENUM];
        byte[] bytes = new byte[42];
        ByteBuffer byteBuffer = ByteBuffer.wrap(bytes);
        for(int k = 0; k < dataFileList.length; k++)
        {
            File dataFile = dataFileList[k];
            RandomAccessFile fis = new RandomAccessFile(dataFile, "r");
            FileChannel channel = fis.getChannel();
            allFileChannel[k] = channel;
            long size = channel.size();
            long sizePerBuffer = size / Constant.PRODUCER_NUM;
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


            for(int i = 0; i < Constant.PRODUCER_NUM; i++)
            {
                if(i == Constant.PRODUCER_NUM - 1)
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
        for(int j = 0; j < Constant.TABLENUM; j++)
        {
            for(int i = 0; i < Constant.BOUNDARYSIZE; i++)
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
        LinkedBlockingDeque<Tuple_3> fullQueue = new LinkedBlockingDeque<>(20000);
        LinkedBlockingDeque<Tuple_3> emptyQueue = new LinkedBlockingDeque<>(20000);
        for(int i = 0; i < Constant.PRODUCER_NUM; i++)
        {
            new Thread(new ProducerTask(i, readStartEachThread[i], trueSizeOfMmapEachThread[i], allFileChannel, fullQueue, emptyQueue)).start();
        }
        for(int i = 0; i < Constant.THREADNUM; i++)
        {
            new Thread(new ConsumerTask(i, fullQueue, emptyQueue)).start();
        }
        latch.await();

        StringBuilder builder= new StringBuilder(workDir + "/index");
        FileChannel fileChannel = new RandomAccessFile(new File(builder.toString()), "rw").getChannel();
        for(int i = 0; i < Constant.TABLENUM; i++)
        {
            fileChannel.write(ByteBuffer.wrap(tabName[i].getBytes(StandardCharsets.UTF_8)));
            fileChannel.write(ByteBuffer.wrap("\n".getBytes(StandardCharsets.UTF_8)));
            for(int j = 0; j < Constant.COLNUM_EACHTABLE; j++)
            {
                fileChannel.write(ByteBuffer.wrap(colName[i][j].getBytes(StandardCharsets.UTF_8)));
                fileChannel.write(ByteBuffer.wrap("\n".getBytes(StandardCharsets.UTF_8)));
            }
        }
        for(int j = 0; j < Constant.TABLENUM; j++)
        {
            int  lBry = 0, rBry = 0;
            for (int i = 0; i < Constant.BOUNDARYSIZE; i++){
                blockSize[j][0][i] = (int)leftChannel[j][i].position() >> 3;
                blockSize[j][1][i] = (int)rightChannel[j][i].position() >> 3;
                beginOrder[j][0][i] = lBry + 1;
                lBry += blockSize[j][0][i];
                beginOrder[j][1][i] = rBry + 1;
                rBry += blockSize[j][1][i];
            }
        }
        byte[] b_t = new byte[Constant.BOUNDARYSIZE * 4];
        ByteBuffer b_t_buffer = ByteBuffer.wrap(b_t);
        //b_t_buffer.order(ByteOrder.LITTLE_ENDIAN);
        for(int i = 0; i < Constant.TABLENUM; i++)
        {
            for(int j = 0; j < Constant.COLNUM_EACHTABLE; j++)
            {
                b_t_buffer.clear();
                for(int k = 0; k < Constant.BOUNDARYSIZE; k++)
                {
                    b_t_buffer.putInt(beginOrder[i][j][k]);
                }
                b_t_buffer.flip();
                fileChannel.write(b_t_buffer);
            }
        }
        System.out.println();
        System.out.println("table 0 " + ( beginOrder[0][0][Constant.BOUNDARYSIZE - 1] - 1 )  + " " + ( beginOrder[0][1][Constant.BOUNDARYSIZE - 1] - 1) );
        System.out.println("table 1 " + ( beginOrder[1][0][Constant.BOUNDARYSIZE - 1] - 1 )  + " " + ( beginOrder[1][1][Constant.BOUNDARYSIZE - 1] - 1) );
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
                for(int i = 0; i < Constant.AVALIABLE_BUFFERNUM ; i++)
                {
                    ByteBuffer directBuffer = ByteBuffer.allocateDirect(Constant.EACHREADSIZE);
                    long directBufferBase = ((DirectBuffer)directBuffer).address();
                    emptyQueue.put(new Tuple_3(0, directBufferBase, directBuffer));
                }
                for(int k = 0; k < Constant.TABLENUM; k++)
                {
                    long nowRead = 0, realRead, yuzhi = trueSizeOfMmap[k] - Constant.EACHREADSIZE;
                    long curReadStart = readStart[k];
                    while(nowRead < yuzhi) {
                        realRead = Constant.EACHREADSIZE;
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
                    if(k == 0)
                        cyclicBarrier.await();
                }
                if(producerFlag.addAndGet(1 << threadNo) == Constant.ALLREADEREND)
                {
                    for(int i = 0; i < Constant.THREADNUM; i++)
                    {
                        fullQueue.put(new Tuple_3(0, 0, null));
                    }
                }
            } catch (InterruptedException e) {
                e.printStackTrace();
            } catch (IOException e) {
                e.printStackTrace();
            } catch (BrokenBarrierException e) {
                e.printStackTrace();
            }


        }
    }
    class ConsumerTask implements Runnable{
        LinkedBlockingDeque<Tuple_3> fullQueue, emptyQueue;
        ByteBuffer[] leftBufs = new ByteBuffer[Constant.BOUNDARYSIZE];
        ByteBuffer[] rightBufs = new ByteBuffer[Constant.BOUNDARYSIZE];
        int threadNo;
        ConsumerTask(int threadNo, LinkedBlockingDeque<Tuple_3> fullQueue, LinkedBlockingDeque<Tuple_3> emptyQueue  )
        {
            this.threadNo = threadNo;
            this.fullQueue = fullQueue;
            this.emptyQueue = emptyQueue;
        }
        @Override
        public void run() {
            for(int i = 0; i < Constant.BOUNDARYSIZE; i++)
            {
                leftBufs[i] = ByteBuffer.allocateDirect(Constant.BYTEBUFFERSIZE);
                rightBufs[i] = ByteBuffer.allocateDirect(Constant.BYTEBUFFERSIZE);
            }
            Tuple_3 ava_tuple = null, used_tuple = null;
            try {
                while (true)
                {
                    used_tuple = ava_tuple;
                    if(used_tuple != null)
                        emptyQueue.put(used_tuple);
                    ava_tuple = fullQueue.take();
                    //进入第2个表
                    if(ava_tuple.val1 == 1)
                    {
                        for(int i = 0; i < Constant.BOUNDARYSIZE; i++) {
                            FileChannel fileChannel = leftChannel[0][i];
                            AtomicBoolean atomicBoolean = leftChannelSpinLock[0][i];
                            ByteBuffer byteBuffer = leftBufs[i];
                            byteBuffer.flip();
                            while (!atomicBoolean.compareAndSet(false, true)){}
                            fileChannel.write(byteBuffer);
                            atomicBoolean.set(false);
                            byteBuffer.clear();

                        }
                        for(int i = 0; i < Constant.BOUNDARYSIZE; i++)
                        {
                            FileChannel fileChannel = rightChannel[0][i];
                            AtomicBoolean atomicBoolean = rightChannelSpinLock[0][i];
                            ByteBuffer byteBuffer = rightBufs[i];
                            byteBuffer.flip();
                            while (!atomicBoolean.compareAndSet(false, true)){}
                            fileChannel.write(byteBuffer);
                            atomicBoolean.set(false);
                            byteBuffer.clear();
                        }
                    }
                    //System.out.println("Consumer thread " + threadNo  +  "get at " + new SimpleDateFormat("mm:ss").format(new Date(System.currentTimeMillis())));
                    if(ava_tuple.val3 == null) {
                        for(int i = 0; i < Constant.BOUNDARYSIZE; i++) {
                            FileChannel fileChannel = leftChannel[1][i];
                            AtomicBoolean atomicBoolean = leftChannelSpinLock[1][i];
                            ByteBuffer byteBuffer = leftBufs[i];
                            byteBuffer.flip();
                            while (!atomicBoolean.compareAndSet(false, true)){}
                            fileChannel.write(byteBuffer);
                            atomicBoolean.set(false);
                            byteBuffer.clear();

                        }
                        for(int i = 0; i < Constant.BOUNDARYSIZE; i++)
                        {
                            FileChannel fileChannel = rightChannel[1][i];
                            AtomicBoolean atomicBoolean = rightChannelSpinLock[1][i];
                            ByteBuffer byteBuffer = rightBufs[i];
                            byteBuffer.flip();
                            while (!atomicBoolean.compareAndSet(false, true)){}
                            fileChannel.write(byteBuffer);
                            atomicBoolean.set(false);
                            byteBuffer.clear();
                        }
                        System.out.println("Consumer thread " + threadNo  +  "come here  at " + new SimpleDateFormat("mm:ss").format(new Date(System.currentTimeMillis())));
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
                            int block_i = (int)(val >> Constant.SHIFTBITNUM);
                            ByteBuffer byteBuffer = leftBufs[block_i];
                            byteBuffer.putLong(val);
                            position = byteBuffer.position();
                            if(position >= Constant.BYTEBUFFERSIZE)
                            {
                                FileChannel fileChannel;
                                AtomicBoolean atomicBoolean;
                                if(col_i == 0)
                                {
                                    fileChannel = leftChannel[table_i][block_i];
                                    atomicBoolean = leftChannelSpinLock[table_i][block_i];
                                }
                                else
                                {
                                    fileChannel = rightChannel[table_i][block_i];
                                    atomicBoolean = rightChannelSpinLock[table_i][block_i];
                                }
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
}