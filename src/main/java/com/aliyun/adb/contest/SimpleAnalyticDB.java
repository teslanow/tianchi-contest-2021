package com.aliyun.adb.contest;

import com.aliyun.adb.contest.spi.AnalyticDB;
import sun.misc.Unsafe;
import sun.nio.ch.DirectBuffer;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.concurrent.*;

import static com.aliyun.adb.contest.ConsumerThreadPool.CONSUMERPOOL;

public class SimpleAnalyticDB implements AnalyticDB {
    private int current_Quantile_threadNUM = 0;
    private final String[][] colName = new String[Constant.TABLENUM][Constant.COLNUM_EACHTABLE];
    private final String[] tabName = new String[Constant.TABLENUM];
    private final Unsafe unsafe;
    private final int[][][] blockSize = new int[Constant.TABLENUM][Constant.COLNUM_EACHTABLE][Constant.BOUNDARYSIZE];
    private final int[][][] beginOrder = new int[Constant.TABLENUM][Constant.COLNUM_EACHTABLE][Constant.BOUNDARYSIZE];
    private final long[] quantile_load_base = new long[Constant.CONCURRENT_QUANTILE_THREADNUM];
    private final ByteBuffer[] quantile_load_buffer = new ByteBuffer[Constant.CONCURRENT_QUANTILE_THREADNUM];
    private final long[] arrThreadId = new long[Constant.CONCURRENT_QUANTILE_THREADNUM];
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
        System.out.println("thread " + Thread.currentThread().getId() + " current_threadnum " + current_Quantile_threadNUM + " buffer_id " + buffer_index );
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
        System.out.println("one quantile time is " + (e1 - s1) + " percentile " + percentile + "rank "+ rank + " index " + index  + " table " + tabName[flag_table] + " column " + colName[flag_table][flag_colum] + " " + ans);
        return "0";
//        return ans;
    }

    private void loadStore(File[] dataFileList) throws Exception {
        for(int j = 0; j < Constant.TABLENUM; j++)
        {
            for (int i = 0; i < Constant.BOUNDARYSIZE; i++){
                beginOrder[j][0][i] = 0;
                beginOrder[j][1][i] = 0;
            }
        }
        long[][] readStartEachThread = new long[Constant.THREADNUM][Constant.TABLENUM];
        long[][] trueSizeOfMmapEachThread = new long[Constant.THREADNUM][Constant.TABLENUM];
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
            long sizePerBuffer = size / Constant.THREADNUM;
            byteBuffer.clear();
            channel.read(byteBuffer);
            long hasReadByte = 0;
            for(int i = 0, pre = 0, j = 0; i < 42; i++)
            {
                byte t = bytes[i];
                hasReadByte++;
                if(t == 44)
                {
                    colName[k][j++] = new String(bytes, pre, i, StandardCharsets.UTF_8);
                    pre = i + 1;
                }
                else if(bytes[i] == 10)
                {
                    colName[k][j++] = new String(bytes, pre, i - pre, StandardCharsets.UTF_8);
                    break;
                }
            }
            tabName[k] = dataFile.getName();


            for(int i = 0; i < Constant.THREADNUM; i++)
            {
                if(i == Constant.THREADNUM - 1)
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
        FileChannel[][][] allWriteChannel = new FileChannel[Constant.TABLENUM][Constant.BOUNDARYSIZE][Constant.COLNUM_EACHTABLE];
        for(int i = 0; i < Constant.TABLENUM; i++)
        {
            for(int j = 0; j < Constant.BOUNDARYSIZE; j++)
            {
                for(int k = 0; k < Constant.COLNUM_EACHTABLE; k++)
                {
                    String outDir = workDir + "/" + tabName[i] + "-" + colName[i][k] + "-"  +  j;
                    allWriteChannel[i][j][k] = new RandomAccessFile(new File(outDir), "rw").getChannel();
                }
            }
        }
        CyclicBarrier[] barriers = new CyclicBarrier[2];
        barriers[0] = new CyclicBarrier(Constant.THREADNUM);
        barriers[1] = new CyclicBarrier(Constant.THREADNUM + 1);
        Partion[][] allPartion = new Partion[Constant.COLNUM_EACHTABLE][Constant.BOUNDARYSIZE];
        for (int i = 0; i < Constant.COLNUM_EACHTABLE; i++)
        {
            for(int j = 0; j < Constant.BOUNDARYSIZE; j++)
            {
                allPartion[i][j] = new Partion(j, allWriteChannel[i][j]);
            }
        }
        for(int i = 0; i < Constant.THREADNUM; i++)
        {
            new Thread(new ProducerThread(i, readStartEachThread[i],trueSizeOfMmapEachThread[i], allFileChannel, allPartion, barriers)).start();
        }
        barriers[1].await();
        CONSUMERPOOL.shutdown();
        while (!CONSUMERPOOL.isShutdown()){Thread.sleep(1000);}
        StringBuilder builder= new StringBuilder(workDir).append("/index");
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
        for(int i = 0; i < Constant.TABLENUM; i++)
        {
            for(int j = 0; j < Constant.COLNUM_EACHTABLE; j++)
            {
                for(int k = 0; k < Constant.BOUNDARYSIZE; k++)
                {
                    blockSize[i][j][k] = (int) (allPartion[i][k].hasWritensize[i] >> 3);
                }
            }
        }
        for(int j = 0; j < Constant.TABLENUM; j++)
        {
            int  lBry = 0, rBry = 0;
            for (int i = 0; i < Constant.BOUNDARYSIZE; i++){
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



}