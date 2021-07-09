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

public class SimpleAnalyticDB implements AnalyticDB {

    private static final int BOUNDARYSIZE = 130;
    private static final int THREADNUM = 16;
    private static final int DATALENGTH = 1000000000;
    private static final int BYTEBUFFERSIZE = 1024 * 64;
    private static final int EACHREADSIZE = 1024 * 1024 * 16;
    private static final int TABLENUM = 2;
    private static final int COLNUM_EACHTABLE = 2;
    private String[][] colName = new String[TABLENUM][COLNUM_EACHTABLE];
    private String[] tabName = new String[TABLENUM];
    private String curTableName;
    private Unsafe unsafe;
    private final int[][][] blockSize = new int[TABLENUM][COLNUM_EACHTABLE][BOUNDARYSIZE];
    private final int[][][] beginOrder = new int[TABLENUM][COLNUM_EACHTABLE][BOUNDARYSIZE];

    private static final CountDownLatch latch = new CountDownLatch(THREADNUM);
    private  String workDir;
    
    public SimpleAnalyticDB() throws NoSuchFieldException, IllegalAccessException {
        this.unsafe = GetUnsafe.getUnsafe();
    }

    @Override
    public void load(String tpchDataFileDir, String workspaceDir) throws Exception {
        long ss = System.currentTimeMillis();
        workDir = workspaceDir;
        //判断工作区是否为空
        if(new File(workspaceDir + "/index").exists())
        {
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
        File dir = new File(tpchDataFileDir);
        loadStore(dir.listFiles());
        long end = System.currentTimeMillis();
        System.out.println("load time is " + (end - ss));
    }

    @Override
    public String quantile(String table, String column, double percentile) throws Exception {
        long s1 = System.currentTimeMillis();
        String ans;
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
        int retIndex = Arrays.binarySearch(curBeginOrder,rank);
        if (retIndex >= 0) {
            index = retIndex;
        }
        else {
            index = - retIndex - 2;
        }
        int rankDiff;
        rankDiff = rank - curBeginOrder[index]+ 1;
        int data_length = index == BOUNDARYSIZE - 1 ? (DATALENGTH + 1 - curBeginOrder[index]):  curBeginOrder[index + 1] - curBeginOrder[index] ;
        long[] data = new long[data_length];
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
            inFile.close();
        }
        ans = MyFind.quickFind(data, 0, pos - 1, rankDiff).toString();
        long e1 = System.currentTimeMillis();
        System.out.println("one quantile time is " + (e1 - s1) + " rank "+ rank + " index " + index + " ans "+ ans + " table " + tabName[flag_table] + " column " + colName[flag_table][flag_colum]);
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
                    hasReadByte++;
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
                beginOrder[j][0][i] = lBry + 1;
                lBry += blockSize[j][0][i];
                beginOrder[j][1][i] = rBry + 1;
                rBry += blockSize[j][1][i];
            }
        }
        byte[] b_t = new byte[BOUNDARYSIZE * Integer.SIZE];
        ByteBuffer b_t_buffer = ByteBuffer.wrap(b_t);
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
        System.out.println("table 0 " + ( beginOrder[0][0][BOUNDARYSIZE - 1] - 1 )  + " " + ( beginOrder[0][1][BOUNDARYSIZE - 1] - 1) );
        System.out.println("table 1 " + ( beginOrder[1][0][BOUNDARYSIZE - 1] - 1 )  + " " + ( beginOrder[1][1][BOUNDARYSIZE - 1] - 1) );
        for(int i = 0; i < TABLENUM; i++)
        {
            for(int j = 0; j < COLNUM_EACHTABLE; j++)
            {
                System.out.println(Arrays.toString(beginOrder[i][j]));
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
        FileOutputStream[] leftChannel;
        FileOutputStream[] rightChannel;
        ByteBuffer[] leftBufs;
        ByteBuffer[] rightBufs;

        //初始化
        public ThreadTask(int threadNo, long[] readStart ,long[] trueSizeOfMmap, FileChannel[] fileChannel) throws Exception {
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
            //int leftReadNum = 0, rightReadNum = 0;
            try{
                for(int k = 0; k < TABLENUM; k++)
                {
                    String outLDir, outRDir;
                    File LoutFile, RoutFile;
                    RandomAccessFile Lrw, Rrw;
                    StringBuilder builder;
                    curTableName = tabName[k];
                    for (int i = 0; i < BOUNDARYSIZE; i++) {
                        builder = new StringBuilder(workDir);
                        outLDir = builder.append("/").append(curTableName).append("-").append(colName[k][0]).append(threadNo).append("-").append(i).toString();
                        builder = new StringBuilder(workDir);
                        outRDir = builder.append("/").append(curTableName).append("-").append(colName[k][1]).append(threadNo).append("-").append(i).toString();
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
                        int position;
                        byte t;
                        for(int index = 0; index < realRead; index++) {
                            t = unsafe.getByte(directBufferBase + index);
                            if((t & 16) == 0) {
                                if(t == 44) {
                                    //leftReadNum++;
                                    int leftIndex = (int)(val >> 56);
                                    leftBufs[leftIndex].putLong(val);
                                    position = leftBufs[leftIndex].position();
                                    if (position >= BYTEBUFFERSIZE) {
                                        leftChannel[leftIndex].write(leftBufs[leftIndex].array(), 0, position);
                                        leftBufs[leftIndex].clear();
                                    }
                                    val = 0;
                                }else {
                                    //rightReadNum++;
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
                    realRead = trueSizeOfMmap[k] - nowRead;
                    directBuffer.clear();
                    fileChannel[k].read(directBuffer, curReadStart + nowRead);
                    nowRead += realRead;
                    long val = 0;
                    int position;
                    byte t;
                    for(int index = 0; index < realRead; index++) {
                        t = unsafe.getByte(directBufferBase + index);
                        if((t & 16) == 0) {
                            if(t == 44) {
                                //leftReadNum++;
                                int leftIndex = (int)(val >> 56);
                                leftBufs[leftIndex].putLong(val);
                                position = leftBufs[leftIndex].position();
                                if (position >= BYTEBUFFERSIZE) {
                                    leftChannel[leftIndex].write(leftBufs[leftIndex].array(), 0, position);
                                    leftBufs[leftIndex].clear();
                                }
                                val = 0;
                            }else {
                                //rightReadNum++;
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
                        synchronized (blockSize[k])
                        {
                            blockSize[k][0][i] += leftChannel[i].getChannel().size() >> 3;
                            blockSize[k][1][i] += rightChannel[i].getChannel().size() >> 3;
                        }
                    }
                    for(int i = 0; i < BOUNDARYSIZE; i++)
                    {
                        leftChannel[i].close();
                        rightChannel[i].close();
                    }
                }
            }catch (Exception e){
                e.printStackTrace();
            }
            //System.out.println("thread " + threadNo + " " + leftReadNum + " " + rightReadNum);
            latch.countDown();
        }

    }
}