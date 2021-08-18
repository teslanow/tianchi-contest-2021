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
    private static final int THREADNUM = 64;
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
        return "0";
    }

    private void loadStore(File[] dataFileList) throws Exception {

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

        long size = 16 * 1024;
        long tmp = unsafe.allocateMemory(size);
        unsafe.setMemory(null, tmp, size, (byte) 1);
        long s = System.currentTimeMillis();
        for(int i = 0; i < 460; i++)
        {
            for(int j = 0; j < TABLENUM; j++)
            {
                for(int k = 0; k < BOUNDARYSIZE; k++)
                {
                    long address = allRightWriteAddress[j][k];
                    unsafe.copyMemory(null, tmp, null, address, size);
                    allRightWriteAddress[j][k] += size;
                    address = allLeftWriteAddress[j][k];
                    unsafe.copyMemory(null, tmp, null, address, size);
                    allLeftWriteAddress[j][k] += size;
                }
            }
        }
        for(int i = 0; i < TABLENUM; i++)
        {
            for(int j = 0; j < BOUNDARYSIZE; j++)
            {
                allLeftWriteMapBuffer[i][j].force();
                allRightWriteMapBuffer[i][j].force();
            }
        }
        long e = System.currentTimeMillis();
        System.out.println(" time " + (e - s));
    }

}