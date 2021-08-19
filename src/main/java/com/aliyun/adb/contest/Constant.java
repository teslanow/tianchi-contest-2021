package com.aliyun.adb.contest;


public class Constant {
    public static final int BOUNDARYSIZE = 1040;
    public static final int QUANTILE_DATA_SIZE = 16000000; //每次查询的data量，基本等于DATALENGTH / BOUNDARYSIZE * 8
    public static final int THREADNUM = 1;
    public static final int WRITETHREAD = 2;
    public static final long DATALENGTH = 1000000000;
    public static final int BYTEBUFFERSIZE = 64 * 1024;
    public static final int EACHREADSIZE = 16 * 1024 * 1024;
    public static final int TABLENUM = 2;
    public static final int COLNUM_EACHTABLE = 2;
    public static final int SHIFTBITNUM = 53;
    public static final int CONCURRENT_QUANTILE_THREADNUM = 8;
    public static final int PARTITION_SIZE = BYTEBUFFERSIZE * WRITETHREAD;
}
