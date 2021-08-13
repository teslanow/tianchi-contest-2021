package com.aliyun.adb.contest;

public class Constant {
    public static final int BOUNDARYSIZE = 520;
    public static final int QUANTILE_DATA_SIZE = 16000000; //每次查询的data量，基本等于DATALENGTH / BOUNDARYSIZE * 8
    public static final int THREADNUM = 32;
    public static final long DATALENGTH = 1000000000;
    public static final int BYTEBUFFERSIZE = 1024 * 64;
    public static final int EACHREADSIZE = 1024 * 1024 * 16;
    public static final int TABLENUM = 2;
    public static final int COLNUM_EACHTABLE = 2;
    public static final int SHIFTBITNUM = 54;
    public static final int CONCURRENT_QUANTILE_THREADNUM = 8;
    public static final int AVALIABLE_BUFFERNUM = 10;
    public static final int PRODUCER_NUM = 16;
    public static final int ALLREADEREND = (1 << PRODUCER_NUM) - 1;



//    private static final int Constant.BOUNDARYSIZE = 130;
//    private static final int Constant.QUANTILE_DATA_SIZE = 32000000; //每次查询的data量，基本等于DATALENGTH / Constant.BOUNDARYSIZE * 8
//    private static final int Constant.THREADNUM = 16;
//    private static final long Constant.DATALENGTH = 500000000;
//    private static final int Constant.BYTEBUFFERSIZE = 1024 * 128;
//    private static final int Constant.EACHREADSIZE = 1024 * 1024 * 16;
//    //private static final int Constant.EACHREADSIZE = 1024;
//    private static final int Constant.TABLENUM = 2;
//    private static final int Constant.COLNUM_EACHTABLE = 2;
//    private static final int Constant.SHIFTBITNUM = 56;
//    private static final int Constant.CONCURRENT_QUANTILE_THREADNUM = 8;

//    private static final int Constant.BOUNDARYSIZE = 130;
//    private static final int Constant.QUANTILE_DATA_SIZE = 800; //每次查询的data量，基本等于DATALENGTH / Constant.BOUNDARYSIZE * 8
//    private static final int Constant.THREADNUM = 1;
//    private static final long Constant.DATALENGTH = 10000;
//    private static final int Constant.BYTEBUFFERSIZE = 1024 * 128;
//    private static final int Constant.EACHREADSIZE = 1024 ;
//    //private static final int Constant.EACHREADSIZE = 1024;
//    private static final int Constant.TABLENUM = 2;
//    private static final int Constant.COLNUM_EACHTABLE = 2;
//    private static final int Constant.SHIFTBITNUM = 56;
//    private static final int Constant.CONCURRENT_QUANTILE_THREADNUM = 8;

}
