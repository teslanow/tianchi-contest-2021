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



//    public static final int Constant.BOUNDARYSIZE = 130;
//    public static final int Constant.QUANTILE_DATA_SIZE = 32000000; //每次查询的data量，基本等于DATALENGTH / Constant.BOUNDARYSIZE * 8
//    public static final int Constant.THREADNUM = 16;
//    public static final long Constant.DATALENGTH = 500000000;
//    public static final int Constant.BYTEBUFFERSIZE = 1024 * 128;
//    public static final int Constant.EACHREADSIZE = 1024 * 1024 * 16;
//    //public static final int Constant.EACHREADSIZE = 1024;
//    public static final int Constant.TABLENUM = 2;
//    public static final int Constant.COLNUM_EACHTABLE = 2;
//    public static final int Constant.SHIFTBITNUM = 56;
//    public static final int Constant.CONCURRENT_QUANTILE_THREADNUM = 8;

//    public static final int Constant.BOUNDARYSIZE = 130;
//    public static final int Constant.QUANTILE_DATA_SIZE = 800; //每次查询的data量，基本等于DATALENGTH / Constant.BOUNDARYSIZE * 8
//    public static final int Constant.THREADNUM = 1;
//    public static final long Constant.DATALENGTH = 10000;
//    public static final int Constant.BYTEBUFFERSIZE = 1024 * 128;
//    public static final int Constant.EACHREADSIZE = 1024 ;
//    //public static final int Constant.EACHREADSIZE = 1024;
//    public static final int Constant.TABLENUM = 2;
//    public static final int Constant.COLNUM_EACHTABLE = 2;
//    public static final int Constant.SHIFTBITNUM = 56;
//    public static final int Constant.CONCURRENT_QUANTILE_THREADNUM = 8;

}
