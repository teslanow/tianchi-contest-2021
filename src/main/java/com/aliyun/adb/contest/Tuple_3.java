package com.aliyun.adb.contest;

import java.nio.ByteBuffer;

public class Tuple_3 {
    public int val1; //属于哪个表
    public long val2; //基地址
    public ByteBuffer val3; //读取的缓冲
    Tuple_3(int val1, long val2, ByteBuffer val3)
    {
        this.val1 = val1;
        this.val2 = val2;
        this.val3 = val3;
    }
    public void setAll(int val1, long val2, ByteBuffer val3)
    {
        this.val1 = val1;
        this.val2 = val2;
        this.val3 = val3;
    }
}
