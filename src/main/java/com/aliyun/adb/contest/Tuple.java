package com.aliyun.adb.contest;

import java.nio.ByteBuffer;

public class Tuple {
    public int val1, val2, val3;
    public ByteBuffer val4;
    Tuple(int val1, int val2, int val3, ByteBuffer val4)
    {
        this.val1 = val1;
        this.val2 = val2;
        this.val3 = val3;
        this.val4 = val4;
    }
    public void setAll(int val1, int val2, int val3)
    {
        this.val1 = val1;
        this.val2 = val2;
        this.val3 = val3;
    }
}
