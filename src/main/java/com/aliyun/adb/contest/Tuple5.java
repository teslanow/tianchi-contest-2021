package com.aliyun.adb.contest;

public class Tuple5 {
    int table_index, col_index, bound_index;
    long address, size;
    Tuple5(int table_index, int col_index, int bound_index, long address, long size)
    {
        this.table_index = table_index;
        this.col_index = col_index;
        this.bound_index = bound_index;
        this.address = address;
        this.size = size;
    }
    void setAll(int table_index, int col_index, int bound_index, long address, long size)
    {
        this.table_index = table_index;
        this.col_index = col_index;
        this.bound_index = bound_index;
        this.address = address;
        this.size = size;
    }
    void setIndex(int table_index, int col_index, int bound_index)
    {
        this.table_index = table_index;
        this.col_index = col_index;
        this.bound_index = bound_index;
    }
}
