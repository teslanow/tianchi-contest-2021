package com.aliyun.adb.contest;

import sun.misc.Unsafe;

public class MyFind {
    public static Long quickFind(Unsafe unsafe, long low_address, long high_address, long k_address) {
        long num_address = partition(unsafe, low_address, high_address);
        if (k_address == num_address - low_address + 8) {
            return unsafe.getLong(num_address);
        } else if (k_address > num_address - low_address + 8) {
            return quickFind(unsafe, num_address + 8, high_address, k_address - (num_address - low_address + 8));
        } else {
            return quickFind(unsafe, low_address, num_address - 8, k_address);
        }

    }

    private static long partition(Unsafe unsafe, long low_address, long high_address) {
        long key = unsafe.getLong(low_address);
        while (low_address < high_address) {
            while (low_address < high_address && unsafe.getLong(high_address) >= key)
                high_address -= 8;//从后扫描，找到第一个比key大的值
            unsafe.putLong(low_address, unsafe.getLong(high_address));
            while (low_address < high_address && unsafe.getLong(low_address) <= key)
                low_address += 8;//从前扫描，找到第一个比key小的值
            unsafe.putLong(high_address, unsafe.getLong(low_address));
        }
        unsafe.putLong(low_address, key);
        return low_address;
    }
}