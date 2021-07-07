package com.aliyun.adb.contest;

public class MyFind {
    public static Long quickFind(long[] data, int low, int high, int k) {
        int num = partition(data, low, high);
        if (k == num - low + 1){
            return data[num];
        }else if (k > num - low + 1){
            return quickFind(data, num + 1, high, k-num+low-1);
        }else {
            return quickFind(data, low, num-1, k);
        }
    }

    private static int partition(long[] data, int low, int high) {
        long key = data[low];
        while (low < high) {
            while (low<high&&data[high]>=key) high--;//从后扫描，找到第一个比key大的值
            data[low] = data[high];
            while (low<high && data[low]<=key) low++;//从前扫描，找到第一个比key小的值
            data[high] = data[low];
        }
        data[low] = key;
        return low;
    }

}
