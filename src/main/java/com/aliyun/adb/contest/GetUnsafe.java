package com.aliyun.adb.contest;

import sun.misc.Unsafe;

import java.lang.reflect.Field;

public class GetUnsafe {
    public static Unsafe getUnsafe() throws NoSuchFieldException, IllegalAccessException {
        Field f = Unsafe.class.getDeclaredField("theUnsafe");
        f.setAccessible(true);
        Unsafe unsafe = (Unsafe)f.get(null);
        return unsafe;
    }
}
