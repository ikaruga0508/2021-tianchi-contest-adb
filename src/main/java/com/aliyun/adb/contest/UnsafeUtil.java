package com.aliyun.adb.contest;

import java.lang.reflect.Field;
import sun.misc.Unsafe;

public class UnsafeUtil {
    public static final Unsafe UNSAFE;
    static {
        try {
            Field field = Unsafe.class.getDeclaredField("theUnsafe");
            field.setAccessible(true);
            UNSAFE = (Unsafe) field.get(null);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
