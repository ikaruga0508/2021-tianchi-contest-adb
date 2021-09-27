package com.aliyun.adb.contest;

import java.io.Serializable;

public class BinInfo implements Serializable {
    // 所有的成员变量，重新加载时需要赋值
    private long startPosition = 0;
    private long count = 0;

    public long getStartPosition() {
        return startPosition;
    }
    public void setStartPosition(long startPosition) {
        this.startPosition = startPosition;
    }
    public long getCount() {
        return count;
    }
    public void setCount(long count) {
        this.count = count;
    }
    public void addCount(long count) {
        this.count += count;
    }
    public boolean isHit(long index) {
        return (startPosition <= index) && (index < startPosition + count);
    }
    public int isHit2(long index) {
        if (index < startPosition) {
            return -1;
        } else if (index >= startPosition + count) {
            return 1;
        } else {
            return 0;
        }
    }
}
