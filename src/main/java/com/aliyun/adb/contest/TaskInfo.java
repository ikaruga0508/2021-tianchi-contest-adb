package com.aliyun.adb.contest;

public class TaskInfo {
    private long startPos;
    private long endPos;

    public TaskInfo(long startPos, long endPos) {
        this.startPos = startPos;
        this.endPos = endPos;
    }

    public void setNewPos(long startPos, long endPos) {
        this.startPos = startPos;
        this.endPos = endPos;
    }

    public long getStartPos() {
        return startPos;
    }

    public long getEndPos() {
        return endPos;
    }
}
