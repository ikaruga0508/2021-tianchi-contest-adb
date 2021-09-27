package com.aliyun.adb.contest;

import java.nio.ByteBuffer;

public class BufferWithHash {
    private ByteBuffer buffer;
    private int hash;

    public BufferWithHash(ByteBuffer buffer, int hash) {
        this.buffer = buffer;
        this.hash = hash;
    }

    public ByteBuffer getBuffer() {
        return buffer;
    }

    public int getHash() {
        return hash;
    }
}
