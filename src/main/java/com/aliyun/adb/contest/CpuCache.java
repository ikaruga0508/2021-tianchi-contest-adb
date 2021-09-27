package com.aliyun.adb.contest;

import java.io.IOException;
import java.nio.ByteBuffer;

import sun.misc.Unsafe;
import sun.nio.ch.DirectBuffer;

public class CpuCache {
    private OutputFile outputFile;
    private long bufferAddress;
    private ByteBuffer cacheBuffer;
    private long[] currentAddresses;
    private Unsafe UNSAFE;

    public CpuCache(int id, OutputFile outputFile) {
        this.outputFile = outputFile;
        this.UNSAFE = UnsafeUtil.UNSAFE;

        cacheBuffer = ByteBuffer.allocateDirect(SimpleAnalyticDB.CPU_CACHE_ALLOCATE_SIZE * SimpleAnalyticDB.BIN_COUNT);
        bufferAddress = ((DirectBuffer) cacheBuffer).address();

        currentAddresses = new long[SimpleAnalyticDB.BIN_COUNT];

        for (int i = 0; i < SimpleAnalyticDB.BIN_COUNT; i++) {
            currentAddresses[i] = bufferAddress + i * SimpleAnalyticDB.CPU_CACHE_ALLOCATE_SIZE;
        }
    }

    public void write(long value, int hash) throws IOException {
        UNSAFE.putLong(currentAddresses[hash], value);
        currentAddresses[hash] += SimpleAnalyticDB.RESTORE_BYTE_COUNT;
        long startAddress = bufferAddress + hash * SimpleAnalyticDB.CPU_CACHE_ALLOCATE_SIZE;

        if (currentAddresses[hash] == startAddress + SimpleAnalyticDB.CPU_CACHE_ACTUAL_SIZE) {
            while (!outputFile.writeBuffer(hash, startAddress)) {
                try {
                    Thread.sleep(SimpleAnalyticDB.SLEEP_MS);
                } catch (InterruptedException e) {
                    UNSAFE.throwException(e);
                }
            }

            currentAddresses[hash] = startAddress;
        }
    }

    public long getBufferAddress() {
        return bufferAddress;
    }

    public long[] getCurrentAddresses() {
        return currentAddresses;
    }
}
