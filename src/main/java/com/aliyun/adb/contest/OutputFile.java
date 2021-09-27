package com.aliyun.adb.contest;

import java.io.IOException;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;

import sun.misc.Unsafe;
import sun.nio.ch.DirectBuffer;

public class OutputFile implements Serializable {
    // 既做读也用做写
    private FileChannel[] channels = null;
    private ByteBuffer[] buffer = null;
    private Unsafe UNSAFE;
    private CpuCache cpuCache;

    // 对应线程的id
    private int id;
    // 当前桶的位置
    private AtomicInteger[] currentPosArray;
    // 当前桶已存放数据字节大小
    private AtomicInteger[] binSizes;
    // 查询用
    private Path[] binPathes;

    private LinkedBlockingQueue<BufferWithHash> productQueue;
    private LinkedBlockingQueue<ByteBuffer> packageQueue;
    private AtomicInteger outBufferCountNow;
    
    public OutputFile(int id, Path[] binPathes, FileChannel[] channels, AtomicInteger[] currentPosArray, AtomicInteger[] binSizes) throws IOException {
        this.id = id;
        this.binPathes = binPathes;
        this.channels = channels;

        this.currentPosArray = currentPosArray;
        this.binSizes = binSizes;

        cpuCache = new CpuCache(id, this);
        this.UNSAFE = UnsafeUtil.UNSAFE;
    }

    // 恢复用的构造函数
    public OutputFile(int id, Path[] binPathes) {
        this.id = id;
        this.binPathes = binPathes;
        this.UNSAFE = UnsafeUtil.UNSAFE;
    }

    public void init(ByteBuffer[] outBuffer, LinkedBlockingQueue<BufferWithHash> productQueue, LinkedBlockingQueue<ByteBuffer> packageQueue, AtomicInteger outBufferCountNow) {
        this.buffer = outBuffer;
        this.productQueue = productQueue;
        this.packageQueue = packageQueue;
        this.outBufferCountNow = outBufferCountNow;
    }

    // 单值写入CPU Cache
    public void writeToCPUCache(long value, int hash) throws IOException {
        cpuCache.write(value, hash);
    }

    // CPU Cache(整块)写入公有Buffer
    public boolean writeBuffer(int hash, long startAddress) throws IOException {
        // 申请写入位置
        int currentPos = currentPosArray[hash].getAndAdd(SimpleAnalyticDB.CPU_CACHE_ACTUAL_SIZE);

        if (currentPos >= SimpleAnalyticDB.OUT_BUFFER_SIZE) {
            return false;
        }

        UNSAFE.copyMemory(null, startAddress, null, ((DirectBuffer) buffer[hash]).address() + currentPos, SimpleAnalyticDB.CPU_CACHE_ACTUAL_SIZE);
        int binSize = binSizes[hash].addAndGet(SimpleAnalyticDB.CPU_CACHE_ACTUAL_SIZE);

        // 缓存满了(大小由OUT_BUFFER_SIZE定义)之后写入FileChannel
        if (binSize == SimpleAnalyticDB.OUT_BUFFER_SIZE) {
            ByteBuffer blockBuffer;

            while ((blockBuffer = packageQueue.poll()) == null) {
                if (outBufferCountNow.get() < SimpleAnalyticDB.MAX_OUT_BUFFER_COUNT) {
                    outBufferCountNow.getAndIncrement();
                    // int count = outBufferCountNow.getAndIncrement();
                    blockBuffer = ByteBuffer.allocateDirect(SimpleAnalyticDB.OUT_BUFFER_SIZE);
                    // if (count + 1 >= SimpleAnalyticDB.MAX_OUT_BUFFER_COUNT) {
                    //     System.out.println(SimpleAnalyticDB.getNow() + String.format("[Info] OutBuffer's upper limit reached. (Max: %d)", count + 1));
                    // }
                    break;
                } else {
                    try {
                        Thread.sleep(SimpleAnalyticDB.SLEEP_MS);
                    } catch (InterruptedException e) {
                        UNSAFE.throwException(e);
                    }
                }
            }

            blockBuffer.clear();

            // 将公有Buffer中满了的数据插进队列
            productQueue.offer(new BufferWithHash(buffer[hash], hash));
            // 将空闲缓存替换公有Buffer原来的位置
            buffer[hash] = blockBuffer;

            binSizes[hash].set(0);
            currentPosArray[hash].set(0);
        }

        return true;
    }

    // CPU Cache(零碎)写入公有Buffer
    public void writeBuffer(int hash, long startAddress, long endAddress) throws IOException {
        int currentPos = currentPosArray[hash].get();
        int size = (int)(endAddress - startAddress);
        int remainSize = 0;

        // 判断剩余空间是否足够
        // 足够: 1次写入 / 不足够: 分开2次写入
        if (size > (SimpleAnalyticDB.OUT_BUFFER_SIZE - currentPos)) {
            remainSize = size - (SimpleAnalyticDB.OUT_BUFFER_SIZE - currentPos);
            size = SimpleAnalyticDB.OUT_BUFFER_SIZE - currentPos;
        }

        // 调整写入大小，第一次写入
        UNSAFE.copyMemory(null, startAddress, null, ((DirectBuffer) buffer[hash]).address() + currentPos, size);
        currentPosArray[hash].addAndGet(size);
        int binSize = binSizes[hash].addAndGet(size);

        if (binSize == SimpleAnalyticDB.OUT_BUFFER_SIZE) {
            buffer[hash].position(0);
            buffer[hash].limit(SimpleAnalyticDB.OUT_BUFFER_SIZE);
            channels[hash].write(buffer[hash]);
            buffer[hash].clear();
            binSizes[hash].set(0);
            currentPosArray[hash].set(0);
        }

        // 写入剩余部分的数据
        if (remainSize > 0) {
            UNSAFE.copyMemory(null, startAddress + size, null, ((DirectBuffer) buffer[hash]).address(), remainSize);
            currentPosArray[hash].addAndGet(remainSize);
            binSizes[hash].addAndGet(remainSize);
        }
    }

    // 避难所数据写入公有Buffer
    public boolean writeBuffer(int hash, ByteBuffer havenCacheBuffer) throws IOException {
        int currentPos;
        int binSize;

        // 申请写入位置
        currentPos = currentPosArray[hash].getAndAdd(SimpleAnalyticDB.CPU_CACHE_ACTUAL_SIZE);

        if (currentPos >= SimpleAnalyticDB.OUT_BUFFER_SIZE) {
            return false;
        }

        UNSAFE.copyMemory(null, ((DirectBuffer) havenCacheBuffer).address(), null, ((DirectBuffer) buffer[hash]).address() + currentPos, SimpleAnalyticDB.CPU_CACHE_ACTUAL_SIZE);
        binSize = binSizes[hash].addAndGet(SimpleAnalyticDB.CPU_CACHE_ACTUAL_SIZE);

        // 缓存满了(大小由OUT_BUFFER_SIZE定义)之后写入FileChannel
        if (binSize == SimpleAnalyticDB.OUT_BUFFER_SIZE) {
            buffer[hash].position(0);
            buffer[hash].limit(SimpleAnalyticDB.OUT_BUFFER_SIZE);
            channels[hash].write(buffer[hash]);
            buffer[hash].clear();
            binSizes[hash].set(0);
            currentPosArray[hash].set(0);
        }

        return true;
    }
    
    public CpuCache getCpuCache() {
        return cpuCache;
    }

    public ByteBuffer[] getBuffer() {
        return buffer;
    }

    public AtomicInteger[] getCurrentPosArray() {
        return currentPosArray;
    }

    public FileChannel getFileChannel(int hash) {
        return channels[hash];
    }

    public void pickupData(int hash, long[] values, ByteBuffer buffer, int count) throws IOException {
        // 读取索引对应的缓存块
        // channels[hash].read(buffer);
        try (FileChannel channel = FileChannel.open(binPathes[hash], StandardOpenOption.READ)) {
            channel.read(buffer);
            long address = ((DirectBuffer) buffer).address();

            for (int i = 0; i < count; i++) {
                values[i] = UNSAFE.getLong(address) & 0x00ffffffffffffffL;
                address += SimpleAnalyticDB.RESTORE_BYTE_COUNT;
            }
            buffer.clear();
        }
    }
}
