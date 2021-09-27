package com.aliyun.adb.contest;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;

public class ColumnMetaInfo implements Serializable {
    private OutputFile[] outputFiles;
    // 公有缓存
    private ByteBuffer[] outBuffer;
    private Thread threadCreateOutBuffer;
    private Thread threadOpenFile;
    private BinInfo[] binInfos;
    private long recordCount;
    private String column;

    private class DummyPadding {
        private long l1, l2, l3, l4, l5;
        public long sum() {
            return l1 + l2 + l3 + l4 + l5;
        }
    }

    public ColumnMetaInfo(String column) {
        outputFiles = new OutputFile[SimpleAnalyticDB.MIDDLEMAN_COUNT];
        binInfos = new BinInfo[SimpleAnalyticDB.BIN_COUNT];
        this.outBuffer = new ByteBuffer[SimpleAnalyticDB.BIN_COUNT];
        recordCount = 0;
        this.column = column;
    }

    public void initAsyc(CountDownLatch lastCdlMiddlemanCompleted, LinkedBlockingQueue<ByteBuffer> packageQueue, int round) {
        threadCreateOutBuffer = new Thread(new Runnable(){
            @Override
            public void run() {
                while ((lastCdlMiddlemanCompleted != null) && (lastCdlMiddlemanCompleted.getCount() > 0)) {
                    try {
                        Thread.sleep(100);
                    } catch (InterruptedException e) {
                        UnsafeUtil.UNSAFE.throwException(e);
                    }
                }
        
                // System.out.println(SimpleAnalyticDB.getNow() + String.format("[Info][%s] Creating output buffers. (PackageQueue: %d)", column, packageQueue.size()));
                if (round == 0) {
                    for (int i = 0; i < SimpleAnalyticDB.BIN_COUNT; i++) {
                        outBuffer[i] = ByteBuffer.allocateDirect(SimpleAnalyticDB.OUT_BUFFER_SIZE);
                        // outBuffer[i].order(ByteOrder.nativeOrder());
                    }
                } else {
                    for (int i = 0; i < SimpleAnalyticDB.BIN_COUNT; i++) {
                        ByteBuffer buffer = packageQueue.poll();
                        if (buffer == null) {
                            buffer = ByteBuffer.allocateDirect(SimpleAnalyticDB.OUT_BUFFER_SIZE);
                        } else {
                            buffer.clear();
                        }

                        outBuffer[i] = buffer;
                    }
                }
                // System.out.println(SimpleAnalyticDB.getNow() + String.format("[Info][%s] Output buffers is created. (PackageQueue: %d)", column, packageQueue.size()));
            }
        });
        threadCreateOutBuffer.start();
    }

    public void initJoin1() {
        try {
            threadCreateOutBuffer.join();
        } catch (Throwable e) {
            UnsafeUtil.UNSAFE.throwException(e);
        }
    }

    public void initJoin2() {
        try {
            threadOpenFile.join();
        } catch (Throwable e) {
            UnsafeUtil.UNSAFE.throwException(e);
        }
    }

    public void recycle(LinkedBlockingQueue<ByteBuffer> packageQueue, int startHash, int endHash) {
        for (int i = startHash; i < endHash; i++) {
            packageQueue.offer(outBuffer[i]);
        }
    }

    public void init(File columnFolder) throws IOException, InterruptedException {
        Path[] pathes = new Path[SimpleAnalyticDB.BIN_COUNT];
        FileChannel[] channels = new FileChannel[SimpleAnalyticDB.BIN_COUNT];

        threadOpenFile = new Thread(new Runnable(){
            @Override
            public void run() {
                try {
                    // Date now = new Date();
                    for (int i = 0; i < SimpleAnalyticDB.BIN_COUNT; i++) {
                        pathes[i] = new File(columnFolder, String.format("%d.data", i)).toPath();
                        channels[i] = FileChannel.open(pathes[i], StandardOpenOption.CREATE, StandardOpenOption.WRITE, StandardOpenOption.READ);
                    }
                    // System.out.println(SimpleAnalyticDB.getNow() + String.format("[Info] Bin file is opened. (%d)", (new Date()).getTime() - now.getTime()));
                } catch (Throwable e) {
                    UnsafeUtil.UNSAFE.throwException(e);
                }
            }
        });
        threadOpenFile.start();

        AtomicInteger[] currentPosArray = new AtomicInteger[SimpleAnalyticDB.BIN_COUNT];
        AtomicInteger[] binCounts = new AtomicInteger[SimpleAnalyticDB.BIN_COUNT];
        // DummyPadding[] dummyPaddingArray = new DummyPadding[SimpleAnalyticDB.BIN_COUNT];
        // DummyPadding[] dummyPaddingArray2 = new DummyPadding[SimpleAnalyticDB.BIN_COUNT];

        for (int i = 0; i < SimpleAnalyticDB.BIN_COUNT; i++) {
            currentPosArray[i] = new AtomicInteger();
            // dummyPaddingArray[i] = new DummyPadding();
            binCounts[i] = new AtomicInteger();
            // dummyPaddingArray2[i] = new DummyPadding();
        }

        // 防止Java优化对DummyPadding起作用，而导致伪共享的方案失效
        // dummyPaddingArray[0].sum();

        // 初始化outputFiles对象
        for (int thread_id = 0; thread_id < outputFiles.length; thread_id++) {
            outputFiles[thread_id] = new OutputFile(thread_id, pathes, channels, currentPosArray, binCounts);
        }

        // 初始化binInfos对象
        for (int hash = 0; hash < SimpleAnalyticDB.BIN_COUNT; hash++) {
            binInfos[hash] = new BinInfo();
        }

        // 初始化channels的操作在后期有预防，这里不加join()，利用CPU闲置时间来打开所有的文件
        // for (int i = 0; i < threads.length; i++) {
        //     threads[i].join();
        //     threads[i] = null;
        // }
    }

    public OutputFile[] getOutputFiles() {
        return outputFiles;
    }

    // public List<Map<Integer, List<ByteBuffer>>> collectHavenMaps() {
    //     List<Map<Integer, List<ByteBuffer>>> list = new ArrayList<>();
    //     for (OutputFile outputFile : outputFiles) {
    //         list.add(outputFile.getCpuCache().getHavenMap());
    //     }
    //     return list;
    // }

    public long[] collectCpuCacheBufferAddresses() {
        long[] addresses = new long[outputFiles.length];
        for (int i = 0; i < outputFiles.length; i++) {
            OutputFile outputFile = outputFiles[i];
            addresses[i] = outputFile.getCpuCache().getBufferAddress();
        }
        return addresses;
    }

    public List<long[]> collectCpuCacheCurrentAddresses() {
        List<long[]> list = new ArrayList<>();
        for (OutputFile outputFile : outputFiles) {
            list.add(outputFile.getCpuCache().getCurrentAddresses());
        }
        return list;
    }

    public BinInfo[] getBinInfos() {
        return binInfos;
    }

    public ByteBuffer[] getBuffer() {
        return outBuffer;
    }

    public long getRecordCount() {
        return recordCount;
    }

    public void setRecordCount(long recordCount) {
        this.recordCount = recordCount;
    }
}
