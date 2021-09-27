package com.aliyun.adb.contest;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileChannel.MapMode;
import java.nio.file.Path;
import java.util.Calendar;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.atomic.AtomicInteger;

import com.aliyun.adb.contest.spi.AnalyticDB;

public class SimpleAnalyticDB implements AnalyticDB {

    private File workspaceFolder;
    final public static int RESTORE_BYTE_COUNT = 7;
    // 件数需要和OUT_BUFFER_SIZE成倍数关系，否则程序会无法正常执行
    final public static int RECORD_COUNT_IN_CACHE_PER_BIN = 128;
    final public static int IN_BUFFER_SIZE = 1024 * 1024 * 6;
    final public static int OUT_BUFFER_SIZE = 4096 * RESTORE_BYTE_COUNT * 4;
    // final public static int BIT_FOR_HASH = 9;
    final public static int BIT_FOR_HASH = 11;
    final public static int BIT_SHIFT_COUNT = 63 - BIT_FOR_HASH;
    final public static int BIN_COUNT = (1 << BIT_FOR_HASH);
    final public static int PROVIDER_COUNT = 6;
    final public static int MIDDLEMAN_COUNT = 6;
    final public static int CUSTOMER_COUNT = 6;
    final public static int MAX_IN_BUFFER_COUNT = 64;
    final public static int MAX_OUT_BUFFER_COUNT = 4000;
    final public static int SLEEP_MS = 12;
    // 桶的最大大小，使用数据量最多的桶数
    private static long BIN_MAX_RECORD_COUNT;
    // CPU Cache大小
    final public static int CPU_CACHE_ACTUAL_SIZE = RECORD_COUNT_IN_CACHE_PER_BIN * RESTORE_BYTE_COUNT;
    final public static int CPU_CACHE_ALLOCATE_SIZE = CPU_CACHE_ACTUAL_SIZE + 4;
    // final public static int RECORD_COUNT_IN_OUT_BUFFER = OUT_BUFFER_SIZE / RESTORE_BYTE_COUNT;
    // final public static int CACHE_SIZE_PER_BIN = RECORD_COUNT_IN_CACHE_PER_BIN * RESTORE_BYTE_COUNT;
    // 以下变量需要在kill -9 后重建
    private Map<String, Map<String, ColumnMetaInfo>> metaInfoMap = new HashMap<>();
    // 查询用
    ThreadLocal<ByteBuffer> readBuffers = new ThreadLocal<>();
    ThreadLocal<long[]> binArrays = new ThreadLocal<>();
    // 全局唯一
    static Pipeline pipeline;

    // Debug
    // // ========= 正式投递需要注释掉部分 =========
    // final private static int query_limit = 3900;
    // private static AtomicInteger query_count = new AtomicInteger(0);
    // // ========================================

    /**
     *
     * The implementation must contain a public no-argument constructor.
     *
     */
    public SimpleAnalyticDB() {
    }

    @Override
    public void load(String tpchDataFileDir, String workspaceDir) throws Exception {
        // Tool.recordPrint();
        File dir = new File(tpchDataFileDir);
        workspaceFolder = new File(workspaceDir);
        File readyFlag = new File(workspaceFolder, "ready");

        if (!readyFlag.exists()) {
            System.out.println(
                getNow() + String.format("[Info] nThread(provider, middleman, customer)=(%d, %d, %d), nBin=%d, nRestoreBytes=%d, inBuffer=%d, outBuffer=%d, recordCountInCachePerBin=%d",
                PROVIDER_COUNT,
                MIDDLEMAN_COUNT,
                CUSTOMER_COUNT,
                BIN_COUNT,
                RESTORE_BYTE_COUNT,
                IN_BUFFER_SIZE,
                OUT_BUFFER_SIZE,
                RECORD_COUNT_IN_CACHE_PER_BIN
                ));
            
            // 初始化流水线
            pipeline = new Pipeline(workspaceFolder, metaInfoMap);
            
            // 初始化文件列表
            pipeline.init(dir.listFiles());

            // 开始流水线操作
            // System.out.println(getNow() + String.format("[Info] Start pipeline."));
            pipeline.start();

            // 等待流水线操作结束
            pipeline.end();
            // System.out.println(getNow() + String.format("[Info] Pipeline stoped."));

            writeMetaInfo();
            // System.out.println(getNow() + "[Info] Metainfo is written.");
            System.out.println(getNow() + "[Info] All is ready.");
            // 创建空文件代表数据已处理完毕
            readyFlag.createNewFile();
            // Tool.doSout();
        } else {
            // kill -9后的重新加载处理
            // System.out.println(getNow() + "[Info] Loading from metaInfo.");
            readMetaInfo();
            System.out.println(getNow() + "[Info] Loaded.");
        }
    }

    @Override
    public String quantile(String table, String column, double percentile) throws Exception {
        // // ========= 正式投递需要注释掉部分 =========
        // Date now = new Date();
        // System.out.println(getNow() + String.format("[Info] Query Start. ([%s][%s] percentile: %f)", table, column, percentile));
        // // ========================================
        ColumnMetaInfo columnMetaInfo = metaInfoMap.get(table).get(column);

        String ret = null;

        // 计算位置
        long row_count = columnMetaInfo.getRecordCount();
        long idx = (long) Math.round(row_count * percentile) - 1;
        BinInfo[] binInfos = columnMetaInfo.getBinInfos();
        int binCount = binInfos.length;
        OutputFile outputFile = columnMetaInfo.getOutputFiles()[0];

        // 计算哪个桶
        int hash = (int)(idx * binCount / row_count);
        int hitRet = binInfos[hash].isHit2(idx);
        if (hitRet < 0) {
            while (!binInfos[--hash].isHit(idx));
        } else if (hitRet > 0) {
            while (!binInfos[++hash].isHit(idx));
        }

        BinInfo binInfo = binInfos[hash];
        int innerIdx = (int)(idx - binInfo.getStartPosition());

        // 读缓存，根据线程只创建一次
        ByteBuffer readBuffer = readBuffers.get();
        if (readBuffer == null) {
            readBuffer = ByteBuffer.allocateDirect((int)BIN_MAX_RECORD_COUNT * RESTORE_BYTE_COUNT);
            readBuffers.set(readBuffer);
        }

        // 传给quickSelect用的数组，根据线程只创建一次
        long[] values = binArrays.get();
        if (values == null) {
            values = new long[(int)BIN_MAX_RECORD_COUNT];
            binArrays.set(values);
        }

        // 计算要加载多少块数据
        outputFile.pickupData(hash, values, readBuffer, (int)binInfo.getCount());

        // quickSelect的K值从1开始，而传入的是索引，所以此处需要+1
        // long answer = quickSelect(values, 0, (int)binInfo.getCount() - 1, innerIdx + 1);
        int answerIndex = quickSelectV3(values, 0, (int)binInfo.getCount() - 1, innerIdx + 1);
        long answer = values[answerIndex];

        answer |= ((long)hash << BIT_SHIFT_COUNT);
        ret = Long.toString(answer);

        // ========= 正式投递需要注释掉部分 =========
        // System.out.println(getNow() + String.format("[Info] Query End. ([%s][%s] percentile: %f, hash: %d, answer: %d) (%d)", table, column, percentile, hash, answer, (new Date()).getTime() - now.getTime()));

        // // [For Debug]
        // if (query_count.incrementAndGet() > query_limit) {
        //     throw new Exception(getNow() + "[Debug] Performance Test");
        // }
        // // ========================================
        return ret;
    }

    // 序列化到metaInfo文件中
    private void writeMetaInfo() throws IOException, InterruptedException {
        Queue<Thread> threads = new LinkedList<>();
        long[] maxCounts = new long[4];
        int i = 0;

        for (String tableName : metaInfoMap.keySet()) {
            Map<String, ColumnMetaInfo> tableMetaInfo = metaInfoMap.get(tableName);
            File tableFolder = new File(workspaceFolder, tableName);
            for (String columnName : tableMetaInfo.keySet()) {
                ColumnMetaInfo columnMetaInfo = tableMetaInfo.get(columnName);
                File columnFolder = new File(tableFolder, columnName);
                final int index = i++;

                Thread thread = new Thread(new Runnable(){
                    @Override
                    public void run() {
                        try {
                            File file = new File(columnFolder, "metaInfo");
                            try (RandomAccessFile raf = new RandomAccessFile(file, "rw"); FileChannel channel = raf.getChannel()) {
                                MappedByteBuffer buffer = channel.map(MapMode.READ_WRITE, 0, 2048 * Long.BYTES + 4096);
                                BinInfo[] binInfos = columnMetaInfo.getBinInfos();
                                long maxCount = 0;

                                // 全局信息
                                // 记录数
                                buffer.putLong(columnMetaInfo.getRecordCount());

                                // 桶个数(直接使用1 << BIT_FOR_HASH)
                                // 所有桶的信息
                                for (int i = 0; i < binInfos.length; i++) {
                                    // 写入桶大小
                                    long count = binInfos[i].getCount();
                                    buffer.putLong(count);
                                    // 记录最大桶的元素数
                                    maxCount = Math.max(maxCount, count);
                                }

                                // 记录拥有最多元素的桶的元素数量
                                buffer.putLong(maxCount);
                                maxCounts[index] = maxCount;
                            }
                        } catch (Throwable e) {
                            UnsafeUtil.UNSAFE.throwException(e);
                        }
                    }
                });
                thread.start();
                threads.offer(thread);
            }
        }

        Thread thread;
        while ((thread = threads.poll()) != null) {
            thread.join();
            thread = null;
        }

        setBinMaxRecordCount(maxCounts);
    }

    // 从metaInfo文件中读取文件
    private void readMetaInfo() throws IOException, ClassNotFoundException, InterruptedException {
        // 桶数
        Queue<Thread> threads = new LinkedList<>();
        long[] maxCounts = new long[4];
        int i = 0;

        for (File tableFolder : workspaceFolder.listFiles())
        {
            if (!tableFolder.isDirectory()) {
                continue;
            }

            String tableName = tableFolder.getName();
            Map<String, ColumnMetaInfo> tableMetaInfo = new HashMap<>();
            metaInfoMap.put(tableName, tableMetaInfo);

            for (File columnFolder : tableFolder.listFiles()) {
                final int index = i++;
                String columnName = columnFolder.getName();
                ColumnMetaInfo columnMetaInfo = new ColumnMetaInfo(columnName);
                tableMetaInfo.put(columnName, columnMetaInfo);

                Thread thread = new Thread(new Runnable(){
                    @Override
                    public void run() {
                        try {
                            // 读取文件
                            File file = new File(columnFolder, "metaInfo");
                            try (RandomAccessFile raf = new RandomAccessFile(file, "r"); FileChannel channel = raf.getChannel()) {
                                MappedByteBuffer buffer = channel.map(MapMode.READ_ONLY, 0, file.length());
                                // 记录数
                                columnMetaInfo.setRecordCount(buffer.getLong());

                                // 所有桶的信息
                                long startPos = 0;
                                BinInfo[] binInfos = columnMetaInfo.getBinInfos();
                                for (int i = 0; i < SimpleAnalyticDB.BIN_COUNT; i++) {
                                    BinInfo binInfo = new BinInfo();
                                    binInfos[i] = binInfo;
                                    long count = buffer.getLong();
                                    binInfo.setStartPosition(startPos);
                                    binInfo.setCount(count);
                                    startPos += count;
                                }
                                // 最大桶的元素数
                                maxCounts[index] = buffer.getLong();

                                Path pathes[] = new Path[SimpleAnalyticDB.BIN_COUNT];
                                for (int i = 0; i < SimpleAnalyticDB.BIN_COUNT; i++) {
                                    pathes[i] = new File(columnFolder, String.format("%d.data", i)).toPath();
                                }

                                OutputFile[] outputFiles = columnMetaInfo.getOutputFiles();
                                for (int i = 0; i < MIDDLEMAN_COUNT; i++) {
                                    OutputFile outputFile = new OutputFile(i, pathes);
                                    outputFiles[i] = outputFile;
                                }
                            }
                        } catch (Throwable e) {
                            UnsafeUtil.UNSAFE.throwException(e);
                        }
                    }
                });
                thread.start();
                threads.offer(thread);
            }
        }

        Thread thread;
        while ((thread = threads.poll()) != null) {
            thread.join();
            thread = null;
        }

        setBinMaxRecordCount(maxCounts);
    }

    private void setBinMaxRecordCount(long[] maxCounts) {
        for (int i = 0; i < maxCounts.length; i++) {
            BIN_MAX_RECORD_COUNT = Math.max(BIN_MAX_RECORD_COUNT, maxCounts[i]);
        }
    }

    public static long quickSelect(long[] values, int start, int end, int k) {
        if (start == end) {
            return values[start];
        }
        int left = start;
        int right = end;
        long pivot = values[(start + end) >> 1];
        while (left <= right) {
            while (left <= right && values[left] < pivot) {
                left++;
            }

            while (left <= right && values[right] > pivot) {
                right--;
            }

            if (left <= right) {
                long temp = values[left];
                values[left] = values[right];
                values[right] = temp;
                left++;
                right--;
            }
        }
        if (start + k - 1 <= right) {
            return quickSelect(values, start, right, k);
        } else if (start + k - 1 >= left) {
            return quickSelect(values, left, end, k - (left - start));
        } else {
            return values[right + 1];
        }
    }

    public static int quickSelectV3(long[] values, int start, int end, int k) {

        long pivot;
        int left, right;

        while (true) {
            int length = end - start + 1;
            if (length > 1000) {
                int sampleNums = length / 100;

                double p = (k - start - 1.0) / length;

                if (p < 0.4) {
                    p += 0.05;
                } else if (p > 0.6) {
                    p -= 0.05;
                }

                int sampleK = (int)(Math.max(sampleNums * p, 1) + start);
                // int sampleK = Math.max(sampleNums * (k - start - 1) / length, 1) + start;

                left = quickSelectV3(values, start, start + sampleNums, sampleK);
                pivot = values[left];
            } else {
                pivot = values[start];
                left = start;
            }

            right = end;
            while (left < right) {
                while (values[right] > pivot) right--;
                values[left] = values[right];

                while (left < right && values[left] <= pivot) left++;
                values[right] = values[left];
            }

            values[left] = pivot;

            if (left < k - 1) {
                start = left + 1;
            } else if (left > k - 1) {
                end = left - 1;
            } else {
                return left;
            }
        }
    }
    
    public static void vmstat() throws Exception {
        String tag = "=";
        for (int i = 0; i < 5; i++) {
            tag += tag;
        }
        String statTag = getNow() + " [Info] " +  tag + " vmstat " + tag;
        String stopTag = getNow() + " [Info] " +  tag + "========" + tag;
        System.out.println(statTag);
        String command = "vmstat";
        Process process = Runtime.getRuntime().exec(command);

        InputStream inputStream = process.getInputStream();
        BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream));
        process.waitFor();

        String s = null;
        while ((s = reader.readLine()) != null) {
            System.out.println(getNow() + " [Info] " + s);
        }
        System.out.println(stopTag);
    }

    // 工具函数
    public static String getNow() {
        Calendar cal = Calendar.getInstance();
        return Integer.toString(cal.get(Calendar.YEAR)) + "/"
                + Integer.toString(cal.get(Calendar.MONTH) + 1) + "/"
                + Integer.toString(cal.get(Calendar.DATE)) + " "
                + Integer.toString(cal.get(Calendar.HOUR_OF_DAY)) + ":"
                + Integer.toString(cal.get(Calendar.MINUTE)) + ":"
                + Integer.toString(cal.get(Calendar.SECOND)) + "."
                + Integer.toString(cal.get(Calendar.MILLISECOND)) + " ";
    }
}