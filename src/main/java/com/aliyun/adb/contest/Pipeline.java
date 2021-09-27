package com.aliyun.adb.contest;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;

import sun.misc.Unsafe;
import sun.nio.ch.DirectBuffer;

public class Pipeline {
    private File workspaceFolder;

    private File[] files;
    private int currFileIndex;

    private AtomicInteger inBufferCountNow;
    private AtomicInteger outBufferCountNow;

    // 生产者 <--> 中间人
    private LinkedBlockingQueue<ByteBuffer> freeQueue;
    // 中间人 <--> 消费者
    private LinkedBlockingQueue<ByteBuffer> packageQueue;
    private Map<String, Map<String, ColumnMetaInfo>> metaInfoMap;

    private Loader loader;
    private Provider[] providers;
    private Middleman[] middlemen;
    private Customer[] customers;

    private Thread loaderThread;
    private Thread[] providerThreads;
    private Thread[] middlemanThreads;
    private Thread[] customerThreads;

    private CyclicBarrier cbProviderReady;
    private CyclicBarrier cbMiddlemanReady;
    private CyclicBarrier cbCustomerReady;
    private CountDownLatch cdlProvider;
    private CountDownLatch cdlMiddleman;
    private CountDownLatch cdlCustomer;

    private Unsafe UNSAFE = UnsafeUtil.UNSAFE;

    public Pipeline(File workspaceFolder, Map<String, Map<String, ColumnMetaInfo>> metaInfoMap) {
        this.workspaceFolder = workspaceFolder;
        this.providers = new Provider[SimpleAnalyticDB.PROVIDER_COUNT];
        this.middlemen = new Middleman[SimpleAnalyticDB.MIDDLEMAN_COUNT];
        this.customers = new Customer[SimpleAnalyticDB.CUSTOMER_COUNT];
        this.providerThreads = new Thread[SimpleAnalyticDB.PROVIDER_COUNT];
        this.middlemanThreads = new Thread[SimpleAnalyticDB.MIDDLEMAN_COUNT];
        this.customerThreads = new Thread[SimpleAnalyticDB.CUSTOMER_COUNT];
        this.freeQueue = new LinkedBlockingQueue<>();
        this.packageQueue = new LinkedBlockingQueue<>();
        this.metaInfoMap = metaInfoMap;
        this.cbProviderReady = new CyclicBarrier(SimpleAnalyticDB.PROVIDER_COUNT + 1);
        this.cbMiddlemanReady = new CyclicBarrier(SimpleAnalyticDB.MIDDLEMAN_COUNT + 1);
        this.cbCustomerReady = new CyclicBarrier(SimpleAnalyticDB.CUSTOMER_COUNT + 1);
    }

    public void init(File[] files) {
        this.files = files;
        this.currFileIndex = 0;
        this.inBufferCountNow = new AtomicInteger(0);
        this.outBufferCountNow = new AtomicInteger(0);
        this.loader = new Loader();
    }

    public void resetProviders(LinkedBlockingQueue<ByteBuffer> taskQueue, TaskInfo[] tasks) {
        this.cdlProvider = new CountDownLatch(SimpleAnalyticDB.PROVIDER_COUNT);
        AtomicInteger taskId = new AtomicInteger();

        for (int i = 0; i < SimpleAnalyticDB.PROVIDER_COUNT; i++) {
            providers[i].init(taskQueue, tasks, taskId);
        }
    }

    public void resetMiddlemen(String tableName, String[] columns, LinkedBlockingQueue<ByteBuffer> taskQueue, int taskCount, LinkedBlockingQueue<BufferWithHash> product1Queue, LinkedBlockingQueue<BufferWithHash> product2Queue, CountDownLatch cdlMiddlemanCompleted) {
        AtomicInteger handleCount = new AtomicInteger();
        this.cdlMiddleman = new CountDownLatch(SimpleAnalyticDB.MIDDLEMAN_COUNT);

        for (int i = 0; i < SimpleAnalyticDB.MIDDLEMAN_COUNT; i++) {
            middlemen[i].init(tableName, columns, taskQueue, taskCount, handleCount, product1Queue, product2Queue, cdlMiddlemanCompleted);
        }
    }

    public void resetCustomers(String tableName, String[] columns, LinkedBlockingQueue<BufferWithHash> product1Queue, LinkedBlockingQueue<BufferWithHash> product2Queue, CountDownLatch cdlMiddlemanCompleted) {
        this.cdlCustomer = new CountDownLatch(SimpleAnalyticDB.CUSTOMER_COUNT);

        for (int i = 0; i < SimpleAnalyticDB.CUSTOMER_COUNT; i++) {
            customers[i].init(tableName, columns, product1Queue, product2Queue, cdlMiddlemanCompleted);
        }
    }

    public void startProviders() throws FileNotFoundException {
        // System.out.println(getNow() + String.format("[Info] Start provider threads. (num: %d)", SimpleAnalyticDB.PROVIDER_COUNT));
        for (int i = 0; i < SimpleAnalyticDB.PROVIDER_COUNT; i++) {
            providers[i] = new Provider(i);
            providerThreads[i] = new Thread(providers[i]);
            providerThreads[i].start();
        }
    }

    public void startMiddlemen() {
        // System.out.println(getNow() + String.format("[Info] Start middleman threads. (num: %d)", SimpleAnalyticDB.MIDDLEMAN_COUNT));
        for (int i = 0; i < SimpleAnalyticDB.MIDDLEMAN_COUNT; i++) {
            middlemen[i] = new Middleman(i);
            middlemanThreads[i] = new Thread(middlemen[i]);
            middlemanThreads[i].start();
        }
    }

    public void startCustomers() {
        // System.out.println(getNow() + String.format("[Info] Start customer threads. (num: %d)", SimpleAnalyticDB.CUSTOMER_COUNT));
        for (int i = 0; i < SimpleAnalyticDB.CUSTOMER_COUNT; i++) {
            customers[i] = new Customer(i);
            customerThreads[i] = new Thread(customers[i]);
            customerThreads[i].start();
        }
    }

    public void start() throws InterruptedException, FileNotFoundException {
        // Loader
        // System.out.println(getNow() + String.format("[Info] Start loader thread."));
        loaderThread = new Thread(loader);
        loaderThread.start();
        
        startProviders();
        startMiddlemen();
        startCustomers();
    }

    public void end() throws InterruptedException, IOException {
        // System.out.println(getNow() + String.format("[Info] Join threads."));
        loaderThread.join();
        // System.out.println(getNow() + String.format("[Info] Loader joined."));
        for (int i = 0; i < SimpleAnalyticDB.PROVIDER_COUNT; i++) {
            providerThreads[i].join();
        }
        // System.out.println(getNow() + String.format("[Info] Provider joined."));
        for (int i = 0; i < SimpleAnalyticDB.MIDDLEMAN_COUNT; i++) {
            middlemanThreads[i].join();
        }
        // System.out.println(getNow() + String.format("[Info] Middlemen joined."));
        for (int i = 0; i < SimpleAnalyticDB.CUSTOMER_COUNT; i++) {
            customerThreads[i].join();
        }
        // System.out.println(getNow() + String.format("[Info] Customers joined."));
        // System.out.println(getNow() + String.format("[Info] All joined. (freeQueue: %d, packageQueue: %d)", freeQueue.size(), packageQueue.size()));
        // System.out.println(getNow() + String.format("[Debug] Joined. (taskQueue: %d, product1Queue: %d, product2Queue: %d)", taskQueue.size(), product1Queue.size(), product2Queue.size()));

        // 统计桶里的件数
        for (String tableName : metaInfoMap.keySet()) {
            Map<String, ColumnMetaInfo> tableMetaInfo = metaInfoMap.get(tableName);
            for (String column : tableMetaInfo.keySet()) {
                OutputFile[] outputFiles = tableMetaInfo.get(column).getOutputFiles();
    
                // 更新数据总件数
                BinInfo[] binInfos = tableMetaInfo.get(column).getBinInfos();
                long startPosition = 0;
                long count;
                for (int i = 0; i < binInfos.length; i++) {
                    binInfos[i].setStartPosition(startPosition);
                    count = outputFiles[0].getFileChannel(i).position() / SimpleAnalyticDB.RESTORE_BYTE_COUNT;
                    binInfos[i].setCount(count);
                    startPosition += count;
                }
                tableMetaInfo.get(column).setRecordCount(startPosition);
                // System.out.println(getNow() + String.format("[Info] [%s] record count: %d.", column, startPosition));
            }
        }
    }

    public class Loader implements Runnable {
        @Override
        public void run() {
            CountDownLatch lastCdlMiddlemanCompleted = null;

            while (currFileIndex < files.length) {
                // Date now = new Date();

                try {
                    // System.out.println(getNow() + String.format("[Info][R%d] Loader is ready.", currFileIndex));

                    final File dataFile = files[currFileIndex];
                    final String tableName = dataFile.getName();

                    final long fileSize = dataFile.length();
                    final File tableFolder = new File(workspaceFolder, tableName);
                    // 计算分割数量(缓存使用率99%)
                    final int taskCount = (int)(dataFile.length() / (int)(SimpleAnalyticDB.IN_BUFFER_SIZE * 0.9999));
                    // 分割数 + 1(多1是为了方便计算)
                    final long[] positions = calcPositions(fileSize, taskCount);
                    final String[] columns = getColumns(dataFile, positions);
                    final ColumnMetaInfo[] columnMetaInfos = new ColumnMetaInfo[columns.length];

                    // 创建任务数组
                    TaskInfo[] tasks = new TaskInfo[taskCount];
                    for (int i = 0; i < taskCount; i++) {
                        tasks[i] = new TaskInfo(positions[i], positions[i + 1]);
                    }

                    Map<String, ColumnMetaInfo> tableMetaInfo = new HashMap<>();
                    metaInfoMap.put(tableName, tableMetaInfo);
            
                    // 创建MetaInfo对象
                    for (int i = 0; i < columns.length; i++) {
                        final String column = columns[i];
                        final File columnFolder = new File(tableFolder, column);
                        columnFolder.mkdirs();

                        columnMetaInfos[i] = new ColumnMetaInfo(column);
                        tableMetaInfo.put(columns[i], columnMetaInfos[i]);
                        // 初始化公有缓存(异步)
                        columnMetaInfos[i].initAsyc(lastCdlMiddlemanCompleted, packageQueue, currFileIndex);
                        // 初始化
                        columnMetaInfos[i].init(columnFolder);
                    }

                    while (cdlProvider != null && cdlProvider.getCount() > 0) {
                        Thread.sleep(SimpleAnalyticDB.SLEEP_MS);
                    }
                        
                    // 任务队列
                    LinkedBlockingQueue<ByteBuffer> taskQueue = new LinkedBlockingQueue<>();

                    // 全部加载完毕，复活Provider
                    resetProviders(taskQueue, tasks);
                    cbProviderReady.await();

                    while (cdlMiddleman != null && cdlMiddleman.getCount() > 0) {
                        Thread.sleep(SimpleAnalyticDB.SLEEP_MS);
                    }

                    LinkedBlockingQueue<BufferWithHash> product1Queue = new LinkedBlockingQueue<>();
                    LinkedBlockingQueue<BufferWithHash> product2Queue = new LinkedBlockingQueue<>();
                    CountDownLatch cdlMiddlemanCompleted = new CountDownLatch(SimpleAnalyticDB.MIDDLEMAN_COUNT);

                    resetMiddlemen(tableName, columns, taskQueue, tasks.length, product1Queue, product2Queue, cdlMiddlemanCompleted);
                    lastCdlMiddlemanCompleted = cdlMiddlemanCompleted;
                    cbMiddlemanReady.await();

                    while (cdlCustomer != null && cdlCustomer.getCount() > 0) {
                        Thread.sleep(SimpleAnalyticDB.SLEEP_MS);
                    }

                    resetCustomers(tableName, columns, product1Queue, product2Queue, cdlMiddlemanCompleted);
                    cbCustomerReady.await();

                    // 重置信号
                    cbProviderReady.reset();
                    cbMiddlemanReady.reset();
                    cbCustomerReady.reset();
                } catch(Exception e) {
                    UNSAFE.throwException(e);
                } finally {
                    // System.out.println(getNow() + String.format("[Info][R%d] Loader completed. (%d)", currFileIndex, (new Date()).getTime() - now.getTime()));
                    currFileIndex++;
                }
            }
        }

        private long[] calcPositions(long fileSize, int taskCount) {
            // 分割数 + 1(多1是为了方便计算)
            final long[] positions = new long[taskCount + 1];

            // 计算位置
            for (int i = 0; i < positions.length; i++) {
                positions[i] = fileSize * i / taskCount;
            }

            // 最后一个位置定位到回车符上，使得处理保持一致
            positions[positions.length - 1]--;

            return positions;
        }

        private String[] getColumns(File dataFile, long[] positions) throws IOException {
            String[] columns = new String[2];
            ByteBuffer detectBuffer = ByteBuffer.allocate(128);
            try (RandomAccessFile raf = new RandomAccessFile(dataFile, "r");
                FileChannel channel = raf.getChannel()) {
                int nRead = channel.read(detectBuffer);
                detectBuffer.flip();
                int j;
                int start = 0;
                for (j = 0; j < nRead; j++) {
                    byte b = detectBuffer.get();
                    if (b == 10) {
                        columns[1] = new String(detectBuffer.array(), start, j - start);
                        positions[0] += j;
                        break;
                    } else if (b == 0x2c) {
                        columns[0] = new String(detectBuffer.array(), start, j);
                        start = j + 1;
                    }
                }
                detectBuffer.clear();
            }
            // System.out.println(getNow() + String.format("[Info] Got columns.(%s, %s)", columns[0], columns[1]));
            return columns;
        }
    }

    public class Provider implements Runnable {
        // 固定不变
        private int id;
        // 每轮都会被覆盖
        private int r_taskCount;
        private TaskInfo[] r_tasks;
        private AtomicInteger r_taskId;
        private LinkedBlockingQueue<ByteBuffer> r_taskQueue;

        public Provider(int id) {
            this.id = id;
        }

        public void init(LinkedBlockingQueue<ByteBuffer> taskQueue, TaskInfo[] tasks, AtomicInteger taskId) {
            this.r_taskQueue = taskQueue;
            this.r_tasks = tasks;
            this.r_taskCount = tasks.length;
            this.r_taskId = taskId;
        }

        @Override
        public void run() {
            for (int r = 0; r < files.length; r++) {
                // Date now = new Date();
            
                try {
                    // 等待加载者和生产者
                    cbProviderReady.await();
                    // System.out.println(getNow() + String.format("[Info][R%d] Provider %d is ready.", r, id));

                    int taskCount = r_taskCount;
                    TaskInfo[] tasks = r_tasks;
                    AtomicInteger taskId = r_taskId;
                    LinkedBlockingQueue<ByteBuffer> taskQueue = r_taskQueue;
                    
                    // 赋值已完成
                    cdlProvider.countDown();

                    try (FileInputStream in = new FileInputStream(files[r]); FileChannel csvFileChannel = in.getChannel()) {
                        while (taskId.get() < taskCount) {
                            int taskIndex = taskId.getAndIncrement();
                            if (taskIndex >= taskCount) {
                                return;
                            }
        
                            // 获得缓存
                            ByteBuffer buffer;
        
                            while ((buffer = freeQueue.poll()) == null) {
                                if (inBufferCountNow.get() < SimpleAnalyticDB.MAX_IN_BUFFER_COUNT) {
                                    inBufferCountNow.getAndIncrement();
                                    // int count = inBufferCountNow.getAndIncrement();
                                    buffer = ByteBuffer.allocateDirect(SimpleAnalyticDB.IN_BUFFER_SIZE);
                                    // if (count + 1 >= SimpleAnalyticDB.MAX_IN_BUFFER_COUNT) {
                                    //     System.out.println(SimpleAnalyticDB.getNow() + String.format("[Info] InBuffer's upper limit reached. (Max: %d)", count + 1));
                                    // }
                                    break;
                                } else {
                                    Thread.sleep(SimpleAnalyticDB.SLEEP_MS);
                                }
                            }
        
                            buffer.clear();
        
                            // 获得任务开始和结束
                            TaskInfo taskInfo = tasks[taskIndex];
                            long startPos = taskInfo.getStartPos();
                            long endPos = taskInfo.getEndPos();
        
                            // 读取数据
                            csvFileChannel.read(buffer, startPos);
        
                            // 精细位置调整
                            int size = (int)(endPos - startPos);
        
                            // 结束位置
                            buffer.clear();
                            buffer.position(size);
                            while (buffer.get() != 10);
                            endPos = startPos + buffer.position();
        
                            // 开始位置
                            buffer.clear();
                            while (buffer.get() != 10);
                            long bufferAddress = ((DirectBuffer) buffer).address() + buffer.position();
                            startPos += buffer.position();
        
                            // buffer有效长度
                            size = (int)(endPos - startPos);
        
                            // 在最后的位置填充8个0，防止越界报错
                            UNSAFE.putLong(bufferAddress + size, 0x3030303030303030L);
        
                            // 设置结束位置
                            buffer.limit(buffer.position() + size);

                            // 插入队列
                            taskQueue.offer(buffer);
                        }
                    }

                } catch (Throwable e) {
                    UNSAFE.throwException(e);
                // } finally {
                //     System.out.println(getNow() + String.format("[Info][R%d] Provider %d completed. (%d)", r, id, (new Date()).getTime() - now.getTime()));
                }
            }
        }
    }

    public class Middleman implements Runnable {
        // 固定不变
        private int id;
        // 每轮都会被覆盖
        private String[] r_columns = new String[2];
        private Map<String, ColumnMetaInfo> r_tableMetaInfo;
        private LinkedBlockingQueue<ByteBuffer> r_taskQueue;
        private int r_taskCount;
        private AtomicInteger r_handleCount;
        private LinkedBlockingQueue<BufferWithHash> r_product1Queue;
        private LinkedBlockingQueue<BufferWithHash> r_product2Queue;
        private CountDownLatch r_cdlMiddlemanCompleted;
    
        public Middleman(int id) {
            this.id = id;
        }

        public void init(String tableName, String[] columns, LinkedBlockingQueue<ByteBuffer> taskQueue, int taskCount, AtomicInteger handleCount, LinkedBlockingQueue<BufferWithHash> product1Queue, LinkedBlockingQueue<BufferWithHash> product2Queue, CountDownLatch cdlMiddlemanCompleted) {
            this.r_columns = columns;
            this.r_tableMetaInfo = metaInfoMap.get(tableName);
            this.r_taskQueue = taskQueue;
            this.r_taskCount = taskCount;
            this.r_handleCount = handleCount;
            this.r_product1Queue = product1Queue;
            this.r_product2Queue = product2Queue;
            this.r_cdlMiddlemanCompleted = cdlMiddlemanCompleted;
        }

        @Override
        public void run() {
            for (int r = 0; r < files.length; r++) {
                Date start = new Date();

                try {
                    // 等待生产者和中间人
                    cbMiddlemanReady.await();
                    // System.out.println(getNow() + String.format("[Info][R%d] Middleman %d is ready.", r, id));

                    // 赋值到局部变量
                    String[] columns = r_columns;
                    Map<String, ColumnMetaInfo> tableMetaInfo = r_tableMetaInfo;
                    LinkedBlockingQueue<ByteBuffer> taskQueue = r_taskQueue;
                    int taskCount = r_taskCount;
                    AtomicInteger handleCount = r_handleCount;
                    LinkedBlockingQueue<BufferWithHash> product1Queue = r_product1Queue;
                    LinkedBlockingQueue<BufferWithHash> product2Queue = r_product2Queue;

                    // 开始正式处理
                    ColumnMetaInfo columnMetaInfo1 = tableMetaInfo.get(columns[0]);
                    ColumnMetaInfo columnMetaInfo2 = tableMetaInfo.get(columns[1]);
                    OutputFile outputFile1 = columnMetaInfo1.getOutputFiles()[id];
                    OutputFile outputFile2 = columnMetaInfo2.getOutputFiles()[id];
                    CountDownLatch cdlMiddlemanCompleted = r_cdlMiddlemanCompleted;

                    outputFile1.init(columnMetaInfo1.getBuffer(), product1Queue, packageQueue, outBufferCountNow);
                    outputFile2.init(columnMetaInfo2.getBuffer(), product2Queue, packageQueue, outBufferCountNow);

                    // 赋值已完成
                    cdlMiddleman.countDown();

                    // 等待公有缓存创建完毕
                    columnMetaInfo1.initJoin1();
                    columnMetaInfo2.initJoin1();

                    while (handleCount.get() < taskCount){
                        ByteBuffer buffer = taskQueue.poll();

                        if (buffer == null) {
                            try {
                                Thread.sleep(SimpleAnalyticDB.SLEEP_MS);
                            } catch (InterruptedException e) {
                                UNSAFE.throwException(e);
                            }
                            continue;
                        }
        
                        // 获得缓存
                        long bufferAddress = ((DirectBuffer) buffer).address() + buffer.position();

                        // string2long处理
                        string2long(bufferAddress, buffer.limit() - buffer.position(), outputFile1, outputFile2);

                        // 将缓存放回空闲队列
                        freeQueue.offer(buffer);

                        // 处理完毕
                        handleCount.incrementAndGet();
                    }

                    // 操作结束
                    cdlMiddlemanCompleted.countDown();

                } catch (Throwable e) {
                    e.printStackTrace();
                    UNSAFE.throwException(e);
                // } finally {
                //     Date end = new Date();
                //     System.out.println(getNow() + String.format("[Info][R%d] Middleman %d completed. (%d)", r, id, end.getTime() - start.getTime()));
                }
            }
        }

        private void string2long(long bufferAddress, long size, OutputFile outputFile1, OutputFile outputFile2) throws IOException {
            long endAddress = bufferAddress + size;
            long firstItemStartAddress = bufferAddress;
            long firstItemEndAddress;
            long secondItemStartAddress;
            long secondItemEndAddress;
            long nextlineStartAddress;
    
            while (firstItemStartAddress < endAddress) {
                // 寻找第二个项目开始的地址
                firstItemEndAddress = firstItemStartAddress + 19;
                while (UNSAFE.getByte(firstItemEndAddress) >= 0x30) {
                    firstItemEndAddress--;
                }
                secondItemStartAddress = firstItemEndAddress + 1;
    
                // 寻找下一行开始的地址
                secondItemEndAddress = secondItemStartAddress + 19;
                while (UNSAFE.getByte(secondItemEndAddress) >= 0x30) {
                    secondItemEndAddress--;
                }
                nextlineStartAddress = secondItemEndAddress + 1;
    
                long value1 = 0;
                int length1 = (int)(firstItemEndAddress - firstItemStartAddress);
                long temp1, temp2, temp3;
    
                if (length1 == 19) {
                    temp1 = UNSAFE.getLong(firstItemStartAddress) & 0x0f0f0f0f0f0f0f0fL; firstItemStartAddress += 8;
                    temp2 = UNSAFE.getLong(firstItemStartAddress) & 0x0f0f0f0f0f0f0f0fL; firstItemStartAddress += 8;
                    temp3 = UNSAFE.getInt(firstItemStartAddress) & 0x000f0f0f;
                    value1 = (byte)(temp1       ) * 1000000000000000000L
                           + (byte)(temp1 >>>  8) * 100000000000000000L
                           + (byte)(temp1 >>> 16) * 10000000000000000L
                           + (byte)(temp1 >>> 24) * 1000000000000000L
                           + (byte)(temp1 >>> 32) * 100000000000000L
                           + (byte)(temp1 >>> 40) * 10000000000000L
                           + (byte)(temp1 >>> 48) * 1000000000000L
                           + (byte)(temp1 >>> 56) * 100000000000L
                           + (byte)(temp2       ) * 10000000000L
                           + (byte)(temp2 >>>  8) * 1000000000L
                           + (byte)(temp2 >>> 16) * 100000000L
                           + (byte)(temp2 >>> 24) * 10000000L
                           + (byte)(temp2 >>> 32) * 1000000L
                           + (byte)(temp2 >>> 40) * 100000L
                           + (byte)(temp2 >>> 48) * 10000L
                           + (byte)(temp2 >>> 56) * 1000L
                           + (byte)(temp3       ) * 100L
                           + (byte)(temp3 >>>  8) * 10L
                           + (byte)(temp3 >>> 16);
                } else if (length1 == 18) {
                    temp1 = UNSAFE.getLong(firstItemStartAddress) & 0x0f0f0f0f0f0f0f0fL; firstItemStartAddress += 8;
                    temp2 = UNSAFE.getLong(firstItemStartAddress) & 0x0f0f0f0f0f0f0f0fL; firstItemStartAddress += 8;
                    temp3 = UNSAFE.getShort(firstItemStartAddress) & 0x0f0f;
                    value1 = (byte)(temp1       ) * 100000000000000000L
                           + (byte)(temp1 >>>  8) * 10000000000000000L
                           + (byte)(temp1 >>> 16) * 1000000000000000L
                           + (byte)(temp1 >>> 24) * 100000000000000L
                           + (byte)(temp1 >>> 32) * 10000000000000L
                           + (byte)(temp1 >>> 40) * 1000000000000L
                           + (byte)(temp1 >>> 48) * 100000000000L
                           + (byte)(temp1 >>> 56) * 10000000000L
                           + (byte)(temp2       ) * 1000000000L
                           + (byte)(temp2 >>>  8) * 100000000L
                           + (byte)(temp2 >>> 16) * 10000000L
                           + (byte)(temp2 >>> 24) * 1000000L
                           + (byte)(temp2 >>> 32) * 100000L
                           + (byte)(temp2 >>> 40) * 10000L
                           + (byte)(temp2 >>> 48) * 1000L
                           + (byte)(temp2 >>> 56) * 100L
                           + (byte)(temp3       ) * 10L
                           + (byte)(temp3 >>>  8);
                } else {
                    while (firstItemEndAddress - firstItemStartAddress >= 8) {
                        temp1 = UNSAFE.getLong(firstItemStartAddress) & 0x0f0f0f0f0f0f0f0fL;
                        value1 = value1 * 100000000 + (byte)(temp1) * 10000000 + (byte)(temp1 >>> 8) * 1000000 + (byte)(temp1 >>> 16) * 100000 + (byte)(temp1 >>> 24) * 10000 + (byte)(temp1 >>> 32) * 1000 + (byte)(temp1 >>> 40) * 100 + (byte)(temp1 >>> 48) * 10 + (byte)(temp1 >>> 56);
                        firstItemStartAddress += 8;
                    }
    
                    // 计算(剩余字节的处理)
                    if (firstItemEndAddress - firstItemStartAddress >= 4) {
                        temp1 = UNSAFE.getInt(firstItemStartAddress) & 0x0f0f0f0f;
                        value1 = value1 * 10000 + (byte)(temp1) * 1000 + (byte)(temp1 >>> 8) * 100 + (byte)(temp1 >>> 16) * 10 + (byte)(temp1 >>> 24);
                        firstItemStartAddress += 4;
                    }
    
                    if (firstItemEndAddress - firstItemStartAddress >= 2) {
                        temp1 = UNSAFE.getShort(firstItemStartAddress) & 0x0f0f;
                        value1 = value1 * 100 + (byte)(temp1) * 10 + (byte)(temp1 >>> 8);
                        firstItemStartAddress += 2;
                    }
    
                    if (firstItemEndAddress - firstItemStartAddress >= 1) {
                        temp1 = UNSAFE.getByte(firstItemStartAddress) & 0x0f;
                        value1 = value1 * 10 + (byte)(temp1);
                    }
                }
    
                long value2 = 0;
                int length2 = (int)(secondItemEndAddress - secondItemStartAddress);
                long temp4, temp5, temp6;
    
                if (length2 == 19) {
                    temp4 = UNSAFE.getLong(secondItemStartAddress) & 0x0f0f0f0f0f0f0f0fL; secondItemStartAddress += 8;
                    temp5 = UNSAFE.getLong(secondItemStartAddress) & 0x0f0f0f0f0f0f0f0fL; secondItemStartAddress += 8;
                    temp6 = UNSAFE.getInt(secondItemStartAddress) & 0x000f0f0f;
                    value2 = (byte)(temp4       ) * 1000000000000000000L
                           + (byte)(temp4 >>>  8) * 100000000000000000L
                           + (byte)(temp4 >>> 16) * 10000000000000000L
                           + (byte)(temp4 >>> 24) * 1000000000000000L
                           + (byte)(temp4 >>> 32) * 100000000000000L
                           + (byte)(temp4 >>> 40) * 10000000000000L
                           + (byte)(temp4 >>> 48) * 1000000000000L
                           + (byte)(temp4 >>> 56) * 100000000000L
                           + (byte)(temp5       ) * 10000000000L
                           + (byte)(temp5 >>>  8) * 1000000000L
                           + (byte)(temp5 >>> 16) * 100000000L
                           + (byte)(temp5 >>> 24) * 10000000L
                           + (byte)(temp5 >>> 32) * 1000000L
                           + (byte)(temp5 >>> 40) * 100000L
                           + (byte)(temp5 >>> 48) * 10000L
                           + (byte)(temp5 >>> 56) * 1000L
                           + (byte)(temp6       ) * 100L
                           + (byte)(temp6 >>>  8) * 10L
                           + (byte)(temp6 >>> 16);
                } else if (length2 == 18) {
                    temp4 = UNSAFE.getLong(secondItemStartAddress) & 0x0f0f0f0f0f0f0f0fL; secondItemStartAddress += 8;
                    temp5 = UNSAFE.getLong(secondItemStartAddress) & 0x0f0f0f0f0f0f0f0fL; secondItemStartAddress += 8;
                    temp6 = UNSAFE.getShort(secondItemStartAddress) & 0x0f0f;
                    value2 = (byte)(temp4       ) * 100000000000000000L
                           + (byte)(temp4 >>>  8) * 10000000000000000L
                           + (byte)(temp4 >>> 16) * 1000000000000000L
                           + (byte)(temp4 >>> 24) * 100000000000000L
                           + (byte)(temp4 >>> 32) * 10000000000000L
                           + (byte)(temp4 >>> 40) * 1000000000000L
                           + (byte)(temp4 >>> 48) * 100000000000L
                           + (byte)(temp4 >>> 56) * 10000000000L
                           + (byte)(temp5       ) * 1000000000L
                           + (byte)(temp5 >>>  8) * 100000000L
                           + (byte)(temp5 >>> 16) * 10000000L
                           + (byte)(temp5 >>> 24) * 1000000L
                           + (byte)(temp5 >>> 32) * 100000L
                           + (byte)(temp5 >>> 40) * 10000L
                           + (byte)(temp5 >>> 48) * 1000L
                           + (byte)(temp5 >>> 56) * 100L
                           + (byte)(temp6       ) * 10L
                           + (byte)(temp6 >>>  8);
                } else {
                    while (secondItemEndAddress - secondItemStartAddress >= 8) {
                        temp4 = UNSAFE.getLong(secondItemStartAddress) & 0x0f0f0f0f0f0f0f0fL;
                        value2 = value2 * 100000000 + (byte)(temp4) * 10000000 + (byte)(temp4 >>> 8) * 1000000 + (byte)(temp4 >>> 16) * 100000 + (byte)(temp4 >>> 24) * 10000 + (byte)(temp4 >>> 32) * 1000 + (byte)(temp4 >>> 40) * 100 + (byte)(temp4 >>> 48) * 10 + (byte)(temp4 >>> 56);
                        secondItemStartAddress += 8;
                    }
    
                    // 计算(剩余字节的处理)
                    if (secondItemEndAddress - secondItemStartAddress >= 4) {
                        temp4 = UNSAFE.getInt(secondItemStartAddress) & 0x0f0f0f0f;
                        value2 = value2 * 10000 + (byte)(temp4) * 1000 + (byte)(temp4 >>> 8) * 100 + (byte)(temp4 >>> 16) * 10 + (byte)(temp4 >>> 24);
                        secondItemStartAddress += 4;
                    }
    
                    if (secondItemEndAddress - secondItemStartAddress >= 2) {
                        temp4 = UNSAFE.getShort(secondItemStartAddress) & 0x0f0f;
                        value2 = value2 * 100 + (byte)(temp4) * 10 + (byte)(temp4 >>> 8);
                        secondItemStartAddress += 2;
                    }
    
                    if (secondItemEndAddress - secondItemStartAddress >= 1) {
                        temp4 = UNSAFE.getByte(secondItemStartAddress) & 0x0f;
                        value2 = value2 * 10 + (byte)(temp4);
                    }
                }
    
                outputFile1.writeToCPUCache(value1, (int)(value1 >>> SimpleAnalyticDB.BIT_SHIFT_COUNT));
                outputFile2.writeToCPUCache(value2, (int)(value2 >>> SimpleAnalyticDB.BIT_SHIFT_COUNT));
    
                firstItemStartAddress = nextlineStartAddress;
            }
        }
    }

    public class Customer implements Runnable {
        // 固定不变
        private int id;
        private int sectionCount;
        // 每轮都会被覆盖
        private String[] r_columns;
        private Map<String, ColumnMetaInfo> r_tableMetaInfo;
        private LinkedBlockingQueue<BufferWithHash> r_product1Queue;
        private LinkedBlockingQueue<BufferWithHash> r_product2Queue;
        private CountDownLatch r_cdlMiddlemanCompleted;
    
        public Customer(int id) {
            this.id = id;
            this.sectionCount = SimpleAnalyticDB.BIN_COUNT / SimpleAnalyticDB.CUSTOMER_COUNT;
        }

        public void init(String tableName, String[] columns, LinkedBlockingQueue<BufferWithHash> product1Queue, LinkedBlockingQueue<BufferWithHash> product2Queue, CountDownLatch cdlMiddlemanCompleted) {
            this.r_columns = columns;
            this.r_tableMetaInfo = metaInfoMap.get(tableName);
            this.r_product1Queue = product1Queue;
            this.r_product2Queue = product2Queue;
            this.r_cdlMiddlemanCompleted = cdlMiddlemanCompleted;
        }

        @Override
        public void run() {
            for (int r = 0; r < files.length; r++) {
                // Date now = new Date();

                try {
                    // 等待中间人与消费者
                    cbCustomerReady.await();
                    // System.out.println(getNow() + String.format("[Info][R%d] Customer %d is ready.", r, id));

                    // 赋值到局部变量
                    String[] columns = r_columns;
                    Map<String, ColumnMetaInfo> tableMetaInfo = r_tableMetaInfo;
                    ColumnMetaInfo columnMetaInfo1 = tableMetaInfo.get(columns[0]);
                    ColumnMetaInfo columnMetaInfo2 = tableMetaInfo.get(columns[1]);
                    LinkedBlockingQueue<BufferWithHash> product1Queue = r_product1Queue;
                    LinkedBlockingQueue<BufferWithHash> product2Queue = r_product2Queue;

                    // 正式处理
                    CountDownLatch cdlMiddlemanCompleted = r_cdlMiddlemanCompleted;
                    LinkedBlockingQueue<BufferWithHash> productQueue;
                    OutputFile outputFile;
    
                    if ((id & 0x01) == 0) {
                        productQueue = product1Queue;
                        outputFile = tableMetaInfo.get(columns[0]).getOutputFiles()[0];
                    } else {
                        productQueue = product2Queue;
                        outputFile = tableMetaInfo.get(columns[1]).getOutputFiles()[0];
                    }

                    // 赋值已完成
                    cdlCustomer.countDown();
    
                    // 等待桶文件全部打开
                    columnMetaInfo1.initJoin2();
                    columnMetaInfo2.initJoin2();

                    BufferWithHash bwh;
                    while (((bwh = productQueue.poll()) != null) || (cdlMiddlemanCompleted.getCount() > 0)) {
                        if (bwh == null) {
                            try {
                                Thread.sleep(SimpleAnalyticDB.SLEEP_MS);
                            } catch (InterruptedException e) {
                                UNSAFE.throwException(e);
                            }
                            continue;
                        }
    
                        FileChannel channel = outputFile.getFileChannel(bwh.getHash());
                        channel.write(bwh.getBuffer());
                        packageQueue.offer(bwh.getBuffer());
                    }

                    // System.out.println(getNow() + String.format("[Info][R%d] end() start.", r));
                    end(tableMetaInfo, columns, r);
                    // System.out.println(getNow() + String.format("[Info][R%d] end() end.", r));
    
                } catch (Throwable e) {
                    UNSAFE.throwException(e);
                // } finally {
                //     System.out.println(getNow() + String.format("[Info][R%d] Customer %d completed. (%d)", r, id, (new Date()).getTime() - now.getTime()));
                }
            }
        }
    
        private void end(Map<String, ColumnMetaInfo> tableMetaInfo, String[] columns, int round) throws IOException, InterruptedException {
            // 因为数据已经按桶存放在各处，后续工作每个线程分段处理，防止抢占资源
            int startHash = id * sectionCount;
            int endHash = startHash + sectionCount;

            if (id + 1 == SimpleAnalyticDB.CUSTOMER_COUNT) {
                endHash = SimpleAnalyticDB.BIN_COUNT;
            }

            // 写入剩余避难所数据
            ColumnMetaInfo columnMetaInfo = tableMetaInfo.get(columns[0]);
            OutputFile outputFile1 = columnMetaInfo.getOutputFiles()[id];

            // 将Cpu Cache写入公有Buffer
            long[] cpuCacheBufferAddresses = columnMetaInfo.collectCpuCacheBufferAddresses();
            List<long[]> cpuCacheCurrentAddresses = columnMetaInfo.collectCpuCacheCurrentAddresses();

            for (int threadId = 0; threadId < SimpleAnalyticDB.CUSTOMER_COUNT; threadId++) {
                long bufferAddresses = cpuCacheBufferAddresses[threadId];
                long[] currentAddresses = cpuCacheCurrentAddresses.get(threadId);

                for (int hash = startHash; hash < endHash; hash++) {
                    outputFile1.writeBuffer(hash, bufferAddresses + hash * SimpleAnalyticDB.CPU_CACHE_ALLOCATE_SIZE, currentAddresses[hash]);
                }
            }

            // 将公有Buffer数据写入
            ByteBuffer[] inBuffer = outputFile1.getBuffer();
            AtomicInteger[] currentPosArray = outputFile1.getCurrentPosArray();

            for (int hash = startHash; hash < endHash; hash++) {
                int currentPos = currentPosArray[hash].get();

                inBuffer[hash].position(0);
                inBuffer[hash].limit(currentPos);
                outputFile1.getFileChannel(hash).write(inBuffer[hash]);
                inBuffer[hash].clear();
            }

            // 回收缓存
            if (round == 0) {
                columnMetaInfo.recycle(packageQueue, startHash, endHash);
            }

            // 写入剩余避难所数据
            columnMetaInfo = tableMetaInfo.get(columns[1]);
            OutputFile outputFile2 = columnMetaInfo.getOutputFiles()[id];

            // 将Cpu Cache写入公有Buffer
            cpuCacheBufferAddresses = columnMetaInfo.collectCpuCacheBufferAddresses();
            cpuCacheCurrentAddresses = columnMetaInfo.collectCpuCacheCurrentAddresses();

            for (int threadId = 0; threadId < SimpleAnalyticDB.CUSTOMER_COUNT; threadId++) {
                long bufferAddresses = cpuCacheBufferAddresses[threadId];
                long[] currentAddresses = cpuCacheCurrentAddresses.get(threadId);

                for (int hash = startHash; hash < endHash; hash++) {
                    outputFile2.writeBuffer(hash, bufferAddresses + hash * SimpleAnalyticDB.CPU_CACHE_ALLOCATE_SIZE, currentAddresses[hash]);
                }
            }

            // 将公有Buffer数据写入
            inBuffer = outputFile2.getBuffer();
            currentPosArray = outputFile2.getCurrentPosArray();

            for (int hash = startHash; hash < endHash; hash++) {
                int currentPos = currentPosArray[hash].get();

                inBuffer[hash].position(0);
                inBuffer[hash].limit(currentPos);
                outputFile2.getFileChannel(hash).write(inBuffer[hash]);
                inBuffer[hash].clear();
            }

            // 回收缓存
            if (round == 0) {
                columnMetaInfo.recycle(packageQueue, startHash, endHash);
            }
        }
    }

    // // 工具函数
    // private String getNow() {
    //     Calendar cal = Calendar.getInstance();
    //     return Integer.toString(cal.get(Calendar.YEAR)) + "/"
    //             + Integer.toString(cal.get(Calendar.MONTH) + 1) + "/"
    //             + Integer.toString(cal.get(Calendar.DATE)) + " "
    //             + Integer.toString(cal.get(Calendar.HOUR_OF_DAY)) + ":"
    //             + Integer.toString(cal.get(Calendar.MINUTE)) + ":"
    //             + Integer.toString(cal.get(Calendar.SECOND)) + "."
    //             + Integer.toString(cal.get(Calendar.MILLISECOND)) + " ";
    // }
}
