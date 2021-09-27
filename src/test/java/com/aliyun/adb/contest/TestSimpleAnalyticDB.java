package com.aliyun.adb.contest;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import org.junit.Assert;
import org.junit.Test;

public class TestSimpleAnalyticDB {
    @Test
    public void run_test() throws Exception {
        SimpleAnalyticDB sadb = new SimpleAnalyticDB();
        sadb.load("tc_data", "user_data");
        AnswerInfo[] answers = pickupAnswers("3ww_line_item", 10);
        for (AnswerInfo answer : answers) {
            String actual = answer.getAnswer();
            String expected = sadb.quantile(answer.getTableName(), answer.getColumnName(), answer.getPercentile());
            System.out.println(String.format("ACTUAL = %s, EXPECTED = %s", actual, expected));
            Assert.assertEquals(expected, actual);
        }
    }

    @Test
    public void run_test_load() throws Exception {
        SimpleAnalyticDB sadb = new SimpleAnalyticDB();
        sadb.load("tc_data", "user_data");
    }

    @Test
    public void run_checkBins() throws Exception {
        int nBin = 1024;
        File workspaceFolder = new File("user_data");
        ByteBuffer buffer = ByteBuffer.allocateDirect(4096);
        buffer.order(ByteOrder.nativeOrder());

        for (File table : workspaceFolder.listFiles()) {
            if (table.isDirectory()) {
                for (File column : table.listFiles()) {
                    int count = 0;
                    for (int i = 0; i < nBin; i++) {
                        File binFile = new File(column, String.format("%d.data", i));
                        try (FileChannel channel = FileChannel.open(binFile.toPath(), StandardOpenOption.READ)) {
                            while (channel.read(buffer) > 0) {
                                buffer.flip();
                                while (buffer.hasRemaining()) {
                                    assert (buffer.getLong() >>> 53) == i;
                                    count++;
                                }
                                buffer.clear();
                            }
                        }
                    }
                    assert count == 300000000;
                }
            }
        }
    }

    private AnswerInfo[] pickupAnswers(String tableName, int count) throws Exception {
        AnswerInfo[] ret = new AnswerInfo[count];
        File answerFile = new File("test_data", tableName + "_answer.txt");
        List<String> answers = new ArrayList<String>();
        Random rand = new Random();
        try (BufferedReader reader = new BufferedReader(new FileReader(answerFile))) {
            String line = null;
            while ((line = reader.readLine()) != null) {
                answers.add(line);
            }
        }

        for (int i = 0; i < count; i++) {
            int idx = rand.nextInt(answers.size());
            String[] values = answers.get(idx).split("\t");
            AnswerInfo answer = new AnswerInfo();
            answer.setTableName(tableName);
            answer.setColumnName(values[0]);
            answer.setPercentile(Double.parseDouble(values[1]));
            answer.setAnswer(values[2]);
            ret[i] = answer;
        }

        return ret;
    }

    private class AnswerInfo {
        private String tableName;
        private String columnName;
        private double percentile;
        private String answer;
        public String getTableName() {
            return tableName;
        }
        public void setTableName(String tableName) {
            this.tableName = tableName;
        }
        public String getColumnName() {
            return columnName;
        }
        public void setColumnName(String columnName) {
            this.columnName = columnName;
        }
        public double getPercentile() {
            return percentile;
        }
        public void setPercentile(double percentile) {
            this.percentile = percentile;
        }
        public String getAnswer() {
            return answer;
        }
        public void setAnswer(String answer) {
            this.answer = answer;
        }
    }

    private static double nano2sec(long startTime) {
        return (double)(System.nanoTime() - startTime) / 1000000000L;
    }
}
