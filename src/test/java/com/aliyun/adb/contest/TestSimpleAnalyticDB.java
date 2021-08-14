package com.aliyun.adb.contest;

import com.aliyun.adb.contest.spi.AnalyticDB;
import org.junit.Assert;
import org.junit.Test;

import java.io.*;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;

public class TestSimpleAnalyticDB {

    @Test
    public void testCorrectness() throws Exception {
        File testDataDir = new File("./test_data3");
        File testWorkspaceDir = new File("./target");
        File testResultsFile = new File("./test_result/results");
        List<String> ans = new ArrayList<>();

        try (BufferedReader resReader = new BufferedReader(new FileReader(testResultsFile))) {
            String line;
            while ((line = resReader.readLine()) != null) {
                ans.add(line);
            }
        }

        // ROUND #1
        SimpleAnalyticDB analyticDB1 = new SimpleAnalyticDB();
        analyticDB1.load(testDataDir.getAbsolutePath(), testWorkspaceDir.getAbsolutePath());
        BufferedWriter bufferedWriter = new BufferedWriter(new FileWriter(new File("/media/nhpcc416/新加卷/okks/2021-tianchi-contest-2/test_result/results1")));
        double base = 0.001;
        for(int i = 0; i < 1000; i++)
        {
            String aa = analyticDB1.quantile("lineitem","HELL0_ORDERKEY", base );
            System.out.println("lineitem HELL0_ORDERKEY " + new DecimalFormat("0.000").format(base) + " " + aa + "\n");
            bufferedWriter.write("lineitem HELL0_ORDERKEY " + new DecimalFormat("0.000").format(base) + " " + aa + "\n");
            base += 0.001;
        }
        bufferedWriter.flush();
        bufferedWriter.close();
//        testQuery(analyticDB1, ans, 10);
//
//        // To simulate exiting
//        analyticDB1 = null;
//
//        // ROUND #2
//        SimpleAnalyticDB analyticDB2 = new SimpleAnalyticDB();
//        analyticDB2.load(testDataDir.getAbsolutePath(), testWorkspaceDir.getAbsolutePath());
//
//        Executor testWorkers = Executors.newFixedThreadPool(8);
//
//        CompletableFuture[] futures = new CompletableFuture[8];
//
//        for (int i = 0; i < 8; i++) {
//            futures[i] = CompletableFuture.runAsync(() -> testQuery(analyticDB2, ans, 500), testWorkers);
//        }
//
//        CompletableFuture.allOf(futures).get();
    }

    private void testQuery(AnalyticDB analyticDB, List<String> ans, int testCount) {
        try {
            for (int i = 0; i < testCount; i++) {
                //int p = ThreadLocalRandom.current().nextInt(ans.size());
                String resultStr[] = ans.get(i).split(" ");
                String table = resultStr[0];
                String column = resultStr[1];
                double percentile = Double.valueOf(resultStr[2]);
                String answer = resultStr[3];
                String gotAnswer = analyticDB.quantile(table, column, percentile);
                System.out.println("" + answer.equals(gotAnswer)+ " expected " + answer + " got " + gotAnswer);
                Assert.assertEquals(answer, gotAnswer);
            }
        } catch (Throwable t) {
            throw new RuntimeException(t);
        }
    }

}
