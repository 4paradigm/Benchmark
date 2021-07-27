package com._4paradigm.benchmark.memory;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
public class MemoryBenchmark {
    private ExecutorService executorService;
    private int threadNum;
    private String method;
    public MemoryBenchmark() {
        try {
            Properties prop = new Properties();
            prop.load(MemoryBenchmark.class.getClassLoader().getResourceAsStream("benchmark.properties"));
            threadNum = Integer.parseInt(prop.getProperty("thread_num", "1"));
            method = prop.getProperty("method");
        } catch (Exception e) {
            e.printStackTrace();
        }
        executorService = Executors.newFixedThreadPool(threadNum);
    }
    public void run() {
        Test tmp = null;
        if (method.equals("rtidb")) {
            tmp = new FedbTest();
        } else if (method.equals("redis")) {
            tmp = new RedisTest();
        } else {
            tmp = new JDBCSqlTest();
        }
        if (!tmp.init()) {
            System.out.println("init failed");
            return;
        }
        final Test test = tmp;
        for (int i = 0; i < threadNum; i++) {
            System.out.println("create thread " + String.valueOf(i));
            executorService.submit(new Runnable() {
                @Override
                public void run() {
                    test.put();
                }
            });
        }
    }

    public static void main(String[] args) {
        MemoryBenchmark benchmark = new MemoryBenchmark();
        benchmark.run();
    }
}
