package com._4paradigm.benchmark.memory;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.Statement;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

class SingleStoreTest {
    private static String singlestoreUrl;
    private static String tableName;
    private static String dbName;
    private static int threadNum;
    private static int pkCnt;
    private static int tsCnt;
    private static String baseKey;
    private static boolean needCreate;
    private Connection cnn;
    private ExecutorService executorService;
    private String createDDL = "create table " + tableName + " (col1 varchar(20), col2 bigint, " +
            "col3 float," +
            "col4 double," +
            "col5 varchar(12)," +
            "KEY (col1, col2));";
    private String sql = "insert into " + tableName + " values(?, ?, 100.0, 200.0, 'hello world');";
    private Random random = new Random(System.currentTimeMillis());

    static {
        try {
            Properties prop = new Properties();
            prop.load(SingleStoreTest.class.getClassLoader().getResourceAsStream("benchmark.properties"));
            singlestoreUrl = prop.getProperty("singlestore_url");
            tableName = prop.getProperty("table_name");
            dbName = prop.getProperty("db_name");
            threadNum = Integer.parseInt(prop.getProperty("thread_num", "1"));
            baseKey = prop.getProperty("base_key");
            threadNum = Integer.parseInt(prop.getProperty("thread_num", "1"));
            pkCnt = Integer.parseInt(prop.getProperty("pk_cnt", "1"));
            tsCnt = Integer.parseInt(prop.getProperty("ts_cnt", "1"));
            needCreate = Boolean.parseBoolean(prop.getProperty("need_create", "true"));
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public boolean init() {
        executorService = Executors.newFixedThreadPool(threadNum);
        try {
            cnn = DriverManager.getConnection(singlestoreUrl);
            Statement st = cnn.createStatement();
            st = cnn.createStatement();
            if (needCreate) {
                st.execute(createDDL);
            }
            //st.execute(ddl1);
            return true;
        } catch (Exception e) {
            e.printStackTrace();
            return false;
        }
    }

    public void put() {
        while(true) {
            int num = random.nextInt(pkCnt) + pkCnt;
            String key = baseKey + String.valueOf(num);
            long ts = System.currentTimeMillis();
            for (int i = 0; i < tsCnt; i++) {
                try {
                    PreparedStatement st = cnn.prepareStatement(sql);
                    st.setString(1, key);
                    st.setLong(2, ts - i);
                    st.executeUpdate();
                    st.close();
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }
    }
    public void run() {
        for (int i = 0; i < threadNum; i++) {
            executorService.submit(new Runnable() {
                @Override
                public void run() {
                    put();
                }
            });
        }
    }

    public static void main(String[] args) {
        SingleStoreTest test = new SingleStoreTest();
        test.init();
        //test.put();
        test.run();
    }
}