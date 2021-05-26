package com._4paradigm.benchmark.memory;

import com._4paradigm.sql.sdk.SqlExecutor;
import com._4paradigm.sql.sdk.SdkOption;
import com._4paradigm.sql.sdk.impl.SqlClusterExecutor;
import com._4paradigm.sql.SQLInsertRow;
import com._4paradigm.sql.SQLInsertRows;

import java.util.Properties;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class FedbTest {
    private static String zkCluster;
    private static String zkRootPath;
    private static String tableName;
    private static String dbName;
    private static int threadNum;
    private static int pkCnt;
    private static int tsCnt;
    private static String baseKey;
    private static boolean needCreate;

    private SqlExecutor executor;
    private String createDDL = "create table " + tableName + " (col1 string, col2 bigint, " +
            "col3 float," +
            "col4 double," +
            "col5 string," +
            "index(key=(col1),ts=col2)) partitionnum=4;";
    private String dropDDL = "drop table " + tableName + " ;";
    private String format = "insert into " + tableName + " values(?, ?, 100.0, 200.0, 'hello world');";
    private ExecutorService executorService;
    private Random random = new Random(System.currentTimeMillis());

    static {
        try {
            Properties prop = new Properties();
            prop.load(SingleStoreTest.class.getClassLoader().getResourceAsStream("benchmark.properties"));
            zkCluster = prop.getProperty("zk_cluster");
            zkRootPath = prop.getProperty("zk_root_path");
            tableName = prop.getProperty("table_name");
            dbName = prop.getProperty("db_name");
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
        SdkOption sdkOption = new SdkOption();
        sdkOption.setSessionTimeout(30000);
        sdkOption.setZkCluster(zkCluster);
        sdkOption.setZkPath(zkRootPath);
        sdkOption.setEnableDebug(false);
        try {
            executor = new SqlClusterExecutor(sdkOption);
        } catch (Exception e) {
            e.printStackTrace();
            return false;
        }
       /* if (!executor.createDB(dbName)) {
            return false;
        }*/
        if (needCreate) {
            if (!executor.executeDDL(dbName, createDDL)) {
                return false;
            }
        }
        return true;
    }

    public void put() {
        while(true) {
            SQLInsertRows rows = executor.getInsertRows(dbName, format);
            int num = random.nextInt(pkCnt) + pkCnt;
            String key = baseKey + String.valueOf(num);
            for (int i = 0; i < tsCnt; i++) {
                SQLInsertRow row = rows.NewRow();
                row.Init(key.length());
                row.AppendString(key);
                row.AppendInt64(System.currentTimeMillis());
                row.delete();
            }
            try {
                executor.executeInsert(dbName, format, rows);
                //counter ++;
            } catch (Exception e) {
                e.printStackTrace();
            } finally {
                rows.delete();
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
        FedbTest test = new FedbTest();
        test.init();
        test.run();
    }

}
