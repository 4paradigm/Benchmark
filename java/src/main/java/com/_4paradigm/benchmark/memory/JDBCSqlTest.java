package com._4paradigm.benchmark.memory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.Statement;
import java.util.Properties;
import java.util.Random;

public class JDBCSqlTest implements Test {
    private static String connectURL;
    private static String tableName;
    private static String dbName;
    private static int pkCnt;
    private static int tsCnt;
    private static String baseKey;
    private static String method;
    private static boolean needCreate;
    private String createDDL = "create table " + tableName + " (col1 varchar(20), col2 bigint, " +
            "col3 float," +
            "col4 float," +
            "col5 varchar(12)," +
            "PRIMARY KEY (col1, col2));";
    private String sql = "insert into " + tableName + " values(?, ?, 100.0, 200.0, 'hello world');";
    private Random random = new Random(System.currentTimeMillis());
    static {
        try {
            Properties prop = new Properties();
            prop.load(JDBCSqlTest.class.getClassLoader().getResourceAsStream("benchmark.properties"));
            connectURL = prop.getProperty("connect_url");
            tableName = prop.getProperty("table_name");
            dbName = prop.getProperty("db_name");
            baseKey = prop.getProperty("base_key");
            pkCnt = Integer.parseInt(prop.getProperty("pk_cnt", "1"));
            tsCnt = Integer.parseInt(prop.getProperty("ts_cnt", "1"));
            method = prop.getProperty("method");
            needCreate = Boolean.parseBoolean(prop.getProperty("need_create"));
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public boolean init() {
        try {
            if (method.equals("voltdb")) {
                Class.forName("org.voltdb.jdbc.Driver");
            }
            if (needCreate) {
                Connection cnn = DriverManager.getConnection(connectURL);
                Statement st = cnn.createStatement();
                st.execute(createDDL);
                st.close();
                cnn.close();
            }
            return true;
        } catch (Exception e) {
            e.printStackTrace();
            return false;
        }
    }

    public void put() {
        Connection cnn = null;
        try {
            cnn = DriverManager.getConnection(connectURL);
        } catch (Exception e) {
            e.printStackTrace();
            return;
        }
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
}
