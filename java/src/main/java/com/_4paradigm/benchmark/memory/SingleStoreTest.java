package com._4paradigm.benchmark.memory;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.util.Properties;

class SingleStoreTest {
    private static String singlestoreUrl;
    private static String tableName;
    private static String dbName;
    private static int threadNum;
    private Connection cnn;
    private String createDDL = "create table " + tableName + " (col1 varchar(20), col2 bigint, " +
            "col3 float," +
            "col4 double," +
            "col5 varchar(2)," +
            "KEY (col1, col2));";
    private String dropDDL = "drop table " + tableName + " ;";
    private String format = "insert into %s values('%s', %d," +
            "100.0, 200.0, 'hello world');";

    static {
        try {
            Properties prop = new Properties();
            prop.load(SingleStoreTest.class.getClassLoader().getResourceAsStream("benchmark.properties"));
            singlestoreUrl = prop.getProperty("singlestore_url");
            tableName = prop.getProperty("table_name");
            dbName = prop.getProperty("db_name");
            threadNum = Integer.parseInt(prop.getProperty("thread_num", "1"));

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public boolean init() {
        try {
            cnn = DriverManager.getConnection(singlestoreUrl);
            Statement st = cnn.createStatement();
            try {
                st.execute(dropDDL);
            } catch (Exception e) {
                e.printStackTrace();
            }
            st = cnn.createStatement();
            st.execute(createDDL);
            //st.execute(ddl1);
            return true;
        } catch (Exception e) {
            e.printStackTrace();
            return false;
        }
    }

    public static void main(String[] args) {
        SingleStoreTest test = new SingleStoreTest();
        //test.init();
        System.out.println(threadNum);
    }
}