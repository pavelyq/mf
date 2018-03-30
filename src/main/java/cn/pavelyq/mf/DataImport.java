package cn.pavelyq.mf;

import com.linuxense.javadbf.DBFReader;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.charset.Charset;
import java.sql.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Created by Pavel on 2018/3/29.
 */
@Controller
@Slf4j
public class DataImport {


    @Value("${db.url}")
    private String url;
    @Value("${db.username}")
    private String username;
    @Value("${db.password}")
    private String password;


    @RequestMapping("import")
    public void importData(String path) throws IOException, SQLException {
        File file = new File(path);
        if (file.isDirectory()) {
            importDirectory(file);
        } else {
            importFile(file);
        }
    }

    private void importFile(File file) {
        try (FileInputStream fi = new FileInputStream(file)) {
            log.info("importing file " + file.getName() + " ...");
            DBFReader reader = new DBFReader(fi, Charset.forName("GBK"));
            int count = insertToDB(reader, file.getName());
            log.info("import file " + file.getName() + " successful, import recode number is " + count);
        } catch (Exception e) {
            log.error("import file failed ", e);
        }
    }

    private void importDirectory(File directory) throws IOException, SQLException {
        File[] files = directory.listFiles();
        if (files == null) {
            log.error("can't find any files in directory " + directory.getAbsolutePath());
            System.exit(-1);
        }
        ExecutorService executorService = Executors.newFixedThreadPool(10);
        CountDownLatch countDownLatch = new CountDownLatch(files.length);
        for (File file : files) {
            executorService.submit(new Runnable() {
                @Override
                public void run() {
                    importFile(file);
                    countDownLatch.countDown();
                }
            });
        }
        try {
            countDownLatch.await();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }


    private int insertToDB(DBFReader reader, String name) throws SQLException {
        Connection conn = getConn();
        int fieldCount = reader.getFieldCount();
        StringBuilder sb = new StringBuilder(500);
        sb.append("insert into fdld( ");
        for (int i = 0; i < fieldCount; i++) {
            sb.append(reader.getField(i).getName());
            sb.append(",");
        }
        sb.deleteCharAt(sb.length() - 1);
        sb.append(")");
        sb.append(" values (");
        for (int i = 0; i < fieldCount; i++) {
            sb.append("?,");
        }
        sb.deleteCharAt(sb.length() - 1);
        sb.append(")");
        PreparedStatement ps = conn.prepareStatement(sb.toString());
        Object[] objects;
        int count = 0;
        while ((objects = reader.nextRecord()) != null) {
            int length = objects.length;
            for (int i = 0; i < length; i++) {
                Object obj = objects[i];
                if (obj instanceof java.util.Date) {
                    java.util.Date date = (java.util.Date) obj;
                    ps.setObject(i + 1, new Date(date.getTime()));
                } else if (obj instanceof String) {
                    String str = (String) obj;
                    ps.setObject(i + 1, str);
                } else {
                    ps.setObject(i + 1, obj);
                }
            }
            ps.addBatch();
            count++;
            if (count % 100 == 0) {
                ps.executeBatch();
                ps.clearBatch();
            }
            if (count % 1000 == 0) {
                log.info("import file {} ... : {}", name, count);
            }
        }
        ps.executeBatch();
        ps.clearBatch();
        ps.close();
        return count;

    }

    private Connection getConn() throws SQLException {
        return DriverManager.getConnection(url, username, password);
    }


}
