package cn.pavelyq.mf;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ConfigurableApplicationContext;

import java.io.IOException;
import java.sql.SQLException;

@SpringBootApplication
public class MfApplication {

    public static void main(String[] args) throws IOException, SQLException {
        if (args == null || args.length == 0) {
            System.out.println("please enter your file location or directory");
            System.exit(-1);
        }
        String path = args[0];
        ConfigurableApplicationContext context = SpringApplication.run(MfApplication.class, args);
        DataImport dataImport = context.getBean(DataImport.class);
        dataImport.importData(path);
        System.out.println("done!");
        System.exit(0);

    }
}
