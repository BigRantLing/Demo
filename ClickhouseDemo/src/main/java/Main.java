import ru.yandex.clickhouse.ClickHouseConnection;
import ru.yandex.clickhouse.ClickHouseDataSource;
import ru.yandex.clickhouse.ClickHouseStatement;
import ru.yandex.clickhouse.domain.ClickHouseFormat;
import ru.yandex.clickhouse.settings.ClickHouseProperties;
import ru.yandex.clickhouse.util.ClickHouseRowBinaryStream;
import ru.yandex.clickhouse.util.ClickHouseStreamCallback;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

public class Main {
    public static void main(String[] args) throws SQLException {
            timeOutTest();
    }

    public static void timeOutTest() {
        Runnable writeTask = new Runnable() {
            @Override
            public void run() {
                String url = "jdbc:clickhouse://macmini:8123/test";
                String user = "admin";
                String password = "123";

                ClickHouseProperties clickHouseProperties = new ClickHouseProperties();
                clickHouseProperties.setUser(user);
                clickHouseProperties.setPassword(password);
                clickHouseProperties.setUseServerTimeZone(false);
                clickHouseProperties.setUseTimeZone("Asia/Shanghai");

                ClickHouseDataSource clickHouseDataSource = new ClickHouseDataSource(url, clickHouseProperties);
                ClickHouseConnection connection = null;
                Random random = new Random();
                while (true) {
                    try {
                        connection = clickHouseDataSource.getConnection();
                    } catch (SQLException throwables) {
                        throwables.printStackTrace();
                    }

                    PreparedStatement sth = null;
                    try {
                        sth = connection.prepareStatement("insert in to test.listen_backlog values (?,?,?)");
                    } catch (SQLException throwables) {
                        throwables.printStackTrace();
                    }
                    try {
                        int count=10000000;
                        while (count >= 0) {
                            sth.setString(1, getRandomStringName(3));
                            sth.setInt(2, random.nextInt(130));
                            sth.setString(3, getRandomStringName(300));
                            sth.addBatch();
                        }
                        sth.executeBatch();
                    } catch (SQLException throwables) {
                        throwables.printStackTrace();
                    }
                }
            }};

        List<Thread> task = new ArrayList<>();

        for (int i =1; i <=10; i++){
            Thread t = new Thread(writeTask);
            task.add(t);
            t.start();
        }

        task.stream().forEach(t -> {
            try {
                t.join();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        });
    }


    public static String getRandomStringName(int len) {
        String ret = "";
        for (int i = 0; i < len; i++) {
            String str = null;
            int highPos, lowPos; // 定义高低位
            Random random = new Random();
            highPos = (176 + Math.abs(random.nextInt(39))); // 获取高位值
            lowPos = (161 + Math.abs(random.nextInt(93))); // 获取低位值
            byte[] b = new byte[2];
            b[0] = (new Integer(highPos).byteValue());
            b[1] = (new Integer(lowPos).byteValue());
            try {
                str = new String(b, "GBK"); // 转成中文
            } catch (UnsupportedEncodingException ex) {
                ex.printStackTrace();
            }
            ret += str;
        }
        return ret;
    }
        

}
