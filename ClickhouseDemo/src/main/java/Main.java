import data.DataGenerator;
import ru.yandex.clickhouse.ClickHouseConnection;
import ru.yandex.clickhouse.ClickHouseDataSource;
import ru.yandex.clickhouse.settings.ClickHouseProperties;

import java.sql.PreparedStatement;
import java.sql.SQLException;
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
                        sth = connection.prepareStatement("insert into test.listen_backlog values (?,?,?)");
                    } catch (SQLException throwables) {
                        throwables.printStackTrace();
                    }
                    try {
                        int count=1000000;
                        while (count >= 0) {
                            count--;
                            sth.setString(1, DataGenerator.generateRandomString(3));
                            sth.setInt(2, random.nextInt(130));
                            sth.setString(3, DataGenerator.generateRandomString(10));
                            sth.addBatch();
                        }
                        System.out.println("===Write a batch!");
                        sth.executeBatch();
                        connection.commit();
                        connection.close();
                    } catch (SQLException throwables) {
                        throwables.printStackTrace();
                    }
                }
            }};

        List<Thread> task = new ArrayList<>();

        for (int i =1; i <=1; i++){
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
}
