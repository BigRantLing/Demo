import data.DataGenerator;
import ru.yandex.clickhouse.ClickHouseConnection;
import ru.yandex.clickhouse.ClickHouseDataSource;
import ru.yandex.clickhouse.ClickHouseStatement;
import ru.yandex.clickhouse.domain.ClickHouseFormat;
import ru.yandex.clickhouse.settings.ClickHouseProperties;
import ru.yandex.clickhouse.util.ClickHouseRowBinaryStream;
import ru.yandex.clickhouse.util.ClickHouseStreamCallback;

import java.io.IOException;
import java.util.Random;

public class ArrayWriterJob {
    public static void main(String[] args) {
        String clickhouseUrl = "jdbc:clickhouse://macmini:8123";
        ClickHouseProperties clickHouseProperties = new ClickHouseProperties();
        clickHouseProperties.setUser("default");
        clickHouseProperties.setDatabase("test");

        ClickHouseDataSource dataSource = new ClickHouseDataSource(clickhouseUrl, clickHouseProperties);
        Random rd = new Random();

        String[] keys = new String[30];


        for (int index = 0; index < 30; index++) {
            keys[index] = "complex.key"+index;
        }

        try (ClickHouseConnection connection = dataSource.getConnection()) {
            ClickHouseStatement sth = connection.createStatement();
            sth.write().send("INSERT INTO test.arra_test", new ClickHouseStreamCallback() {
                @Override
                public void writeTo(ClickHouseRowBinaryStream stream) throws IOException {
                    stream.writeString(DataGenerator.generateRandomString(4));
                    stream.writeInt32(rd.nextInt(120));
                    String[] values = new String[30];

                    for(int index = 0; index < 30; index++) {
                        values[index] = DataGenerator.generateRandomString(30);
                    }

                    stream.writeStringArray(keys);
                    stream.writeStringArray(values);
                    stream.writeString(DataGenerator.generateRandomCityNameFromChina());
                }
            }, ClickHouseFormat.RowBinary);
        } catch (Exception e) {
            System.out.println("Error: " + e.getMessage());
            System.exit(-1);
        }

    }

}
