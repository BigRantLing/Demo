import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.io.UnsupportedEncodingException;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.Random;

public class KafkaMain {
    public static void main(String[] args) throws JsonProcessingException {

        Properties kafkaProperties = new Properties();

        kafkaProperties.put("bootstrap.servers", "localhost:9092");
        kafkaProperties.put("request.required.acks", "0");
        kafkaProperties.put("key.serializer","org.apache.kafka.common.serialization.StringSerializer");
        kafkaProperties.put("value.serializer","org.apache.kafka.common.serialization.StringSerializer");
        Producer producer = new KafkaProducer(kafkaProperties);

        sendJsonMessage(producer, 1000);
    }


    public static void sendJsonMessage(Producer producer, int messageBatch) throws JsonProcessingException {
        ObjectMapper mapper = new ObjectMapper();
        String[] levels = {"VH","H","N","L"};
        Random random = new Random();

        while (messageBatch!=0) {
            Event event = new Event();
            event.setAge(random.nextInt(120));
            event.setName(getRandomStringName(4 - random.nextInt(2)));
            event.setCtime(LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd")));

            int matesCounter = random.nextInt(10);
            List<String> matesList = new ArrayList<>();

            for (int i = 1; i <= matesCounter; i++) {
                matesList.add(getRandomStringName(4 - random.nextInt(2)));
            }

            event.setMates(matesList);
            event.setLevel(levels[3 - random.nextInt(3)]);

            ProducerRecord<Long, String> record = new ProducerRecord<Long, String>("clickhouse_xf", mapper.writeValueAsString(event));
            producer.send(record);
            messageBatch--;
        }
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
