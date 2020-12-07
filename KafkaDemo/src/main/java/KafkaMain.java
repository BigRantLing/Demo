import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.twitter.bijection.Injection;
import com.twitter.bijection.avro.GenericAvroCodecs;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import protobuf.model.Student;

import java.io.IOException;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

public class KafkaMain {
    public static void main(String[] args) throws JsonProcessingException, InterruptedException, ExecutionException {

        Properties kafkaProperties = new Properties();

        kafkaProperties.put("bootstrap.servers", "localhost:9092");
        kafkaProperties.put("request.required.acks", "0");
        kafkaProperties.put("key.serializer","org.apache.kafka.common.serialization.StringSerializer");
        kafkaProperties.put("value.serializer","org.apache.kafka.common.serialization.ByteArraySerializer");
        Producer producer = new KafkaProducer(kafkaProperties);

        while(true) {
            sendProtobufMessage(producer, 2);
            Thread.sleep(1000);
        }

//        String t = "\u00012020-12-01\u0001驼泛滴\u0001118\u0001L\u0001";

//        sendJsonMessage(producer, 10000);
//        sendTSVMessage(producer, 20,"\u0001");
    }


    public static void sendJsonMessage(Producer producer, int messageBatch) throws JsonProcessingException {
        ObjectMapper mapper = new ObjectMapper();
        String[] levels = {"VH","H","N","L"};
        Random random = new Random();

        while (messageBatch!=0) {
            Event event = new Event();
            event.setAge(random.nextInt(120));
            event.setName(getRandomStringName(4 - random.nextInt(3)));
            event.setCtime(LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd")));

            int matesCounter = random.nextInt(10);
            List<String> matesList = new ArrayList<>();

            for (int i = 1; i <= matesCounter; i++) {
                matesList.add(getRandomStringName(4 - random.nextInt(3)));
            }

            event.setMates(matesList);
            event.setLevel(levels[3 - random.nextInt(4)]);

            ProducerRecord<Long, String> record = new ProducerRecord<Long, String>("clickhouse_xf", mapper.writeValueAsString(event));
            producer.send(record);
            messageBatch--;
        }
    }


    public static void sendTSVMessage(Producer producer, int messageBatch, String delimiter) throws JsonProcessingException {
        ObjectMapper mapper = new ObjectMapper();
        String[] levels = {"VH","H","N","L"};
        Random random = new Random();

        while (messageBatch!=0) {
            Event event = new Event();
            event.setAge(random.nextInt(120));
            event.setName(getRandomStringName(4 - random.nextInt(3)));
            event.setCtime(LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd")));

            int matesCounter = random.nextInt(10);
            List<String> matesList = new ArrayList<>();

            for (int i = 1; i <= matesCounter; i++) {
                matesList.add(getRandomStringName(4 - random.nextInt(3)));
            }

            event.setMates(matesList);
            event.setLevel(levels[3 - random.nextInt(4)]);

            StringBuffer record = new StringBuffer();
            record.append(delimiter);
            record.append(event.getCtime());
            record.append(delimiter);
            record.append(event.getName());
            record.append(delimiter);
            record.append(event.getAge());
            record.append(delimiter);
            record.append(event.getLevel());
            record.append(delimiter);

//            System.out.println(record.toString());
//
            ProducerRecord<Long, String> kafkaRecord = new ProducerRecord<Long, String>("clickhouse_xf_csv", record.toString());
            System.out.println(record.toString());
//            producer.send(kafkaRecord);
            messageBatch--;
        }
    }


    public static void sendAvroMessage(Producer producer, int messageBatch) {

        Injection<GenericRecord, byte[]> recordInjection;
        Schema schema;

        try (InputStream inputStream = KafkaMain.class.getClassLoader().getResourceAsStream("polarisAvro.json")) {
            schema = new Schema.Parser().parse(inputStream);
            recordInjection = GenericAvroCodecs.toBinary(schema);
        } catch (IOException e) {
           throw new RuntimeException(e);
        }

        Random random = new Random();

        for (int count=0; count<messageBatch; count++) {
            String name = getRandomStringName(4 - random.nextInt(3));
            GenericData.Record record = new GenericData.Record(schema);
            record.put("user_name", name);
            record.put("time_hour", Long.parseLong(String.valueOf(random.nextInt(1000000))));
            record.put("age", random.nextInt(130));

//            int matesCounter = random.nextInt();
//            List<String> mates = new ArrayList<>();
//            for (int f =0; f<matesCounter; f++) {
//                mates.add(getRandomStringName(4 - random.nextInt(3)));
//            }

//            record.put("mates", mates.toArray());
            byte[] event = recordInjection.apply(record);
            ProducerRecord<String, byte[]> kafkaRecord = new ProducerRecord("clickhouse_xf_77",name, event);

            producer.send(kafkaRecord);
        }

    }

    public static void sendProtobufMessage(Producer producer, int messageBatch) throws ExecutionException, InterruptedException {
        Student.Builder eventBuilder = Student.newBuilder();

        Random random = new Random();

        for (int count=0; count<messageBatch; count++) {
            String name = getRandomStringName(4 - random.nextInt(3));
            eventBuilder.setName(name);
            eventBuilder.setAge(random.nextInt(18));
            eventBuilder.setClazz("A");

            Student event = eventBuilder.build();

            if (event.isInitialized()) {
                event
                ProducerRecord<String, String> kafkaRecord = new ProducerRecord("clickhouse_protobuf_02", String.valueOf(random.nextInt()), event.toByteArray());
                producer.send(kafkaRecord);
            }



        }

    }

    public static void sendCanProtoMessage() {}


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
