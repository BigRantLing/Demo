import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.LocalStreamEnvironment;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Random;

public class Main {
    public static void main(String[] args) throws Exception {

        LocalStreamEnvironment env = StreamExecutionEnvironment.createLocalEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.setParallelism(1);

        DataStreamSource<String> source = env.addSource(new MySource_2());

        SingleOutputStreamOperator<Tuple2<String, Integer>> transformatted = source
                .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<String>(Time.seconds(0)) {
                    @Override
                    public long extractTimestamp(String s) {
                        return Long.parseLong(s.split("_")[1]);
                    }
                })
                    .map(element -> Tuple2.of(element.split("_")[0], 1))
                    .returns(Types.TUPLE(Types.STRING, Types.INT))
                    .keyBy(value -> value.f0)
                    .timeWindow(Time.seconds(5))
                    .sum(1);

        transformatted.print();
        env.execute("Watermark Demo");
    }


    public static class MySource implements SourceFunction<String> {

        private boolean isRunning = true;
        private SimpleDateFormat sdf = new SimpleDateFormat("HH:mm:ss");

        @Override
        public void run(SourceContext<String> sourceContext) throws Exception {
            while (isRunning) {
                Thread.sleep(1000);
                char letter =(char)(65 + new Random().nextInt(25));
                long timeMills = System.currentTimeMillis() - 3000L;
                String element = new StringBuffer()
                        .append(letter)
                        .append("_")
                        .append(sdf.format(new Date(timeMills)))
                        .toString();

                System.out.println(element);
                sourceContext.collectWithTimestamp(element, timeMills);
                sourceContext.emitWatermark(new Watermark(timeMills - 1000));
            }
        }

        @Override
        public void cancel() {
            this.isRunning = false;
        }
    }


    public static class MySource_2 implements SourceFunction<String> {

        private boolean isRunning = true;
        private SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

        @Override
        public void run(SourceContext<String> sourceContext) throws Exception {
            while (isRunning) {
                Thread.sleep(1000);
                char letter =(char)(65 + new Random().nextInt(25));
                long timeMills = System.currentTimeMillis();
                String element = new StringBuffer()
                        .append(letter)
                        .append("_")
                        .append(timeMills)
                        .toString();

                System.out.println(element);
                sourceContext.collect(element);

            }
        }

        @Override
        public void cancel() {
            this.isRunning = false;
        }
    }


}


