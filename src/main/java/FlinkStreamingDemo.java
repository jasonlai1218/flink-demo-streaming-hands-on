import org.apache.flink.api.common.functions.FlatJoinFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer09;
import org.apache.flink.streaming.util.serialization.SimpleStringSchema;
import org.apache.flink.util.Collector;

import javax.xml.soap.SAAJResult;
import java.util.Properties;

/**
 * Created by jasonlai on 2017/9/10.
 */
public class FlinkStreamingDemo {

    public static void main(String[] args) throws Exception {

        // flink會自己去判斷自己在什麼環境，目前在ide中
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //data source : kafka
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "localhost:9092");
        properties.setProperty("zookeeper.connect", "localhost:2181");
        properties.setProperty("group.id", "jason-lai-19891218");

        DataStream<String> lines = env.addSource(new FlinkKafkaConsumer09<String>("random_text_lines", new SimpleStringSchema(), properties));

        DataStream<Tuple2<String, Integer>> words = lines.flatMap(new SplitLines());
        DataStream<Tuple2<String, Integer>> count = words.keyBy(0).timeWindow(Time.seconds(10)).sum(1);
        //sliding windows
        //DataStream<Tuple2<String, Integer>> count = words.keyBy(0).timeWindow(Time.seconds(10), Time.seconds(1)).sum(1);

        //data sink
        count.print();

        //如果今天sink是到別的地方database之類的就需要用到這行，用print()其實這行可以註解
        env.execute("Flink Streaming Demo");
    }

    //operator
    public static class TransformDigits implements MapFunction<String, Integer> {
        public Integer map(String s) throws Exception {
            if (s.equals("one"))
                return 1;
            else if(s.equals("two"))
                return 2;
            else if(s.equals("three"))
                return 3;
            else
                throw new RuntimeException();
        }
    }

    public static class SplitLines implements FlatMapFunction<String, Tuple2<String, Integer>> {
        public void flatMap(String s, Collector<Tuple2<String, Integer>> collector) throws Exception {
            for (String word : s.split("\\W+"))
                collector.collect(new Tuple2<String, Integer>(word, 1));
        }
    }
}
