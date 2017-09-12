import org.apache.flink.api.common.functions.*;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.checkpoint.Checkpointed;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer09;
import org.apache.flink.streaming.util.serialization.SimpleStringSchema;
import org.apache.flink.util.Collector;
import java.util.HashMap;
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

        DataStream<String> customerEventsStr = env.addSource(new FlinkKafkaConsumer09<String>("shopping_customer_events", new SimpleStringSchema(), properties));

        //將字串1對1轉成Customer Object
        DataStream<CustomerEvent> customerEventDataStream = customerEventsStr.map(new MarshallToPOJO());

        //每一個事件觸發不一定會觸發alert,一個事件進來可能觸發0或多個,使用flatMap
        DataStream<String> alertLeaveWithin3Sec = customerEventDataStream.flatMap(new Alter3SecondsLocalState());

        //data sink
        alertLeaveWithin3Sec.print();

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

    public static class CustomerEvent {
        public String name;
        public String event;
        public long eventTime;

        public CustomerEvent() {}
        public CustomerEvent(String name, String event, long eventTime) {
            this.name = name;
            this.event = event;
            this.eventTime = eventTime;
        }
    }

    public static class MarshallToPOJO implements MapFunction<String, CustomerEvent> {
        public CustomerEvent map(String s) throws Exception {
            String[] split = s.split(" ");
            return new CustomerEvent(split[0], split[1], Long.valueOf(split[2]));
        }
    }

    //local state operator
    public static class Alter3SecondsLocalState implements FlatMapFunction<CustomerEvent, String>, Checkpointed<HashMap<String, Long>> {

        //需要去維護一個HashMap,某個人進來他的時間是何時,這就是state. flink會幫我們去維護這個state,保證所有進來的record只會反應這個state一次
        //如果不透過flink,當record replay就必須考慮state更新各種狀況等
        private HashMap<String, Long> lastEntryTime = new HashMap<String, Long>();

        public void flatMap(CustomerEvent customerEvent, Collector<String> collector) throws Exception {
            String event = customerEvent.event;

            if(event.equals("ENTER_STORE")) {
                lastEntryTime.put(customerEvent.name, customerEvent.eventTime);
            } else if(event.equals("SHOPPING")) {
                //
            } else if(event.equals("LEFT_STORE")) {
                if(!lastEntryTime.containsKey(customerEvent.name)) {
                    //
                } else {
                    if(customerEvent.eventTime - lastEntryTime.get(customerEvent.name) <= 3000) {
                        collector.collect("! ! ! ! " + customerEvent.name + " left store within 3 seconds.");
                        lastEntryTime.remove(customerEvent.name);
                    }
                }
            } else {
                //
            }
        }

        //如果flink掛掉,如何去拿回state,最後一次 最新的hashmap的狀態,每一個operator都會去拿自己最後的狀態
        public void restoreState(HashMap<String, Long> stringLongHashMap) throws Exception {
            //拿flink紀錄的最後一個狀態(內部operator狀態)去填入到state
            lastEntryTime = stringLongHashMap;
        }

        //flink叫operator去snapshot後,我要snapshot什麼東西,就是snapshot state
        public HashMap<String, Long> snapshotState(long l, long l1) throws Exception {
            return lastEntryTime;
        }
    }

}
