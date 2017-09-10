import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * Created by jasonlai on 2017/9/10.
 */
public class FlinkStreamingDemo {

    public static void main(String[] args) throws Exception {

        // flink會自己去判斷自己在什麼環境，目前在IDE中
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //data source
        DataStream<String> elements = env.fromElements("One", "two" , "three");

        //data operator

        //data sink
        elements.print();

        //如果今天sink是到別的地方database之類的就需要用到這行，用print()其實這行可以註解
        env.execute("Flink Streaming Demo");
    }

}
