import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * Created by jasonlai on 2017/9/10.
 */
public class FlinkStreamingDemo {

    public static void main(String[] args) throws Exception {

        // flink會自己去判斷自己在什麼環境，目前在ide中
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //data source
        DataStream<String> elements = env.fromElements("one", "two" , "three");

        //data operator
        DataStream<Integer> digits = elements.map(new TransformDigits());

        //data sink
        digits.print();

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
}
