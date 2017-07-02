package stream;

import common.StreamGenerator;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.DiscardingSink;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

public class StreamJoin {


    public static void main(String[] args) throws Exception {

        new StreamJoin().start();
    }

    public void start() throws Exception {

//        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
//        BatchTableEnvironment tEnv = BatchTableEnvironment.getTableEnvironment(env);

        StreamExecutionEnvironment senv = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment stEnv = TableEnvironment.getTableEnvironment(senv);

        DataStream<Row> tradeStream = StreamGenerator.getTradeRowStream(senv);
        DataStream<Row> clientStream = StreamGenerator.getClientStream(senv);

        DataStream<Row> dataStream = tradeStream.join(clientStream)
                .where(row -> row.getField(1))
                .equalTo(row -> row.getField(0))
                .window(TumblingProcessingTimeWindows.of(Time.milliseconds(1000)))
                .apply((row1, row2) -> Row.of(row1.getField(0), row1.getField(1), row2.getField(1)));

//        dataStream.print();
        SingleOutputStreamOperator<Integer> reduce = dataStream.map(row -> 1).timeWindowAll(Time.milliseconds(1000)).reduce((in1, in2) -> in1 + in2);
        reduce.print();
        reduce.addSink(new DiscardingSink<>());

        senv.execute();

    }


}
