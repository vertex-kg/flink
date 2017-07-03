package streamtobatch;

import common.func.ListMapFunction;
import common.StreamGenerator;
import common.func.WindowAggregateFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.DiscardingSink;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.util.List;

import static common.Constants.TRADE_COUNT;

public class StreamToBatch {

    private long start;
    private long end;

    public static void main(String[] args) throws Exception {

        StreamToBatch streamToBatch = new StreamToBatch();
//        streamToBatch.processByCountWindow();
        streamToBatch.processByTimeWindow();
        streamToBatch.calculateMetrics();
    }

    public void processByCountWindow() throws Exception {

        StreamExecutionEnvironment senv = StreamExecutionEnvironment.getExecutionEnvironment();
        senv.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        senv.setParallelism(1);

        DataStream<Tuple3<Integer, Integer, Integer>> tradeStream =
                StreamGenerator.getTradeStream(senv).assignTimestampsAndWatermarks(new CustomTimeAssigner());

        SingleOutputStreamOperator<List<Tuple>> streamOperator = tradeStream
                .countWindowAll(100_000)
                .aggregate(new WindowAggregateFunction())
                .map(new ListMapFunction());
        streamOperator.setParallelism(2);


//        streamOperator.print();
        streamOperator.addSink(new DiscardingSink<>());

        start = System.currentTimeMillis();
        senv.execute();
        end = System.currentTimeMillis();
    }

    public void processByTimeWindow() throws Exception {

        StreamExecutionEnvironment senv = StreamExecutionEnvironment.getExecutionEnvironment();
        senv.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);
        senv.setParallelism(1);

        DataStream<Tuple3<Integer, Integer, Integer>> tradeStream =
                StreamGenerator.getTradeStream(senv).assignTimestampsAndWatermarks(new CustomTimeAssigner());

        SingleOutputStreamOperator<List<Tuple>> streamOperator = tradeStream
                .timeWindowAll(Time.seconds(2))
                .aggregate(new WindowAggregateFunction())
                .map(new ListMapFunction());
        streamOperator.setParallelism(2);


//        streamOperator.print();
        streamOperator.addSink(new DiscardingSink<>());

        start = System.currentTimeMillis();
        senv.execute();
        end = System.currentTimeMillis();
    }

    private void calculateMetrics() {
        double duration = (end - start) / 1000D;
        int rps = (int) (TRADE_COUNT / duration);
        System.out.println("Duration (seconds): " + duration);
        System.out.println("Records per second: " + rps);
    }
}
