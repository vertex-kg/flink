package typedstream;

import common.StreamGenerator;
import common.model.Trade;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.DiscardingSink;
import org.apache.flink.streaming.api.windowing.time.Time;
import streamtobatch.CustomTimeAssigner;

import java.util.List;

import static common.Constants.TRADE_COUNT;

public class TypedStreamToBatch {

    private long start;
    private long end;

    public static void main(String[] args) throws Exception {

        TypedStreamToBatch typedStreamToBatch = new TypedStreamToBatch();
//        typedStreamToBatch.processByCountWindow();
        typedStreamToBatch.processByTimeWindow();
        typedStreamToBatch.calculateMetrics();

    }

    public void processByCountWindow() throws Exception {

        StreamExecutionEnvironment senv = StreamExecutionEnvironment.getExecutionEnvironment();
        senv.setParallelism(1);

        DataStream<Trade> tradeStream = StreamGenerator.getTypedTradeStream(senv);

        SingleOutputStreamOperator<List<Trade>> streamOperator = tradeStream
                .countWindowAll(100_000)
                .aggregate(new TradeWindowAggregateFunction())
                .map(new TradeListMapFunction());
        streamOperator.setParallelism(1);


//        streamOperator.print();
        streamOperator.addSink(new DiscardingSink<>());

        start = System.currentTimeMillis();
        senv.execute();
        end = System.currentTimeMillis();
    }

    public void processByTimeWindow() throws Exception {

        StreamExecutionEnvironment senv = StreamExecutionEnvironment.getExecutionEnvironment();
        senv.setStreamTimeCharacteristic(TimeCharacteristic.IngestionTime);
        senv.setParallelism(1);

        DataStream<Trade> tradeStream =
                StreamGenerator.getTypedTradeStream(senv).assignTimestampsAndWatermarks(new CustomTimeAssigner());

        SingleOutputStreamOperator<List<Trade>> streamOperator = tradeStream
                .timeWindowAll(Time.seconds(2))
                .aggregate(new TradeWindowAggregateFunction())
                .map(new TradeListMapFunction());
        streamOperator.setParallelism(1);


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
