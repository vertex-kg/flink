package common;

import common.model.Trade;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.types.Row;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.LongStream;

import static common.Constants.*;

public class StreamGenerator {

    public static DataStream<Tuple3<Integer, Integer, Integer>> getTradeStream(StreamExecutionEnvironment env) {

        final AtomicInteger clientId = new AtomicInteger(0);
        final AtomicInteger productId = new AtomicInteger(0);
        List<Tuple3<Integer, Integer, Integer>> trades = IntStream.range(0, TRADE_COUNT)
                .mapToObj(i -> new Tuple3<>(i,
                        clientId.get() < CLIENT_COUNT ? clientId.getAndIncrement() : clientId.getAndSet(0),
                        productId.get() < PRODUCT_COUNT ? productId.getAndIncrement() : productId.getAndSet(0)))
                .map(t -> Tuple3.of(t.f0, t.f1, t.f2))
                .collect(Collectors.toList());

        return env.fromCollection(trades);
    }

    public static DataStream<Trade> getTypedTradeStream(StreamExecutionEnvironment env) {

        final AtomicLong clientId = new AtomicLong(0);
        final AtomicLong productId = new AtomicLong(0);
        List<Trade> trades = LongStream.range(0, TRADE_COUNT)
                .mapToObj(i -> new Tuple3<>(i,
                        clientId.get() < CLIENT_COUNT ? clientId.getAndIncrement() : clientId.getAndSet(0),
                        productId.get() < PRODUCT_COUNT ? productId.getAndIncrement() : productId.getAndSet(0)))
                .map(t -> new Trade(t.f0, t.f1, t.f2))
                .collect(Collectors.toList());

        return env.fromCollection(trades);
    }

    public static DataStream<Row> getTradeRowStream(StreamExecutionEnvironment env) {
        List<Row> list = new ArrayList<>();
        Random random = new Random();
        int j = 0;
        for (int i = 0; i < 100; i++) {
            list.add(Row.of(i, random.nextInt(CLIENT_COUNT), random.nextInt(5)));
//            list.add(new Tuple3<>(i, 10200, 100));
            if (j == 10) j = 0;
        }
        return env.fromCollection(list);
    }

    public static DataStream<Row> getClientStream(StreamExecutionEnvironment env) {
        List<Row> list = new ArrayList<>();
        for (int i = 0; i < CLIENT_COUNT; i++) {
            list.add(Row.of(i, "Client " + i));
        }
        return env.fromCollection(list);
    }


}
