package typedstream;

import common.model.Client;
import common.model.Trade;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;

import java.io.Serializable;

public class TradeClientDetailsFunc implements MapFunction<Tuple2<Trade, Client>, Trade>, Serializable {

    @Override
    public Trade map(Tuple2<Trade, Client> value) throws Exception {
        value.f0.setClientName(value.f1.getName());
        value.f0.setClientParent(value.f1.getParent());
        return value.f0;
    }
}
