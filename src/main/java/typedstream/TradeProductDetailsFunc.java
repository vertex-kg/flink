package typedstream;

import common.model.Product;
import common.model.Trade;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;

import java.io.Serializable;

public class TradeProductDetailsFunc implements MapFunction<Tuple2<Trade, Product>, Trade>, Serializable {

    @Override
    public Trade map(Tuple2<Trade, Product> value) throws Exception {
        value.f0.setProductName(value.f1.getName());
        return value.f0;
    }
}
