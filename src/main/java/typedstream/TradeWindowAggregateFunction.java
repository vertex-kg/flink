package typedstream;


import common.model.Trade;
import org.apache.flink.api.common.functions.AggregateFunction;

import java.util.ArrayList;
import java.util.List;

public class TradeWindowAggregateFunction implements AggregateFunction<Trade, List<Trade>, List<Trade>> {

    @Override
    public List<Trade> createAccumulator() {
        return new ArrayList<>();
    }

    @Override
    public void add(Trade value, List<Trade> accumulator) {
        accumulator.add(value);
    }

    @Override
    public List<Trade> getResult(List<Trade> accumulator) {
        return accumulator;
    }

    @Override
    public List<Trade> merge(List<Trade> a, List<Trade> b) {
        a.addAll(b);
        return a;
    }
}
