package common.func;


import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple3;

import java.util.ArrayList;
import java.util.List;

public class WindowAggregateFunction implements AggregateFunction<Tuple3<Integer,Integer,Integer>, List<Tuple3<Integer,Integer,Integer>>, List<Tuple3<Integer,Integer,Integer>>> {

    @Override
    public List<Tuple3<Integer, Integer, Integer>> createAccumulator() {
        return new ArrayList<>();
    }

    @Override
    public void add(Tuple3<Integer, Integer, Integer> value, List<Tuple3<Integer, Integer, Integer>> accumulator) {
        accumulator.add(value);
    }

    @Override
    public List<Tuple3<Integer, Integer, Integer>> getResult(List<Tuple3<Integer, Integer, Integer>> accumulator) {
        return accumulator;
    }

    @Override
    public List<Tuple3<Integer, Integer, Integer>> merge(List<Tuple3<Integer, Integer, Integer>> a, List<Tuple3<Integer, Integer, Integer>> b) {
        a.addAll(b);
        return a;
    }
}
