package common.func;

import common.DataSetLoader;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple3;

import java.io.Serializable;
import java.util.List;

public class ListMapFunction implements MapFunction<List<Tuple3<Integer, Integer, Integer>>, List<Tuple>>, Serializable {

    private ExecutionEnvironment executionEnvironment;
    private DataSet<Tuple> clientsDataSet;
    private DataSet<Tuple> productsDataSet;

    @Override
    public List<Tuple> map(List<Tuple3<Integer, Integer, Integer>> tupleList) throws Exception {
        init();

        DataSet<Tuple3<Integer, Integer, Integer>> transactionDataSet = executionEnvironment.fromCollection(tupleList);
        List<Tuple> result = transactionDataSet
                .join(clientsDataSet)
                .where(1)
                .equalTo(0)
                .projectFirst(0).projectSecond(1, 2).projectFirst(2)
                .join(productsDataSet)
                .where(3)
                .equalTo(0)
                .projectFirst(0, 1, 2).projectSecond(1)
                .collect();

        return result;

    }

    private void init() {
        if (executionEnvironment == null) {
            executionEnvironment = ExecutionEnvironment.getExecutionEnvironment();
            clientsDataSet = DataSetLoader.getClientDataSet(executionEnvironment);
            productsDataSet = DataSetLoader.getProductDataSet(executionEnvironment);
        }
    }
}
