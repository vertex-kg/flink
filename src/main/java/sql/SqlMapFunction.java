package sql;

import common.DataSetLoader;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.java.BatchTableEnvironment;
import org.apache.flink.types.Row;

import java.io.Serializable;
import java.util.List;

public class SqlMapFunction implements MapFunction<List<Tuple3<Integer, Integer, Integer>>, List<Row>>, Serializable {

    private ExecutionEnvironment executionEnvironment;
    private BatchTableEnvironment tableEnvironment;
    private DataSet<Tuple> clientsDataSet;
    private DataSet<Tuple> productsDataSet;

    @Override
    public List<Row> map(List<Tuple3<Integer, Integer, Integer>> tupleList) throws Exception {
        init();

        DataSet<Tuple3<Integer, Integer, Integer>> tradeDataSet = executionEnvironment.fromCollection(tupleList);

        tableEnvironment = TableEnvironment.getTableEnvironment(executionEnvironment);
        tableEnvironment.registerDataSet("clients", clientsDataSet, "clientId, clientName, clientParent");
        tableEnvironment.registerDataSet("products", productsDataSet, "productId, productName");
        tableEnvironment.registerDataSet("trades", tradeDataSet, "tradeId, clientId, productId");

        Table table = tableEnvironment.sql(
                "select tradeId, clients.clientId, clientName, clientParent, products.productId, productName " +
                "from trades, clients, products " +
                "where trades.clientId = clients.clientId and trades.productId = products.productId");

        List<Row> result = tableEnvironment.toDataSet(table, Row.class).collect();

        return result;

    }

    private void init() {
        if (executionEnvironment == null) {
            executionEnvironment = ExecutionEnvironment.getExecutionEnvironment();
            tableEnvironment = TableEnvironment.getTableEnvironment(executionEnvironment);
            clientsDataSet = DataSetLoader.getClientDataSet(executionEnvironment);
            productsDataSet = DataSetLoader.getProductDataSet(executionEnvironment);

        }
    }
}
