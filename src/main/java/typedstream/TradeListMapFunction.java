package typedstream;

import common.model.Client;
import common.DataSetLoader;
import common.model.Product;
import common.model.Trade;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import typedstream.TradeClientDetailsFunc;
import typedstream.TradeProductDetailsFunc;

import java.io.Serializable;
import java.util.List;

public class TradeListMapFunction implements MapFunction<List<Trade>, List<Trade>>, Serializable {

    private ExecutionEnvironment executionEnvironment;
    private DataSet<Client> clientsDataSet;
    private DataSet<Product> productsDataSet;

    @Override
    public List<Trade> map(List<Trade> tupleList) throws Exception {
        init();

        DataSet<Trade> transactionDataSet = executionEnvironment.fromCollection(tupleList);
        List<Trade> result = transactionDataSet
                .join(clientsDataSet)
                .where(Trade::getClientId)
                .equalTo(Client::getId)
                .map(new TradeClientDetailsFunc())
                .join(productsDataSet)
                .where(Trade::getProductId)
                .equalTo(Product::getId)
                .map(new TradeProductDetailsFunc())
                .collect();

        return result;

    }

    private void init() {
        if (executionEnvironment == null) {
            executionEnvironment = ExecutionEnvironment.getExecutionEnvironment();
            clientsDataSet = DataSetLoader.getTypedClientDataSet(executionEnvironment);
            productsDataSet = DataSetLoader.getTypedProductDataSet(executionEnvironment);
        }
    }
}
