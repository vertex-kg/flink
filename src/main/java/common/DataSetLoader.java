package common;

import common.model.Client;
import common.model.Product;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.LongStream;

import static common.Constants.CLIENT_COUNT;
import static common.Constants.PRODUCT_COUNT;

public class DataSetLoader {


    public static DataSet<Tuple3<Integer, String, String>> getClientFileDataSet(ExecutionEnvironment env) {

        return env.readCsvFile("/Users/krasimir/repos/lab/flink-tables/src/main/resources/clients.csv")
                .ignoreFirstLine()
                .fieldDelimiter(",")
                .includeFields("111")
                .types(Integer.class, String.class, String.class);
    }

    public static DataSet<Tuple> getClientDataSet(ExecutionEnvironment env) {

            List<Tuple> list = new ArrayList<>();
            for(int i = 0; i < CLIENT_COUNT; i++) {
                list.add(Tuple3.of(i, "Client " + i, "Parent " + i));
            }
            return env.fromCollection(list);
    }

    public static DataSet<Client> getTypedClientDataSet(ExecutionEnvironment env) {

        List<Client> list = LongStream.range(0, CLIENT_COUNT)
                .mapToObj(i -> new Client(i, "Client " + i, "Parent " + i))
                .collect(Collectors.toList());
        return env.fromCollection(list);
    }

    public static DataSet<Tuple> getProductDataSet(ExecutionEnvironment env) {

            List<Tuple> list = new ArrayList<>();
            for(int i=0; i < PRODUCT_COUNT; i++) {
                list.add(Tuple2.of(i, "Product " + i));
            }
            return env.fromCollection(list);
    }

    public static DataSet<Product> getTypedProductDataSet(ExecutionEnvironment env) {

        List<Product> list = LongStream.range(0, PRODUCT_COUNT)
                .mapToObj(i -> new Product(i, "Product " + i))
                .collect(Collectors.toList());
        return env.fromCollection(list);
    }
}
