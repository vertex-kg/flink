package stream;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.streaming.api.collector.selector.OutputSelector;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.SplitStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.DiscardingSink;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.sinks.CsvTableSink;
import org.apache.flink.table.sources.CsvTableSource;
import org.apache.flink.types.Row;

import java.util.ArrayList;
import java.util.List;

public class StreamEntrypoint {


    public static void main(String[] args) throws Exception {

        new StreamEntrypoint().start();
    }

    public void start() throws Exception {

//        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
//        BatchTableEnvironment tEnv = BatchTableEnvironment.getTableEnvironment(env);

        StreamExecutionEnvironment senv = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment stEnv = TableEnvironment.getTableEnvironment(senv);

//        Transaction transaction = new Transaction(10000, 10100, 100);

//        DataStreamSource<Transaction> streamSource = senv.fromElements(Transaction.class, transaction);
//        stEnv.registerDataStream("transactions", streamSource);

        CsvTableSource clientsTableSource = new CsvTableSource(
                "/Users/krasimir/repos/lab/flink-tables/src/main/resources/clients.csv",
                new String[]{"clients_id", "clients_name", "clients_parent"},
                new TypeInformation<?>[]{Types.INT, Types.STRING, Types.STRING},
                ",",    // fieldDelim
                "\n",    // rowDelim
                null,   // quoteCharacter
                true,   // ignoreFirstLine
                "%",    // ignoreComments
                false); // lenient


        CsvTableSource productsTableSource = new CsvTableSource(
                "/Users/krasimir/repos/lab/flink-tables/src/main/resources/products.csv",
                new String[]{"products_id", "products_name"},
                new TypeInformation<?>[]{Types.INT, Types.STRING},
                ",",    // fieldDelim
                "\n",    // rowDelim
                null,   // quoteCharacter
                true,   // ignoreFirstLine
                "%",    // ignoreComments
                false); // lenient

        CsvTableSource transactionsTableSource = new CsvTableSource(
                "/Users/krasimir/repos/lab/flink-tables/src/main/resources/transactions.csv",
                new String[]{"transactionId", "transactions_clientId", "transactions_productId"},
                new TypeInformation<?>[]{Types.INT, Types.INT, Types.INT},
                ",",    // fieldDelim
                "\n",    // rowDelim
                null,   // quoteCharacter
                true,   // ignoreFirstLine
                "%",    // ignoreComments
                false); // lenient


        stEnv.registerTableSource("clients", clientsTableSource);
        stEnv.registerTableSource("products", productsTableSource);
        stEnv.registerTableSource("transactions", transactionsTableSource);

        Table clientsTable = stEnv.scan("clients");
        Table transactionsTable = stEnv.scan("transactions");
        Table productsTable = stEnv.scan("products");
        CsvTableSink tableSink =
                new CsvTableSink("/Users/krasimir/repos/lab/flink-tables/src/main/resources/result.csv", ",", 1, FileSystem.WriteMode.OVERWRITE);


        DataStream<Row> rowDataStream = stEnv.toAppendStream(transactionsTable, Row.class);
        SplitStream<Row> split = rowDataStream.split(new OutputSelector<Row>() {
            @Override
            public Iterable<String> select(Row value) {
                List<String> output = new ArrayList<String>();
                output.add("1");
                output.add("2");
                return output;
            }
        });

        DataStream<Row> one = split.select("1");
        DataStream<Row> two = split.select("2");

//        one.map(row -> (Integer)row.getField(1)).map(id -> stEnv.toAppendStream(clientsTable.where("id="+id), Row.class).countWindowAll(1).process());

//        SingleOutputStreamOperator<Row> streamOperator = rowDataStream.keyBy(0).countWindow(1).apply((key, window, in, out) -> in.forEach(out::collect));
//        SingleOutputStreamOperator<Row> streamOperator = rowDataStream.keyBy(0).countWindow(1).aggregate();


        Table table = transactionsTable;
//                .join(clientsTable)
//                .where("transactions_clientId = clients_id")
//                .join(productsTable)
//                .where("transactions_productId = products_id")
//                .select("transactionId, clients_name, clients_parent, products_name");

//        SingleOutputStreamOperator<DataStream<Row>> outputStreamOperator = rowDataStream.map(new RowMapFunction());

//        rowDataStream.join(outputStreamOperator).where(in -> in.getField(0)).equalTo(in -> in.iterate().)


        rowDataStream.print();
        rowDataStream.addSink(new DiscardingSink<>());

        senv.execute();

    }


}
