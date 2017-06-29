import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.table.api.*;
import org.apache.flink.table.sinks.CsvTableSink;
import org.apache.flink.table.sources.CsvTableSource;

public class BatchEntrypoint {


    public static void main(String[] args) throws Exception {

        new BatchEntrypoint().start();
    }

    public void start() throws Exception {

        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        BatchTableEnvironment tEnv = BatchTableEnvironment.getTableEnvironment(env);

//        StreamExecutionEnvironment senv = StreamExecutionEnvironment.getExecutionEnvironment();
//        StreamTableEnvironment stEnv = TableEnvironment.getTableEnvironment(senv);

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


        tEnv.registerTableSource("clients", clientsTableSource);
        tEnv.registerTableSource("products", productsTableSource);
        tEnv.registerTableSource("transactions", transactionsTableSource);

        Table clientsTable = tEnv.scan("clients");
        Table transactionsTable = tEnv.scan("transactions");
        Table productsTable = tEnv.scan("products");
        CsvTableSink tableSink =
                new CsvTableSink("/Users/krasimir/repos/lab/flink-tables/src/main/resources/result.csv", ",", 1, FileSystem.WriteMode.OVERWRITE);


        Table table = transactionsTable
                .join(clientsTable)
                .where("transactions_clientId = clients_id")
                .join(productsTable)
                .where("transactions_productId = products_id")
                .select("transactionId, clients_name, clients_parent, products_name");
        table.writeToSink(tableSink);

        env.execute();

    }


}
