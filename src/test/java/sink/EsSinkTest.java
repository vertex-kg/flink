package sink;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.elasticsearch.util.NoOpFailureHandler;
import org.junit.Test;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class EsSinkTest {

    @Test
    public void name() throws Exception {

        //given
        StreamExecutionEnvironment executionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> stringDataStreamSource = executionEnvironment.fromElements("elem1", "elem2");
        Map<String, String> config = new HashMap<>();
        // This instructs the sink to emit after every element, otherwise they would be buffered
        config.put("bulk.flush.max.actions", "1");
        config.put("cluster.name", "elasticsearch");
        config.put("path.home", "/Users/krasimir/tmp");

        config.put("searchguard.ssl.transport.keystore_filepath", "/Users/krasimir/repos/search-guard-ssl/example-pki-scripts/kirk-keystore.jks");
        config.put("searchguard.ssl.transport.keystore_password", "changeit");
        config.put("searchguard.ssl.transport.truststore_filepath", "/Users/krasimir/repos/search-guard-ssl/example-pki-scripts/truststore.jks");
        config.put("searchguard.ssl.transport.truststore_password", "changeit");
        config.put("searchguard.ssl.transport.enforce_hostname_verification", "false");

        List<InetSocketAddress> transportAddresses = new ArrayList<>();
        transportAddresses.add(new InetSocketAddress(InetAddress.getByName("127.0.0.1"), 9300));

        EsApiBridge apiBridge = new EsApiBridge(transportAddresses);
        stringDataStreamSource.addSink(new EsSink<>(apiBridge, config, new EsSinkFunction(), new NoOpFailureHandler()));

        //when

        executionEnvironment.execute();

        //then
    }
}