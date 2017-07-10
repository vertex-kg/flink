package sink;

import com.floragunn.searchguard.ssl.SearchGuardSSLPlugin;
import org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchApiCallBridge;
import org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchSinkBase;
import org.apache.flink.streaming.connectors.elasticsearch.util.ElasticsearchUtils;
import org.apache.flink.streaming.connectors.elasticsearch5.Elasticsearch5ApiCallBridge;
import org.apache.flink.util.Preconditions;
import org.elasticsearch.action.bulk.BackoffPolicy;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkProcessor;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.network.NetworkModule;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.transport.Netty3Plugin;
import org.elasticsearch.transport.client.PreBuiltTransportClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.net.InetSocketAddress;
import java.util.List;
import java.util.Map;

public class EsApiBridge implements ElasticsearchApiCallBridge {

    private static final long serialVersionUID = -5222683870097809633L;

    private static final Logger LOG = LoggerFactory.getLogger(Elasticsearch5ApiCallBridge.class);

    /**
     * User-provided transport addresses.
     *
     * We are using {@link InetSocketAddress} because {@link TransportAddress} is not serializable in Elasticsearch 5.x.
     */
    private final List<InetSocketAddress> transportAddresses;

    EsApiBridge(List<InetSocketAddress> transportAddresses) {
        Preconditions.checkArgument(transportAddresses != null && !transportAddresses.isEmpty());
        this.transportAddresses = transportAddresses;
    }

    @Override
    public Client createClient(Map<String, String> clientConfig) {
        Settings settings = Settings.builder().put(clientConfig)
//                .put(NetworkModule.HTTP_TYPE_KEY, Netty3Plugin.NETTY_HTTP_TRANSPORT_NAME)
//                .put(NetworkModule.TRANSPORT_TYPE_KEY, Netty3Plugin.NETTY_TRANSPORT_NAME)
                .build();

        TransportClient transportClient = new PreBuiltTransportClient(settings, SearchGuardSSLPlugin.class);
//        TransportClient transportClient = new PreBuiltTransportClient(settings);
        for (TransportAddress transport : ElasticsearchUtils.convertInetSocketAddresses(transportAddresses)) {
            transportClient.addTransportAddress(transport);
        }

        // verify that we actually are connected to a cluster
        if (transportClient.connectedNodes().isEmpty()) {
            throw new RuntimeException("Elasticsearch client is not connected to any Elasticsearch nodes!");
        }

        if (LOG.isInfoEnabled()) {
            LOG.info("Created Elasticsearch TransportClient with connected nodes {}", transportClient.connectedNodes());
        }

        return transportClient;
    }

    @Override
    public Throwable extractFailureCauseFromBulkItemResponse(BulkItemResponse bulkItemResponse) {
        if (!bulkItemResponse.isFailed()) {
            return null;
        } else {
            return bulkItemResponse.getFailure().getCause();
        }
    }

    @Override
    public void configureBulkProcessorBackoff(
            BulkProcessor.Builder builder,
            @Nullable ElasticsearchSinkBase.BulkFlushBackoffPolicy flushBackoffPolicy) {

        BackoffPolicy backoffPolicy;
        if (flushBackoffPolicy != null) {
            switch (flushBackoffPolicy.getBackoffType()) {
                case CONSTANT:
                    backoffPolicy = BackoffPolicy.constantBackoff(
                            new TimeValue(flushBackoffPolicy.getDelayMillis()),
                            flushBackoffPolicy.getMaxRetryCount());
                    break;
                case EXPONENTIAL:
                default:
                    backoffPolicy = BackoffPolicy.exponentialBackoff(
                            new TimeValue(flushBackoffPolicy.getDelayMillis()),
                            flushBackoffPolicy.getMaxRetryCount());
            }
        } else {
            backoffPolicy = BackoffPolicy.noBackoff();
        }

        builder.setBackoffPolicy(backoffPolicy);
    }

    @Override
    public void cleanup() {
        // nothing to cleanup
    }

}
