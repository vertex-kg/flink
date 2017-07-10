package sink;

import org.apache.flink.streaming.connectors.elasticsearch.ActionRequestFailureHandler;
import org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchApiCallBridge;
import org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchSinkBase;
import org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchSinkFunction;
import org.apache.flink.streaming.connectors.elasticsearch5.ElasticsearchSink;

import java.net.InetSocketAddress;
import java.util.List;
import java.util.Map;

public class EsSink<T> extends ElasticsearchSinkBase<T> {

    public EsSink(ElasticsearchApiCallBridge callBridge, Map<String, String> userConfig, ElasticsearchSinkFunction<T> elasticsearchSinkFunction, ActionRequestFailureHandler failureHandler) {
        super(callBridge, userConfig, elasticsearchSinkFunction, failureHandler);
    }
}
