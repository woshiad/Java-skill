package com.codeDemo.kafka;

import com.google.common.net.HostAndPort;
import com.orbitz.consul.Consul;
import com.orbitz.consul.KeyValueClient;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.KafkaAdminClient;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.util.Properties;

//@Component
@Slf4j
public class CreatClient {

    private final ConsumerEntity consumerEntity;

    @Value("${consul.host}")
    private String consulhost;

    @Value("${consul.port}")
    private Integer consulport;

    @Value("${logKafka.group_id}")
    private String kafkaConsumerLogGroupId;
    @Value("${logKafka.enable_auto_commit}")
    private String kafkaConsumerLogEnableAutoCommit;
//    @Value("${logKafka.bootstrap_servers}")
//    private String kafkaConsumerLogBootstrapServers;

    @Value("${data_handle.group_id}")
    private String dataHandleGroupId;
    @Value("${data_handle.max_poll}")
    private Integer maxPoll;

    @Autowired
    IsConsul isConsul;

    public CreatClient(ConsumerEntity consumerEntity) {
        this.consumerEntity = consumerEntity;
    }


    public KafkaConsumer getConsumer() {
        Properties properties = new Properties();
        if ("true".equals(isConsul.getValue())) {
            Consul client = Consul.builder().withHostAndPort(HostAndPort.fromParts(consulhost, consulport)).build();
            KeyValueClient kvClient = client.keyValueClient();
            String[] split = consumerEntity.getBootstrapServers().split(":");
            String espost = split[split.length - 1];
            String value = kvClient.getValueAsString("xkh/kafka/host").get() + ":" + espost;
            properties.put("bootstrap.servers", value);
            properties.put("group.id", consumerEntity.getGroupId());
            properties.put("enable.auto.commit", consumerEntity.getEnableAutoCommit());
            properties.put("key.deserializer", consumerEntity.getKeyDeserializer());
            properties.put("value.deserializer", consumerEntity.getValueDeserializer());
            properties.put("max.poll.records", consumerEntity.getMaxPollRecords());
            properties.put("max.poll.interval.ms", consumerEntity.getMaxPollIntervalMs());
            properties.put("auto.offset.reset", consumerEntity.getAutoOffsetReset());
            return new KafkaConsumer(properties);
        }
        properties.put("bootstrap.servers", consumerEntity.getBootstrapServers());
        properties.put("group.id", consumerEntity.getGroupId());
        properties.put("enable.auto.commit", consumerEntity.getEnableAutoCommit());
        properties.put("key.deserializer", consumerEntity.getKeyDeserializer());
        properties.put("value.deserializer", consumerEntity.getValueDeserializer());
        properties.put("max.poll.records", consumerEntity.getMaxPollRecords());
        properties.put("max.poll.interval.ms", consumerEntity.getMaxPollIntervalMs());
        properties.put("auto.offset.reset", consumerEntity.getAutoOffsetReset());
        return new KafkaConsumer(properties);
    }

    public AdminClient getAdminClient() {
        if ("true".equals(isConsul.getValue())) {
            Consul client = Consul.builder().withHostAndPort(HostAndPort.fromParts(consulhost, consulport)).build();
            KeyValueClient kvClient = client.keyValueClient();
            String value = kvClient.getValueAsString("xkh/kafka/host").get() + ":50004";
            Properties properties = new Properties();
            properties.put("bootstrap.servers", value);
            properties.put("group.id", consumerEntity.getGroupId());
            properties.put("enable.auto.commit", consumerEntity.getEnableAutoCommit());
            properties.put("key.deserializer", consumerEntity.getKeyDeserializer());
            properties.put("value.deserializer", consumerEntity.getValueDeserializer());
            properties.put("max.poll.records", consumerEntity.getMaxPollRecords());
            return AdminClient.create(properties);
        }
        Properties properties = new Properties();
        properties.put("bootstrap.servers", consumerEntity.getBootstrapServers());
        properties.put("group.id", consumerEntity.getGroupId());
        properties.put("enable.auto.commit", consumerEntity.getEnableAutoCommit());
        properties.put("key.deserializer", consumerEntity.getKeyDeserializer());
        properties.put("value.deserializer", consumerEntity.getValueDeserializer());
        properties.put("max.poll.records", consumerEntity.getMaxPollRecords());
        return AdminClient.create(properties);
    }

    public Producer getProducer() {
        if ("true".equals(isConsul.getValue())) {
            Consul client = Consul.builder().withHostAndPort(HostAndPort.fromParts(consulhost, consulport)).build();
            KeyValueClient kvClient = client.keyValueClient();
            String value = kvClient.getValueAsString("xkh/kafka/host").get() + ":50004";
            Properties properties = new Properties();
            properties.put("bootstrap.servers", value);
            properties.put("key.serializer", consumerEntity.getKeySerializer());
            properties.put("value.serializer", consumerEntity.getValueSerializer());
            return new KafkaProducer(properties);
        }
        Properties properties = new Properties();
        properties.put("bootstrap.servers", consumerEntity.getBootstrapServers());
        properties.put("key.serializer", consumerEntity.getKeySerializer());
        properties.put("value.serializer", consumerEntity.getValueSerializer());
        return new KafkaProducer(properties);
    }

    //-------------------------------界面使用kafka配置--------------------------------

    /**
     * 获取kafka topic Admin(用于获取topic List)
     *
     * @return
     */
    public AdminClient getKafkaAdminClient() {
        Properties properties = new Properties();
        if ("true".equals(isConsul.getValue())) {
            Consul client = Consul.builder().withHostAndPort(HostAndPort.fromParts(consulhost, consulport)).build();
            KeyValueClient kvClient = client.keyValueClient();
            String value = kvClient.getValueAsString("xkh/kafka/host").get() + ":50004";
            properties.put("bootstrap.servers", value);
        } else {
            properties.put("bootstrap.servers", consumerEntity.getBootstrapServers());
        }
        properties.put("key.deserializer", consumerEntity.getKeyDeserializer());
        properties.put("value.deserializer", consumerEntity.getValueDeserializer());

        return KafkaAdminClient.create(properties);
    }

    /**
     * 获取Kafka Consumer（用于界面获取一条日志）
     *
     * @return
     */
    public KafkaConsumer getKafkaConsumerToLog() {
        Properties properties = new Properties();
        if ("true".equals(isConsul.getValue())) {
            Consul client = Consul.builder().withHostAndPort(HostAndPort.fromParts(consulhost, consulport)).build();
            KeyValueClient kvClient = client.keyValueClient();
            String value = kvClient.getValueAsString("xkh/kafka/host").get() + ":50004";
            properties.put("bootstrap.servers", value);
        } else {
            properties.put("bootstrap.servers", consumerEntity.getBootstrapServers());
        }
        properties.put("group.id", kafkaConsumerLogGroupId);
        properties.put("key.deserializer", consumerEntity.getKeyDeserializer());
        properties.put("value.deserializer", consumerEntity.getValueDeserializer());
        properties.put("enable.auto.commit", kafkaConsumerLogEnableAutoCommit);
        properties.put("auto.offset.reset", consumerEntity.getAutoOffsetReset());

        return new KafkaConsumer(properties);
    }

    /**
     * 获取Kafka Consumer（用于获取预处理之后的数据）
     *
     * @return
     */
    public KafkaConsumer getKafkaConsumerClient() {
        Properties properties = new Properties();
        if ("true".equals(isConsul.getValue())) {
            Consul client = Consul.builder().withHostAndPort(HostAndPort.fromParts(consulhost, consulport)).build();
            KeyValueClient kvClient = client.keyValueClient();
            String value = kvClient.getValueAsString("xkh/kafka/host").get() + ":50004";
            properties.put("bootstrap.servers", value);
        } else {
            properties.put("bootstrap.servers", consumerEntity.getBootstrapServers());
        }
        properties.put("group.id", dataHandleGroupId);
        properties.put("key.deserializer", consumerEntity.getKeyDeserializer());
        properties.put("value.deserializer", consumerEntity.getValueDeserializer());
        properties.put("enable.auto.commit", kafkaConsumerLogEnableAutoCommit);
        properties.put("auto.offset.reset", consumerEntity.getAutoOffsetReset());
        properties.put("max.poll.records", maxPoll);

        return new KafkaConsumer(properties);
    }

    /**
     * 获取Kafka Producer（用于定时写入监管策略配置）
     *
     * @return
     */
    public Producer getKafkaProducer() {
        Properties properties = new Properties();
        if ("true".equals(isConsul.getValue())) {
            Consul client = Consul.builder().withHostAndPort(HostAndPort.fromParts(consulhost, consulport)).build();
            KeyValueClient kvClient = client.keyValueClient();
            String value = kvClient.getValueAsString("xkh/kafka/host").get() + ":50004";
            properties.put("bootstrap.servers", value);
        } else {
            properties.put("bootstrap.servers", consumerEntity.getBootstrapServers());
        }
        properties.put("key.serializer", consumerEntity.getKeySerializer());
        properties.put("value.serializer", consumerEntity.getValueSerializer());

        return new KafkaProducer(properties);
    }


}
