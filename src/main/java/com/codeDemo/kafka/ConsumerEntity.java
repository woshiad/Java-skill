package com.codeDemo.kafka;

import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

@Slf4j
@Data
@Component
public class ConsumerEntity {

    @Value("${kafka.bootstrap_servers}")
    private String bootstrapServers;

    @Value("${kafka.producer.key_serializer}")
    private String keySerializer;

    @Value("${kafka.producer.value_serializer}")
    private String valueSerializer;

    @Value("${kafka.consumer.group_id}")
    private String groupId;

    @Value("${kafka.consumer.enable_auto_commit}")
    private String enableAutoCommit;

    @Value("${kafka.consumer.key_deserializer}")
    private String keyDeserializer;

    @Value("${kafka.consumer.value_deserializer}")
    private String valueDeserializer;

    @Value("${kafka.consumer.max_poll_records}")
    private String maxPollRecords;

    @Value("${kafka.consumer.max_poll_interval_ms}")
    private String maxPollIntervalMs;

    @Value("${kafka.consumer.auto_offset_reset}")
    private String autoOffsetReset;

}
