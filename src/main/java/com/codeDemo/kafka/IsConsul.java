package com.codeDemo.kafka;

import lombok.Data;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;

@Data
//@Configuration
@ConfigurationProperties(prefix = "consul")
public class IsConsul {

    @Value("${consul.enabled}")
    private String value;
    @Value("${consul.host}")
    private String host;

    @Value("${consul.port}")
    private Integer port;
}
