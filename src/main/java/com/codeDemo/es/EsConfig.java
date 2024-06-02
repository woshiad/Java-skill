package com.codeDemo.es;

import com.google.common.net.HostAndPort;
import com.orbitz.consul.Consul;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * 配置注入 RestHighLevelClient 的 Bean
 * @author LJF
 */
//@Configuration
public class EsConfig {

    private Integer esPort = 8080;
    private String scheme = "http";


    private Integer consulport = 8080;

    public static final RequestOptions COMMON_OPTIONS;

    static {
        RequestOptions.Builder builder = RequestOptions.DEFAULT.toBuilder();
        COMMON_OPTIONS = builder.build();
    }

    @Bean
    public RestHighLevelClient client() {
        //定义账号密码凭证
        CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
        credentialsProvider.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials("elastic", "123asd!@#"));

        //es的ip地址
        String hostUrl = "127.0.0.1";

        if ("true".equals("isConsul.getValue()")) {
            // connect on localhost  从consul中取出需要连接es的ip地址
            Consul client = Consul.builder().withHostAndPort(HostAndPort.fromParts("consulhost", consulport)).build();
            hostUrl = client.keyValueClient().getValueAsString("xkh/" + "es/host").get();
//            return new RestHighLevelClient(
//                    RestClient.builder(new HttpHost(s1, port, scheme)).setHttpClientConfigCallback(httpAsyncClientBuilder -> {
//                        httpAsyncClientBuilder.disableAuthCaching();
//                        return httpAsyncClientBuilder.setDefaultCredentialsProvider(credentialsProvider);
//                    })
//            );

        }

        HttpHost httpHost = new HttpHost(hostUrl, esPort, scheme);


        RestClientBuilder restClientBuilder = RestClient.builder(httpHost)
                .setHttpClientConfigCallback(httpAsyncClientBuilder -> {
                    httpAsyncClientBuilder.disableAuthCaching();
                    return httpAsyncClientBuilder.setDefaultCredentialsProvider(credentialsProvider);
                });

        return new RestHighLevelClient(restClientBuilder);
    }
}
