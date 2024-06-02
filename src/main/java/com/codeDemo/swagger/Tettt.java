package com.codeDemo.swagger;

import com.alibaba.fastjson.JSON;
import lombok.extern.slf4j.Slf4j;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.io.IOException;

@Slf4j
@Component
public class Tettt {

    @Autowired
    RestHighLevelClient client;

    @PostConstruct
    public void tess() throws IOException {

        SearchSourceBuilder builder = new SearchSourceBuilder();
        builder.query(QueryBuilders.matchAllQuery());

        SearchRequest request = new SearchRequest("cxy_index").source(builder);
        SearchResponse search = client.search(request, RequestOptions.DEFAULT);

        System.out.println(JSON.toJSONString(search.getHits().getHits()));

    }


}
