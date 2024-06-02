package com.codeDemo.es;

import com.alibaba.fastjson.JSON;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.elasticsearch.action.admin.indices.alias.IndicesAliasesRequest;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.indices.CreateIndexRequest;
import org.elasticsearch.client.indices.GetIndexRequest;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.sort.SortOrder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * Es查询工具类
 */
//@Component
@Data
@Slf4j
public class EsUtils {

    /**
     * 告警索引
     */
    @Value("${es.search.zjg_alarm_another}")
    private String index;

    @Autowired
    RestHighLevelClient client;


    /**
     * 查询ES数据(有聚合)
     *
     * @return
     */
    public SearchResponse searchAlarm(Integer from, Integer size, BoolQueryBuilder must, String index, String sortField, String sortType, List<TermsAggregationBuilder> aggList) {
        SearchSourceBuilder builder = new SearchSourceBuilder();
        builder.from(from);
        builder.size(size);
        builder.sort(sortField, sortType.equals("asc") ? SortOrder.ASC : SortOrder.DESC);

        //bool查询条件
        builder.query(must);

        //获取索引下的实际文档数
        builder.trackTotalHits(true);
        //聚合
        for (TermsAggregationBuilder agg : aggList) {
            builder.aggregation(agg);
        }

        //创建一个SearchRequest，查询的请求对象
        SearchRequest request = new SearchRequest(index).source(builder);
        try {
            log.info("ES查询请求为：{}", request);
            //获取返回的数据
            return client.search(request, RequestOptions.DEFAULT);
        } catch (Exception e) {
            log.error("ES数据搜索失败！失败原因：", e);
        }
        return null;
    }

    /**
     * 查询ES 仅聚合数据
     *
     * @return
     */
    public SearchResponse searchAggregation(BoolQueryBuilder must, String index, List<TermsAggregationBuilder> aggList) {
        SearchSourceBuilder builder = new SearchSourceBuilder();
        //bool查询条件
        builder.query(must);
        //击中数为0，不关注具体数据
        builder.size(0);

        //聚合
        for (TermsAggregationBuilder agg : aggList) {
            builder.aggregation(agg);
        }

        //创建一个SearchRequest，查询的请求对象
        SearchRequest request = new SearchRequest(index).source(builder);
        try {
            log.info("ES查询请求为：{}", request);
            //获取返回的数据
            return client.search(request, RequestOptions.DEFAULT);
        } catch (Exception e) {
            log.error("ES数据搜索失败！失败原因：", e);
        }
        return null;
    }

    /**
     * 查询ES数据(无聚合)
     *
     * @param must  查询条件对象
     * @param index 索引
     * @return
     */
    public SearchResponse searchData(QueryBuilder must, String index) {
        SearchSourceBuilder builder = new SearchSourceBuilder();
        //需要条件
        builder.query(must);
        //创建一个SearchRequest，查询的请求对象
        SearchRequest request = new SearchRequest(index).source(builder);
        try {
            log.info("ES查询请求为：{}", request);
            return client.search(request, RequestOptions.DEFAULT);
        } catch (IOException e) {
            log.error("ES查询异常！", e);
        }
        return null;
    }

    /**
     * 修改ES数据
     *
     * @param data  修改数据内容
     * @param index 修改索引
     * @param docId 修改文档的id
     * @return
     */
    public boolean updateESData(Map<String, Object> data, String index, String docId) {
        IndexRequest indexRequest = new IndexRequest(index).id(docId);
        indexRequest.source(data, XContentType.JSON);
        try {
            IndexResponse response = client.index(indexRequest, RequestOptions.DEFAULT);
            int status = response.status().getStatus();
            if (status == 200) {
                return true;
            }
            log.error("修改数据失败,失败原因：{}", response.getResult());
        } catch (IOException e) {
            log.error("ES修改数据异常", e);
        }
        return false;
    }


    /**
     * 创建索引,并添加别名
     *
     * @param index
     * @return
     */
    public boolean createIndex(String index) {
        try {
            GetIndexRequest request = new GetIndexRequest(index);
            //判断索引是否存在
            boolean exists = client.indices().exists(request, RequestOptions.DEFAULT);
            if (!exists) {
                //如果索引不存在，则创建索引
                CreateIndexRequest createIndexRequest = new CreateIndexRequest(index);
                client.indices().create(createIndexRequest, RequestOptions.DEFAULT);
            }

            //添加别名
            IndicesAliasesRequest indicesAliasesRequest = new IndicesAliasesRequest();
            indicesAliasesRequest = indicesAliasesRequest.addAliasAction(IndicesAliasesRequest.AliasActions.add().index(index).alias(this.index));
            AcknowledgedResponse acknowledgedResponse = client.indices().updateAliases(indicesAliasesRequest, RequestOptions.DEFAULT);
            boolean acknowledged = acknowledgedResponse.isAcknowledged();

            return true;
        } catch (Exception e) {
            log.error("创建索引失败", e);
        }
        return false;
    }


    /**
     * 批量写入ES数据
     *
     * @param index
     * @param dataList
     */
    public void addDataToES(String index, List<Map<String, Object>> dataList) {
        try {
            BulkRequest bulkRequest = new BulkRequest();
            for (Map<String, Object> dataMap : dataList) {
                dataMap.remove("id");
                IndexRequest indexRequest = new IndexRequest(index).source(dataMap);
                bulkRequest.add(indexRequest);
            }
            // 将数据刷新到ES中
            BulkResponse resp = client.bulk(bulkRequest, RequestOptions.DEFAULT);
            log.info("添加数据到ES : index = {},resp = {}", index, JSON.toJSONString(resp));
        } catch (Exception e) {
            log.error("添加数据到ES 失败！", e);
        }
    }

    /**
     * 判断es index 是否存在
     *
     * @return
     */
    public boolean ifIndex(String index) {
        try {
            //判断索引是否已经存在
            //创建一个对象用来传递索引名，传入要判断的索引名字
            GetIndexRequest existsRequest = new GetIndexRequest(index);
            //返回值是布尔类型，判断方法是client对象下indices()方法的exists方法，在这个方法里有索引的名字；
            return client.indices().exists(existsRequest, RequestOptions.DEFAULT);
        } catch (Exception e) {
            log.error("判断索引是否存在异常！", e);
        }
        return false;
    }


    /**
     * 查询ES数据总量
     *
     * @return
     */
    public Long searchEsDataCount(BoolQueryBuilder must, String index) {
        SearchSourceBuilder builder = new SearchSourceBuilder();
        //bool查询条件
        builder.query(must);
        //击中数为0，不关注具体数据
        builder.size(0);
        //获取索引下的实际文档数
        builder.trackTotalHits(true);

        //创建一个SearchRequest，查询的请求对象
        SearchRequest request = new SearchRequest(index).source(builder);
        try {
            log.info("ES查询请求为：{}", request);
            //获取返回的统计数据
            SearchResponse response = client.search(request, RequestOptions.DEFAULT);
            return response.getHits().getTotalHits().value;
        } catch (Exception e) {
            log.error("ES数据搜索失败！失败原因：", e);
        }
        return 0L;
    }
}
