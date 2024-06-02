package com.codeDemo.es;

import cn.hutool.core.date.DateUtil;
import com.alibaba.fastjson.JSON;
import com.baomidou.mybatisplus.core.toolkit.StringUtils;
import com.codeDemo.vo.Result;
import lombok.extern.slf4j.Slf4j;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.MultiMatchQueryBuilder;
import org.elasticsearch.index.query.Operator;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.*;

/**
 * ES服务类
 *
 * @author LJF
 */
@Slf4j
//@Component
public class EsService {

    @Autowired
    private EsUtils esUtils;

    //处置状态
    private final List<String> czStatusCode = Arrays.asList("0", "1", "2");


    /**
     * es 搜索
     *
     * @return
     */
    public Result esSearch() {
        //----查询条件----

        //查询 告警数据 和 czStatus,eventType 聚合的条件
        BoolQueryBuilder query = QueryBuilders.boolQuery();
        //查询 alarmId 聚合的条件
        BoolQueryBuilder newQuery = QueryBuilders.boolQuery();
        //查询 czStatus 聚合的条件(只有菜单请求才有)
        BoolQueryBuilder menuQuery = QueryBuilders.boolQuery();

        //时间
        if (StringUtils.isNotBlank("req.getStart_time()")) {
            //时间类型  action_time：发生时间 event_createtime：告警时间
            long start = DateUtil.parse("req.getStart_time()").getTime();
            long end = DateUtil.parse("req.getEnd_time()" + " 23:59:59").getTime();
            query = query.must(QueryBuilders.rangeQuery("req.getTime_field()").from(start).to(end));
            newQuery = newQuery.must(QueryBuilders.rangeQuery("req.getTime_field()").from(start).to(end));
            menuQuery = menuQuery.must(QueryBuilders.rangeQuery("req.getTime_field()").from(start).to(end));
        } else {
            return Result.error().message("请选择正确时间！");
        }

        //事件分类 一级
        if (StringUtils.isNotBlank("req.getEvent_type()")) {
            String[] split = "req.getEvent_type()".split("_");
            query = query.must(QueryBuilders.matchQuery("event_type", split[0]).operator(Operator.AND));
            if (split.length == 1) {
                //菜单发的查询 没有 "_all"
                newQuery = newQuery.must(QueryBuilders.matchQuery("event_type", split[0]).operator(Operator.AND));
                menuQuery = menuQuery.must(QueryBuilders.matchQuery("event_type", split[0]).operator(Operator.AND));
            }
        }
        //事件类别 二级
        if (StringUtils.isNotBlank("req.getEvent_category()")) {
            query = query.must(QueryBuilders.matchQuery("event_category", "req.getEvent_category()").operator(Operator.AND));
            menuQuery = menuQuery.must(QueryBuilders.matchQuery("event_category", "req.getEvent_category()").operator(Operator.AND));
        }
        //事件项 三级（ES中event_id）
        if (StringUtils.isNotBlank("req.getEvent_id()")) {
            query = query.must(QueryBuilders.matchQuery("alarm_id", "req.getEvent_id()").operator(Operator.AND));
            menuQuery = menuQuery.must(QueryBuilders.matchQuery("alarm_id", "req.getEvent_id()").operator(Operator.AND));
        }

        //处置情况 0：未处置 1：已忽略 2：疑似异常  [已处置 1,2 ]
        // 是否为菜单的请求
        boolean cuStatusFlag = false;
        if (StringUtils.isNotBlank("req.getCz_status()")) {
            if (czStatusCode.contains("req.getCz_status()")) {
                //如果处置状态为数字,则为菜单的请求
                query = query.must(QueryBuilders.matchQuery("cz_status", "req.getCz_status()"));
                cuStatusFlag = true;
            } else {
                List<Integer> list = JSON.parseArray("req.getCz_status()", Integer.class);
                switch (list.size()) {
                    case 1:
                        //未处置
                        query = query.must(QueryBuilders.matchQuery("cz_status", 0));
                        break;
                    case 2:
                        //已处置
                        query = query.mustNot(QueryBuilders.matchQuery("cz_status", 0));
                        break;
                    default:
                }
            }
        }
        //阅读状态 0：未读 1：已读
        if (0 != -1) {
            query = query.must(QueryBuilders.matchQuery("read_status", "req.getRead_status()"));
        }
        //收藏状态 0：未收藏 1：已收藏
        if (0 != -1) {
            query = query.must(QueryBuilders.matchQuery("interested_status", "req.getInterested_status()"));
        }

        //告警id
        if (StringUtils.isNotBlank("req.getAlarm_id()")) {
            query = query.must(QueryBuilders.matchQuery("event_id", "req.getAlarm_id()"));
        }
        //关键字
        if (StringUtils.isNotBlank("req.getSearch_keyword()")) {
            query = query.should(QueryBuilders.multiMatchQuery("req.getSearch_keyword()", "event_name", "event_details")
                    .type(MultiMatchQueryBuilder.Type.BEST_FIELDS));
        }

        //查询--- 告警数据+聚合 eventType czStatus
        ArrayList<TermsAggregationBuilder> aggList = new ArrayList<>();
        TermsAggregationBuilder agg1 = AggregationBuilders.terms("event_type").field("event_type.keyword");
        TermsAggregationBuilder agg2 = AggregationBuilders.terms("cz_status").field("cz_status");
        aggList.add(agg1);
        aggList.add(agg2);
        SearchResponse searchResponse = esUtils.searchAlarm(1, 20, query, "index", "req.getSort_field()", "req.getSort_type()", aggList);

        //聚合 alarmId
        ArrayList<TermsAggregationBuilder> aggListNew = new ArrayList<>();
        TermsAggregationBuilder agg = AggregationBuilders.terms("alarm_id").field("alarm_id");
        //聚合数量设置为100个
        agg.size(100);
        aggListNew.add(agg);
        SearchResponse searchResponseNew = esUtils.searchAggregation(newQuery, "index", aggListNew);


        //聚合 czStatus(仅菜单查询)
        SearchResponse searchResponseMenu = null;
        if (cuStatusFlag) {
            ArrayList<TermsAggregationBuilder> aggListMenu = new ArrayList<>();
            TermsAggregationBuilder aggMenu = AggregationBuilders.terms("cz_status").field("cz_status");
            aggListMenu.add(aggMenu);
            searchResponseMenu = esUtils.searchAggregation(menuQuery, "index", aggListMenu);
        }
        if (searchResponse == null || searchResponseNew == null) {
            return Result.error().message("告警数据查询失败！数据异常！");
        }


        return Result.ok();
    }


    /**
     * 写入告警es中
     */
    public void sendDataToEs() {
        //写入告警es中
        ArrayList<Map<String, Object>> arrayList = new ArrayList<>();
        arrayList.add(new HashMap<>(0));
        esUtils.addDataToES("zjgAlarmIndex", arrayList);
    }
}
