package com.elasticsearch.es.controller;

import cn.hutool.core.util.StrUtil;
import com.elasticsearch.es.config.UtilPublic;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.RangeQueryBuilder;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.aggregations.AbstractAggregationBuilder;
import org.elasticsearch.search.aggregations.Aggregation;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.terms.StringTerms;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.sort.SortOrder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.text.ParseException;
import java.util.*;

/**
 * @program: es-demo
 * @description: es增删查改接口demo
 * @author: 01
 * @create: 2018-07-01 10:42
 **/
@RestController
@RequestMapping("/es")
public class EsCrudController {

    @Autowired
    private TransportClient client;

    /**
     * 按id查询
     *
     * @param id
     * @return
     */
    @GetMapping("/get")
    public ResponseEntity searchById(@RequestParam("id") String id) {
        if (id.isEmpty()) {
            return new ResponseEntity(HttpStatus.NOT_FOUND);
        }

        // 通过索引、类型、id向es进行查询数据
        GetResponse response = client.prepareGet("logstash", "logs", id).get();

        if (!response.isExists()) {
            return new ResponseEntity(HttpStatus.NOT_FOUND);
        }

        // 返回查询到的数据
        return new ResponseEntity(response.getSource(), HttpStatus.OK);
    }

    @GetMapping("/hello")
    public Map hello() {
        Map map = new HashMap();
        map.put("hello", "world");
        return map;
    }

    /**
     * 复合查询接口
     *
     * @param idcard
     * @param user
     * @return
     */
    @PostMapping("/query")
    public ResponseEntity query(@RequestParam(value = "user", required = false) String user,
                                @RequestParam(value = "idcard", required = false) String idcard,
                                @RequestParam(value = "from", required = false) String from,
                                @RequestParam(value = "to", required = false) String to
    ) throws ParseException {

        // 组装查询条件
        BoolQueryBuilder boolQuery = QueryBuilders.boolQuery();
        if (user != null) {
            boolQuery.must(QueryBuilders.matchQuery("user", user));
        }
        if (idcard != null) {
           /* Map<String,String>map=new HashMap<>();
            map.put("对象特征类型","A010111");
            map.put("对象特征字符串",idcard);*/
            boolQuery.must(QueryBuilders.matchQuery("message", idcard)).must(QueryBuilders.matchQuery("module", "全息档案")).must(QueryBuilders.matchQuery("uri", "index"));
        }
        // 以timestamp作为时间范围
        if(StrUtil.isNotEmpty(from)||StrUtil.isNotEmpty(to)){
        RangeQueryBuilder rangeQuery = QueryBuilders.rangeQuery("@timestamp");
        if(StrUtil.isNotEmpty(from)){
            rangeQuery.from(UtilPublic.CSTtoUTC(from,"yyyy-MM-dd HH:mm:ss"));
        }
        if(StrUtil.isNotEmpty(to)){
            rangeQuery.to(UtilPublic.CSTtoUTC(to,"yyyy-MM-dd HH:mm:ss"));
        }
        boolQuery.filter(rangeQuery);
        }
        /*if (ltWordCount != null && ltWordCount > 0) {
            rangeQuery.to(ltWordCount);
        }*/
       /* if (author != null) {
            boolQuery.must(QueryBuilders.matchQuery("author", author));
        }
        if (wordCount != null) {
            boolQuery.must(QueryBuilders.matchQuery("word_count", wordCount));
        }
        if (publishDate != null) {
            boolQuery.must(QueryBuilders.matchQuery("publish_date", publishDate));
        }
        // 以word_count作为条件范围
        RangeQueryBuilder rangeQuery = QueryBuilders.rangeQuery("word_count").from(gtWordCount);
        if (ltWordCount != null && ltWordCount > 0) {
            rangeQuery.to(ltWordCount);
        }
        boolQuery.filter(rangeQuery);*/

        // 组装查询请求
        SearchRequestBuilder requestBuilder = client.prepareSearch("logstash")
                .setTypes("logs")
                .setSearchType(SearchType.DFS_QUERY_THEN_FETCH)
                .setQuery(boolQuery)
                .setFrom(0)
                .setSize(1000)
                .addSort("@timestamp", SortOrder.DESC);
                ;

        // 发送查询请求
        SearchResponse response = requestBuilder.get();

        // 组装查询到的数据集
        List<Map<String, Object>> result = new ArrayList<>();
        for (SearchHit searchHitFields : response.getHits()) {
            result.add(searchHitFields.getSource());
        }
        result.forEach(e -> {
            try {
                e.put("timestamp", UtilPublic.UTCToCST(String.valueOf(e.get("@timestamp")), "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'"));
            } catch (ParseException e1) {
                e1.printStackTrace();
            }
        });

        return new ResponseEntity(result, HttpStatus.OK);
    }
    /**
     * 被查询人查询次数
     * @return
     */
    @PostMapping("/aggr")
    public ResponseEntity aggr(
                                @RequestParam(value = "from", required = false) String from,
                                @RequestParam(value = "to", required = false) String to
    ) throws ParseException {

        // 组装查询条件
        BoolQueryBuilder boolQuery = QueryBuilders.boolQuery();

        // 以timestamp作为时间范围
        if(StrUtil.isNotEmpty(from)||StrUtil.isNotEmpty(to)){
            RangeQueryBuilder rangeQuery = QueryBuilders.rangeQuery("@timestamp");
            if(StrUtil.isNotEmpty(from)){
                rangeQuery.from(UtilPublic.CSTtoUTC(from,"yyyy-MM-dd HH:mm:ss"));
            }
            if(StrUtil.isNotEmpty(to)){
                rangeQuery.to(UtilPublic.CSTtoUTC(to,"yyyy-MM-dd HH:mm:ss"));
            }
            boolQuery.filter(rangeQuery);
        }
        //聚合查询
        AbstractAggregationBuilder aggregation = AggregationBuilders.terms("per_count").field("message").order(Terms.Order.count(false));;
        // 组装查询请求
        SearchRequestBuilder requestBuilder = client.prepareSearch("logstash")
                .setTypes("logs")
                .setSearchType(SearchType.DFS_QUERY_THEN_FETCH)
                .setQuery(boolQuery)
                .addAggregation(aggregation)
                .setFrom(0)
                .setSize(1000);


        // 发送查询请求
        SearchResponse response = requestBuilder.get();

        // 组装查询到的数据集
        List<Map<String, Object>> result = new ArrayList<>();
        for (SearchHit searchHitFields : response.getHits()) {
            result.add(searchHitFields.getSource());
        }


        return new ResponseEntity(result, HttpStatus.OK);
    }

    /**
     * 用户查询次数（e.g.anguanshi查询1000次）
     * @return
     */
    @GetMapping("/user")
    public ResponseEntity aaa(){
        Map<String,Object> map =new HashMap<>();
        //创建TransportClient对象
        BoolQueryBuilder boolQuery = QueryBuilders.boolQuery();
        boolQuery.must(QueryBuilders.matchQuery("module", "全息档案")).must(QueryBuilders.matchQuery("uri", "index"));

        AbstractAggregationBuilder aggregation = AggregationBuilders.terms("per_count").field("user");
        SearchResponse response = client.prepareSearch("logstash").setTypes("logs")
                .setQuery(boolQuery).setSize(100)
                .addAggregation(aggregation)
                .execute()
                .actionGet();
        /*SearchHits hits = response.getHits();
        for(SearchHit hit:hits){
            System.out.println("id:"+hit.getSource().get("message")+"\ttitle:"+hit.getSource().get("user")+"====="+hit.getSource().get(""));

        }*/
        Map<String, Aggregation> aggMap = response.getAggregations().asMap();
        StringTerms teamAgg= (StringTerms) aggMap.get("per_count");
        Iterator<StringTerms.Bucket> teamBucketIt = teamAgg.getBuckets().iterator();
        while (teamBucketIt .hasNext()) {
            StringTerms.Bucket buck = teamBucketIt.next();
            Object team = buck.getKey();
            long count = buck.getDocCount();
            map.put(team.toString(),count);
            System.out.println("==========="+team.toString()+":"+count);
        }

        return new ResponseEntity(map, HttpStatus.OK);
    }

}