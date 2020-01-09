package com.atguigu.elasticsearch.demo;

import com.alibaba.fastjson.JSON;
import com.atguigu.elasticsearch.demo.config.ElasticSearchConfig;
import com.atguigu.elasticsearch.demo.pojo.User;

import com.atguigu.elasticsearch.demo.repository.userRepository;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.index.query.Operator;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.aggregations.Aggregation;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.Aggregations;
import org.elasticsearch.search.aggregations.bucket.terms.ParsedStringTerms;
import org.elasticsearch.search.aggregations.bucket.terms.StringTerms;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightBuilder;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightField;
import org.elasticsearch.search.sort.SortBuilders;
import org.elasticsearch.search.sort.SortOrder;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.aggregator.AggregateWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.elasticsearch.core.ElasticsearchRestTemplate;
import org.springframework.data.elasticsearch.core.aggregation.AggregatedPage;
import org.springframework.data.elasticsearch.core.query.NativeSearchQueryBuilder;
import org.springframework.data.elasticsearch.core.query.Query;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

@SpringBootTest
class ElasticsearchDemoApplicationTests {
    @Autowired
    private ElasticsearchRestTemplate restTemplate;
    @Autowired
    private userRepository userRepository;
    @Autowired
    private RestHighLevelClient restHighLevelClient;
    @Test
    void contextLoads() {
        this.restTemplate.createIndex(User.class);
        this.restTemplate.putMapping(User.class);
    }
    @Test
    public void testDocument(){
//       this.userRepository.save(new User(1l,"柳岩真漂亮",38,"123456"));
        List<User> users = new ArrayList<>();
        users.add(new User(1l, "柳岩", 18, "123456"));
        users.add(new User(2l, "范冰冰", 19, "654321"));
        users.add(new User(3l, "李冰冰", 20, "123456"));
        users.add(new User(4l, "锋哥", 21, "654321"));
        users.add(new User(5l, "小鹿", 22, "123456"));
        users.add(new User(6l, "韩红", 23, "654321"));
        this.userRepository.saveAll(users);
    }
    @Test
    public void testDelete(){
        this.userRepository.deleteById(6l);
    }
    @Test
    public void testQuery(){
//        System.out.println(this.userRepository.findById(5l));
//      this.userRepository.findAllById(Arrays.asList(1l,2l,3l,4l)).forEach(System.out::print);
//        this.userRepository.findByAgeBetween(19,22).forEach(System.out::println);
//         this.userRepository.findNative(19,20).forEach(System.out::println);
    }
    @Test
    public void testPage(){
//        this.userRepository.search(QueryBuilders.rangeQuery("age").gte(19).lte(22)).forEach(System.out::println);

//        Page<User> userPage = this.userRepository.search(QueryBuilders.rangeQuery("age").gte(19).lte(22), PageRequest.of(1, 2));
//        System.out.println(userPage.getTotalPages());
//        System.out.println(userPage.getTotalElements());
//        userPage.getContent().forEach(System.out::println);
        NativeSearchQueryBuilder nativeSearchQueryBuilder = new NativeSearchQueryBuilder();
        nativeSearchQueryBuilder.withQuery(QueryBuilders.matchQuery("name","冰冰").operator(Operator.AND));
        nativeSearchQueryBuilder.withSort(SortBuilders.fieldSort("age").order(SortOrder.DESC));
        nativeSearchQueryBuilder.withPageable(PageRequest.of(0,2));
        nativeSearchQueryBuilder.withHighlightBuilder(new HighlightBuilder().field("name").preTags("<em>").postTags("</em>"));
        nativeSearchQueryBuilder.addAggregation(AggregationBuilders.terms("passwordAgg").field("password"));
        AggregatedPage<User> search = (AggregatedPage)this.userRepository.search(nativeSearchQueryBuilder.build());
        System.out.println(search.getTotalElements());
        System.out.println(search.getTotalPages());
        search.getContent().forEach(System.out::println);
        ParsedStringTerms stringTerms = (ParsedStringTerms)search.getAggregation("passwordAgg");
        stringTerms.getBuckets().forEach(bucket -> {
            System.out.println(bucket.getKeyAsString());
        });
    }

    @Test
    public void testSearch(){
        NativeSearchQueryBuilder nativeSearchQueryBuilder = new NativeSearchQueryBuilder();
        nativeSearchQueryBuilder.withQuery(QueryBuilders.matchQuery("name","冰冰").operator(Operator.AND));
        nativeSearchQueryBuilder.withSort(SortBuilders.fieldSort("age").order(SortOrder.DESC));
        nativeSearchQueryBuilder.withPageable(PageRequest.of(0,2));
        nativeSearchQueryBuilder.withHighlightBuilder(new HighlightBuilder().field("name").preTags("<em>").postTags("</em>"));
        nativeSearchQueryBuilder.addAggregation(AggregationBuilders.terms("passwordAgg").field("password"));
        Object query = this.restTemplate.query(nativeSearchQueryBuilder.build(), response -> {
            SearchHit[] hits = response.getHits().getHits();
            for (SearchHit hit : hits) {
                String sourceAsString = hit.getSourceAsString();
                User user = JSON.parseObject(sourceAsString, User.class);
                System.out.println(user);
                System.out.println(sourceAsString);
                Map<String, HighlightField> highlightFields = hit.getHighlightFields();
                HighlightField name = highlightFields.get("name");
                user.setName(name.getFragments()[0].string());
                System.out.println(user);
            }

            Map<String, Aggregation> asMap = response.getAggregations().getAsMap();
            ParsedStringTerms parsedStringTerms = (ParsedStringTerms) asMap.get("passwordAgg");
            parsedStringTerms.getBuckets().forEach(bucket -> {
                System.out.println(bucket.getKeyAsString());
            });
            return null;
        });
    }
    @Test
    public void testHighLevel() throws IOException {
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.query(QueryBuilders.matchQuery("name","冰冰").operator(Operator.AND));
        searchSourceBuilder.sort("age", SortOrder.DESC);
        searchSourceBuilder.from(0);
        searchSourceBuilder.size(2);
        searchSourceBuilder.highlighter(new HighlightBuilder().field("name").preTags("<em>").postTags("</em>"));
        searchSourceBuilder.aggregation(AggregationBuilders.terms("passwordAgg").field("password")
        .subAggregation(AggregationBuilders.avg("ageAgg").field("age")));
        SearchResponse response = this.restHighLevelClient.search(new SearchRequest(new String[]{"user"}, searchSourceBuilder), RequestOptions.DEFAULT);
        SearchHit[] hits = response.getHits().getHits();
        for (SearchHit hit : hits) {
            String sourceAsString = hit.getSourceAsString();
            User user = JSON.parseObject(sourceAsString, User.class);
            System.out.println(user);
            System.out.println(sourceAsString);
            Map<String, HighlightField> highlightFields = hit.getHighlightFields();
            HighlightField name = highlightFields.get("name");
            user.setName(name.getFragments()[0].string());
            System.out.println(user);
        }
        Map<String, Aggregation> asMap = response.getAggregations().getAsMap();
        ParsedStringTerms parsedStringTerms = (ParsedStringTerms) asMap.get("passwordAgg");
        parsedStringTerms.getBuckets().forEach(bucket -> {
            System.out.println(bucket.getKeyAsString());
        });

    }
}
