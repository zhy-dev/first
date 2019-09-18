package com.leyou.es;

import com.alibaba.fastjson.JSON;
import com.google.gson.Gson;
import com.leyou.pojo.Item;
import org.apache.http.HttpHost;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.text.Text;
import org.elasticsearch.common.unit.Fuzziness;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.Aggregations;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightBuilder;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightField;
import org.elasticsearch.search.sort.SortOrder;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class ESManager {

    RestHighLevelClient client = null;
    Gson gson = new Gson();

    @Before
    public void init(){
        client = new RestHighLevelClient(
                RestClient.builder(
                        new HttpHost("127.0.0.1",9201,"http"),
                        new HttpHost("127.0.0.1",9202,"http"),
                        new HttpHost("127.0.0.1",9203,"http")));
    }

    //新增和修改
    @Test
    public void docTest() throws Exception{
        Item item = new Item("1", "小米9手机", "手机", "小米", 3999.0, "123.jpg");

        //indexRequest专门用来插入索引数据的对象
        IndexRequest request = new IndexRequest("item", "docs", item.getId());

        //把对象转成json字符串 两种
        String jsonString = JSON.toJSONString(item);  //fastjson 转 json方式
        //String jsonString = gson.toJson(item);      //gson  转  json方式

        request.source(jsonString, XContentType.JSON);
        client.index(request, RequestOptions.DEFAULT);
    }

    //删除文档
    @Test
    public void deleteDocTest() throws Exception{
        DeleteRequest request = new DeleteRequest("item", "docs", "1");
        client.delete(request,RequestOptions.DEFAULT);
    }

    //批量新增
    @Test
    public void bulkAddDocTest() throws Exception{
        ArrayList<Item> list = new ArrayList<>();

        list.add(new Item("1","小米手机7","手机","小米",3299.0,"234.jpg"));
        list.add(new Item("2","坚果手机R1","手机","锤子",3699.0,"234.jpg"));
        list.add(new Item("3","华为META10","手机","华为",4499.0,"234.jpg"));
        list.add(new Item("4","小米Max2S","手机","小米",4299.0,"234.jpg"));
        list.add(new Item("5","荣耀V10","手机","华为",2799.0,"234.jpg"));

        BulkRequest request = new BulkRequest();
        /*for (Item item : list) {
            IndexRequest indexRequest = new IndexRequest("item", "docs", item.getId());
            String jsonString = gson.toJson(item); //gson 转 json的方式
            indexRequest.source(jsonString,XContentType.JSON);
            request.add(indexRequest);
        }*/
        list.forEach(item -> {
            IndexRequest indexRequest = new IndexRequest("item", "docs", item.getId());
            String jsonString = gson.toJson(item); //gson 转 json的方式
            indexRequest.source(jsonString,XContentType.JSON);
            request.add(indexRequest);
        });
        client.bulk(request,RequestOptions.DEFAULT);
    }

    //各种查询
    @Test
    public void searchTest() throws Exception{
        //构建一个用来查询的对象
        SearchRequest searchRequest = new SearchRequest("item").types("docs");
        //构建查询方式
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();

        //查询所有
        //searchSourceBuilder.query(QueryBuilders.matchAllQuery());

        //构建聚合的条件
        searchSourceBuilder.aggregation(AggregationBuilders.terms("brandCount").field("brand"));

        //分页
        /*searchSourceBuilder.from(0);
        searchSourceBuilder.size(2);*/

        //排序
        //searchSourceBuilder.sort("price", SortOrder.DESC);

        //构造高亮的条件
        HighlightBuilder highlightBuilder = new HighlightBuilder();
        highlightBuilder.field("title");
        highlightBuilder.preTags("<span style='color:red'>");
        highlightBuilder.postTags("</span>");

        searchSourceBuilder.highlighter(highlightBuilder);

        //term查询
        searchSourceBuilder.query(QueryBuilders.termQuery("title","手机"));

        //数据过滤
        //searchSourceBuilder.postFilter(QueryBuilders.termQuery("brand", "锤子"));
        //字段过滤
        //searchSourceBuilder.fetchSource(new String[]{"id","title"},null); //包含
        //searchSourceBuilder.fetchSource(null,new String[]{"id","title"}); //不包含

        //通配符查询
        //searchSourceBuilder.query(QueryBuilders.wildcardQuery("title","*米*"));
        //模糊查询（容错）
        //searchSourceBuilder.query(QueryBuilders.fuzzyQuery("brand","化为").fuzziness(Fuzziness.ONE));
        //区间查询
        //searchSourceBuilder.query(QueryBuilders.rangeQuery("price").gte(3500.0).lte(5000.0));
        //分词查询
        //searchSourceBuilder.query(QueryBuilders.matchQuery("title","华为手机"));
        //组合查询
        /*searchSourceBuilder.query(QueryBuilders.boolQuery()
                .must(QueryBuilders.matchQuery("title","华为手机"))
                .mustNot(QueryBuilders.rangeQuery("price").gte(3000.0).lte(4000.0)));*/
        searchRequest.source(searchSourceBuilder);
        //执行查询
        SearchResponse searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);

        //获取聚合的结果
        Aggregations aggregations = searchResponse.getAggregations();
        Terms terms = aggregations.get("brandCount");
        List<? extends Terms.Bucket> buckets = terms.getBuckets();
        buckets.forEach(bucket->{
            System.out.println(bucket.getKeyAsString() + ":" + bucket.getDocCount());
        });

        SearchHits responseHits = searchResponse.getHits();
        System.out.println("总记录数是:" + responseHits.getTotalHits());

        SearchHit[] searchHits = responseHits.getHits();
        for (SearchHit searchHit : searchHits) {
            //把json字符串转成对象
            String jsonString = searchHit.getSourceAsString();
            //fastjson
            //Item item = JSON.parseObject(jsonString, Item.class);
            //gson
            Item item = gson.fromJson(jsonString, Item.class);

            //获取高亮的结果
            Map<String, HighlightField> highlightFields = searchHit.getHighlightFields();
            HighlightField highlightField = highlightFields.get("title");
            Text[] fragments = highlightField.getFragments();
            if (fragments != null && fragments.length > 0){
                String title = fragments[0].toString();
                item.setTitle(title);
            }

            System.out.println(item);
        }
    }

    @After
    public void end() throws Exception{
        client.close();
    }
}
