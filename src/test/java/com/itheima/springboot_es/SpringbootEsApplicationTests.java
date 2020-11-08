package com.itheima.springboot_es;


import com.alibaba.fastjson.JSON;
import com.itheima.springboot_es.domain.Goods;
import com.itheima.springboot_es.mapper.GoodsMapper;
import org.apache.lucene.index.Term;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.client.IndicesClient;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.indices.CreateIndexRequest;
import org.elasticsearch.client.indices.CreateIndexResponse;

import org.elasticsearch.common.text.Text;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.*;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.aggregations.Aggregation;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.Aggregations;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightBuilder;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightField;
import org.elasticsearch.search.sort.SortOrder;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;


import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@SpringBootTest
class SpringbootEsApplicationTests {
	@Autowired
	private RestHighLevelClient client;
	@Autowired
	private GoodsMapper goodsMapper;
	@Test
	public void getElasticSearch() {
		System.out.println(client);
	}
	@Test
	public void testIndices() throws IOException {
		//使用client对象获取操作索引对象
		IndicesClient indices = client.indices();
		//具体操作获取返回值
		//设置索引名称
		CreateIndexRequest createIndexRequest=new CreateIndexRequest("nihao");

		CreateIndexResponse createIndexResponse = indices.create(createIndexRequest, RequestOptions.DEFAULT);
		//3.根据返回值判断结果
		System.out.println(createIndexResponse);
		//CreateIndexResponse createIndexResponse = indices.create(createIndexRequest, RequestOptions.DEFAULT);
		//3.根据返回值判断结果
		System.out.println(createIndexResponse.isAcknowledged());

	}
	@Test
	public void testIndicesAndMapping() throws IOException {
		//使用client对象获取操作索引对象
		IndicesClient indices = client.indices();
		//具体操作获取返回值
		//设置索引名称
		CreateIndexRequest createIndexRequest=new CreateIndexRequest("person4");

		//CreateIndexResponse createIndexResponse = indices.create(createIndexRequest, RequestOptions.DEFAULT);
		//3.根据返回值判断结
		String mapping = "{\n" +
				"      \"properties\" : {\n" +
				"        \"age\" : {\n" +
				"          \"type\" : \"integer\"\n" +
				"        },\n" +
				"        \"name\" : {\n" +
				"          \"type\" : \"text\"\n" +
				"        }\n" +
				"      }\n" +
				"    }";
		createIndexRequest.mapping(mapping, XContentType.JSON);
		CreateIndexResponse createIndexResponse = indices.create(createIndexRequest, RequestOptions.DEFAULT);
		//3.根据返回值判断结果
		System.out.println(createIndexResponse.isAcknowledged());

	}

	/**
	 * 1.批量操作
	 */
	@Test
	public void testBulk() throws IOException {
		//创建 bulkrequest 对象,整合所有操作
		BulkRequest bulkRequest = new BulkRequest();
		/**
		 *  1. 删除8号记录
		 *  2. 添加6号记录
		 *  3. 修改3号记录 名称为 “三号”
		 */
		//删除8号记录
		DeleteRequest deleteRequest = new DeleteRequest("person2", "8");
		bulkRequest.add(deleteRequest);

		//添加6号记录
		Map map1 = new HashMap();
		map1.put("name","六号");
		map1.put("age",26);
		map1.put("address","陕西西安");
		IndexRequest indexRequest = new IndexRequest("person2").id("9").source(map1);
		bulkRequest.add(indexRequest);

		//修改3号记录 名称为 “三号”
		Map map2 = new HashMap();
		map2.put("name","老余");
		UpdateRequest updateRequest = new UpdateRequest("person2", "6").doc(map2);
		bulkRequest.add(updateRequest);
		//执行批量操作
		BulkResponse responses = client.bulk(bulkRequest, RequestOptions.DEFAULT);
		RestStatus status = responses.status();
		System.out.println(status);

	}

	/**
	 * 批量导入
	 */
	@Test
	public void testImportData() throws IOException {
		//查询数据库的所有信息
		List<Goods> goodsList = goodsMapper.findAll();
		//Bulk导入
		BulkRequest bulkRequest = new BulkRequest();
		for (Goods goods : goodsList) {
			//获取specStr字符串
			String specStr = goods.getSpecStr();
			//将json字符串转换为map集合
			Map map = JSON.parseObject(specStr, Map.class);
			//设置spec map
			goods.setSpec(map);
			//将goods对象转化为字符串
			String data = JSON.toJSONString(goods);
			IndexRequest indexRequest = new IndexRequest("goods");
			indexRequest.id(goods.getId()+"").source(data,XContentType.JSON);
			bulkRequest.add(indexRequest);
		}
		BulkResponse bulk = client.bulk(bulkRequest, RequestOptions.DEFAULT);
		System.out.println(bulk.status());
	}
	/**
	 *查询所有
	 * 1.matchAll
	 * 2.将查询结果封装为goods对象,装载到list集合中
	 * 3.分页.默认显示10条
	 */
	@Test
	public void testMatchAll() throws IOException {
		//2.创建查询请求对象,指定查询结果的索引名称
		SearchRequest searchRequest = new SearchRequest("goods");
		//4.创建查询条件构造器 SearchSourceBuilder
		SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
		//6.查询条件
		//查询所有文档
		QueryBuilder query = QueryBuilders.matchAllQuery();
		//5.指定查询条件
		searchSourceBuilder.query(query);
		//3.添加查询条件构建器 SearchSourceBuilder
		searchRequest.source(searchSourceBuilder);
		//8.添加分页信息
		searchSourceBuilder.from(0);
		searchSourceBuilder.size(100);
		//1.查询,获取结果
		SearchResponse search = client.search(searchRequest, RequestOptions.DEFAULT);
		//7.获取名中对象
		SearchHits hits = search.getHits();
		//获取总记录条数
		long value = hits.getTotalHits().value;
		System.out.println("总记录:"+value);
		getResult(hits);
	}

	private void getResult(SearchHits hits) {
		ArrayList<Goods> goodsArrayList = new ArrayList<>();
		SearchHit[] hitsHits = hits.getHits();
		for (SearchHit hitsHit : hitsHits) {
			//获取json字符串
			String sourceAsString = hitsHit.getSourceAsString();
			Goods goods = JSON.parseObject(sourceAsString, Goods.class);
			goodsArrayList.add(goods);
		}
		for (Goods goods : goodsArrayList) {
			System.out.println(goods);
		}
	}

	/**
	 * term查询:词条不分词查询
	 * @throws IOException
	 */
	@Test
	public void testTermQuery() throws IOException {
		//2.创建查询请求对象,指定查询结果的索引名称
		SearchRequest searchRequest = new SearchRequest("goods");
		//4.创建查询条件构造器 SearchSourceBuilder
		SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
		//6.查询条件
		//查询所有文档
		QueryBuilder query = QueryBuilders.termQuery("title","华为");//term词条查询
		//5.指定查询条件
		searchSourceBuilder.query(query);
		//3.添加查询条件构建器 SearchSourceBuilder
		searchRequest.source(searchSourceBuilder);
		//8.添加分页信息
		searchSourceBuilder.from(0);
		searchSourceBuilder.size(100);
		//1.查询,获取结果
		SearchResponse search = client.search(searchRequest, RequestOptions.DEFAULT);
		//7.获取名中对象
		SearchHits hits = search.getHits();
		//获取总记录条数
		long value = hits.getTotalHits().value;
		System.out.println("总记录:"+value);
		getResult(hits);
	}
	/**
	 * matchQuery:词条分词查询
	 */
	@Test
	public void testMatchQuery() throws IOException {
		//2.创建查询请求对象,指定查询结果的索引名称
		SearchRequest searchRequest = new SearchRequest("goods");
		//4.创建查询条件构造器 SearchSourceBuilder
		SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
		//6.查询条件
		//查询所有文档
		MatchQueryBuilder query = QueryBuilders.matchQuery("title","华为手机");
		query.operator(Operator.AND);
		//5.指定查询条件
		searchSourceBuilder.query(query);
		//3.添加查询条件构建器 SearchSourceBuilder
		searchRequest.source(searchSourceBuilder);
		//8.添加分页信息
		searchSourceBuilder.from(0);
		searchSourceBuilder.size(100);
		//1.查询,获取结果
		SearchResponse search = client.search(searchRequest, RequestOptions.DEFAULT);
		//7.获取名中对象
		SearchHits hits = search.getHits();
		//获取总记录条数
		long value = hits.getTotalHits().value;
		System.out.println("总记录:"+value);
		getResult(hits);
	}
	/**
	 * 模糊查询
	 */
	//wildcard:模糊查询
	@Test
	public void testWildcard() throws IOException {
		//2.创建查询请求对象,指定查询结果的索引名称
		SearchRequest searchRequest = new SearchRequest("goods");
		//4.创建查询条件构造器 SearchSourceBuilder
		SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
		//6.查询条件
		//查询所有文档
		WildcardQueryBuilder query = QueryBuilders.wildcardQuery("title","华*");
		//5.指定查询条件
		searchSourceBuilder.query(query);
		//3.添加查询条件构建器 SearchSourceBuilder
		searchRequest.source(searchSourceBuilder);
		//8.添加分页信息
		searchSourceBuilder.from(0);
		searchSourceBuilder.size(100);
		//1.查询,获取结果
		SearchResponse search = client.search(searchRequest, RequestOptions.DEFAULT);
		//7.获取名中对象
		SearchHits hits = search.getHits();
		//获取总记录条数
		long value = hits.getTotalHits().value;
		System.out.println("总记录:"+value);
		getResult(hits);
	}
	//regexpQuery:正则查询
	@Test
	public void testRegexpQuery() throws IOException {
		//2.创建查询请求对象,指定查询结果的索引名称
		SearchRequest searchRequest = new SearchRequest("goods");
		//4.创建查询条件构造器 SearchSourceBuilder
		SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
		//6.查询条件
		//查询所有文档
		RegexpQueryBuilder query = QueryBuilders.regexpQuery("title", "\\w+(.)*");
		//5.指定查询条件
		searchSourceBuilder.query(query);
		//3.添加查询条件构建器 SearchSourceBuilder
		searchRequest.source(searchSourceBuilder);
		//8.添加分页信息
		searchSourceBuilder.from(0);
		searchSourceBuilder.size(100);
		//1.查询,获取结果
		SearchResponse search = client.search(searchRequest, RequestOptions.DEFAULT);
		//7.获取名中对象
		SearchHits hits = search.getHits();
		//获取总记录条数
		long value = hits.getTotalHits().value;
		System.out.println("总记录:" + value);
		getResult(hits);
	}
	//prefix:前缀查询
	@Test
	public void testPrefixQuery() throws IOException {
		//2.创建查询请求对象,指定查询结果的索引名称
		SearchRequest searchRequest = new SearchRequest("goods");
		//4.创建查询条件构造器 SearchSourceBuilder
		SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
		//6.查询条件
		//查询所有文档
		PrefixQueryBuilder query = QueryBuilders.prefixQuery("brandName","三");
		//5.指定查询条件
		searchSourceBuilder.query(query);
		//3.添加查询条件构建器 SearchSourceBuilder
		searchRequest.source(searchSourceBuilder);
		//8.添加分页信息
		searchSourceBuilder.from(0);
		searchSourceBuilder.size(100);
		//1.查询,获取结果
		SearchResponse search = client.search(searchRequest, RequestOptions.DEFAULT);
		//7.获取名中对象
		SearchHits hits = search.getHits();
		//获取总记录条数
		long value = hits.getTotalHits().value;
		System.out.println("总记录:" + value);
		getResult(hits);
	}

	//范围查询
	@Test
	public void testRangeQuery() throws IOException {
		//2.创建查询请求对象,指定查询结果的索引名称
		SearchRequest searchRequest = new SearchRequest("goods");
		//4.创建查询条件构造器 SearchSourceBuilder
		SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
		//6.查询条件
		//查询所有文档
		RangeQueryBuilder query = new RangeQueryBuilder("price");
		query.gte(2000);
		query.lte(3000);
		//5.指定查询条件
		searchSourceBuilder.query(query);
		searchSourceBuilder.sort("price", SortOrder.DESC);
		//3.添加查询条件构建器 SearchSourceBuilder
		searchRequest.source(searchSourceBuilder);
		//8.添加分页信息
		searchSourceBuilder.from(0);
		searchSourceBuilder.size(100);
		//1.查询,获取结果
		SearchResponse search = client.search(searchRequest, RequestOptions.DEFAULT);
		//7.获取名中对象
		SearchHits hits = search.getHits();
		//获取总记录条数
		long value = hits.getTotalHits().value;
		System.out.println("总记录:" + value);
		getResult(hits);
	}

	//多条件查询
	@Test
	public void testQueryStringQuery() throws IOException {
		//2.创建查询请求对象,指定查询结果的索引名称
		SearchRequest searchRequest = new SearchRequest("goods");
		//4.创建查询条件构造器 SearchSourceBuilder
		SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
		//6.查询条件
		//查询所有文档
		QueryStringQueryBuilder query = QueryBuilders.queryStringQuery("华为手机").field("title").field("categoryName").field("brandName").defaultOperator(Operator.AND);
		//5.指定查询条件
		searchSourceBuilder.query(query);
		//3.添加查询条件构建器 SearchSourceBuilder
		searchRequest.source(searchSourceBuilder);
		//8.添加分页信息
		searchSourceBuilder.from(0);
		searchSourceBuilder.size(100);
		//1.查询,获取结果
		SearchResponse search = client.search(searchRequest, RequestOptions.DEFAULT);
		//7.获取名中对象
		SearchHits hits = search.getHits();
		//获取总记录条数
		long value = hits.getTotalHits().value;
		System.out.println("总记录:" + value);
		getResult(hits);
	}

	/**
	 * 布尔查询：boolQuery
	 * 1. 查询品牌名称为:华为
	 * 2. 查询标题包含：手机
	 * 3. 查询价格在：2000-3000
	 */
	@Test
	public void testBoolQuery() throws IOException {
		//2.创建查询请求对象,指定查询结果的索引名称
		SearchRequest searchRequest = new SearchRequest("goods");
		//4.创建查询条件构造器 SearchSourceBuilder
		SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
		//6.查询条件
		//查询所有文档
		BoolQueryBuilder query = QueryBuilders.boolQuery();
		TermQueryBuilder termQuery = QueryBuilders.termQuery("brandName", "华为");
		query.must(termQuery);
		MatchQueryBuilder matchQuery = QueryBuilders.matchQuery("title", "手机");
		query.filter(matchQuery);
		RangeQueryBuilder rangeQuery = QueryBuilders.rangeQuery("price");
		rangeQuery.gte(2000);
		rangeQuery.lte(3000);
		query.filter(rangeQuery);
		//5.指定查询条件
		searchSourceBuilder.query(query);
		//3.添加查询条件构建器 SearchSourceBuilder
		searchRequest.source(searchSourceBuilder);
		//8.添加分页信息
		searchSourceBuilder.from(0);
		searchSourceBuilder.size(100);
		//1.查询,获取结果
		SearchResponse search = client.search(searchRequest, RequestOptions.DEFAULT);
		//7.获取名中对象
		SearchHits hits = search.getHits();
		//获取总记录条数
		long value = hits.getTotalHits().value;
		System.out.println("总记录:" + value);
		getResult(hits);
	}

	//聚合查询
	@Test
	public void testAggQuery() throws IOException {
		//2.创建查询请求对象,指定查询结果的索引名称
		SearchRequest searchRequest = new SearchRequest("goods");
		//4.创建查询条件构造器 SearchSourceBuilder
		SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
		//6.查询条件
		//查询所有文档
		//1.1. 查询title包含手机的数据
		//MatchQueryBuilder query = QueryBuilders.matchQuery("title", "手机");
		//searchSourceBuilder.query(query);
		//1.2. 查询品牌列表
		/**
		 * 参数:
		 * 1.自定义的名称,将来用于获取数据
		 * 2.分组字段
		 */
		AggregationBuilder agg = AggregationBuilders.terms("goods_brands").field("brandName");
		searchSourceBuilder.aggregation(agg);
		//3.添加查询条件构建器 SearchSourceBuilder
		searchRequest.source(searchSourceBuilder);
		//8.添加分页信息
		searchSourceBuilder.from(0);
		searchSourceBuilder.size(100);
		//1.查询,获取结果
		SearchResponse search = client.search(searchRequest, RequestOptions.DEFAULT);
		//7.获取名中对象
		SearchHits hits = search.getHits();
		//获取总记录条数
		long value = hits.getTotalHits().value;
		System.out.println("总记录:" + value);
		ArrayList<Goods> goodsArrayList = new ArrayList<>();
		SearchHit[] hitsHits = hits.getHits();
		Aggregations aggregations = search.getAggregations();
		Map<String, Aggregation> aggregationMap = aggregations.asMap();
		Terms brands = (Terms) aggregationMap.get("goods_brands");
		List<? extends Terms.Bucket> buckets = brands.getBuckets();
		List list = new ArrayList();
		for (Terms.Bucket bucket : buckets) {
			Object key = bucket.getKey();
			list.add(key);
		}
		for (Object o : list) {
			System.out.println(o);
		}
	}

	/**
	 * 高亮查询
	 * 1.设置高亮
	 *   高亮字段
	 *   前缀
	 *   后缀
	 * 2.将高亮了的字段数据,替换原有的数据
	 * @throws IOException
	 */
	@Test
	public void testHighLightQuery() throws IOException {
		//2.创建查询请求对象,指定查询结果的索引名称
		SearchRequest searchRequest = new SearchRequest("goods");
		//4.创建查询条件构造器 SearchSourceBuilder
		SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
		//6.查询条件
		//查询title包含手机的数据
		QueryBuilder query = QueryBuilders.matchQuery("title","华为手机");
		//5.指定查询条件
		searchSourceBuilder.query(query);
		//设置高亮
		HighlightBuilder highlightBuilder = new HighlightBuilder();
		//设置高亮字段
		highlightBuilder.field("title");
		highlightBuilder.preTags("<font color='red'>");
		highlightBuilder.postTags("</font>");

		searchSourceBuilder.highlighter(highlightBuilder);
		//3.添加查询条件构建器 SearchSourceBuilder
		searchRequest.source(searchSourceBuilder);
		//8.添加分页信息
		searchSourceBuilder.from(0);
		searchSourceBuilder.size(100);
		//1.查询,获取结果
		SearchResponse search = client.search(searchRequest, RequestOptions.DEFAULT);
		//7.获取名中对象
		SearchHits hits = search.getHits();
		//获取总记录条数
		long value = hits.getTotalHits().value;
		System.out.println("总记录:" + value);
		ArrayList<Goods> goodsArrayList = new ArrayList<>();
		SearchHit[] hitsHits = hits.getHits();
		for (SearchHit hitsHit : hitsHits) {
			//获取json字符串
			String sourceAsString = hitsHit.getSourceAsString();
			//转化为java对象
			Goods goods = JSON.parseObject(sourceAsString, Goods.class);
			//获取高亮结果,替换goods中的title
			Map<String, HighlightField> highlightFields = hitsHit.getHighlightFields();
			HighlightField highlightField = highlightFields.get("title");
			Text[] fragments = highlightField.fragments();
			//替换
			goods.setTitle(fragments[0].toString());
			goodsArrayList.add(goods);
		}
		for (Goods goods : goodsArrayList) {
			System.out.println(goods);
		}
	}
}
