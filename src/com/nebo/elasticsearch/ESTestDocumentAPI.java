package com.nebo.elasticsearch;

 

import java.io.IOException;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import static org.elasticsearch.node.NodeBuilder.*;
import static org.elasticsearch.common.xcontent.XContentFactory.*;
import org.elasticsearch.action.bulk.BackoffPolicy;
import org.elasticsearch.action.bulk.BulkProcessor;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;
import org.codehaus.jackson.map.ObjectMapper;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteRequestBuilder;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.get.MultiGetItemResponse;
import org.elasticsearch.action.get.MultiGetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.node.Node;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.search.SearchHits;
import org.junit.Before;
import org.junit.Test;



/**
 * Document API 操作
 * 
 * @author 大讲台
 * 
 */
public class ESTestDocumentAPI {
	private TransportClient client;

	@Before
	public void test0() throws UnknownHostException {

		 Settings settings = Settings.settingsBuilder()
				//.put("client.transport.sniff", true)
				.put("cluster.name", "escluster").build();		 
		  client = TransportClient
				.builder()
				.settings(settings)
				.build()
				.addTransportAddress(
						new InetSocketTransportAddress(InetAddress
								.getByName("nebo1"), 9300));
		System.out.println("===");
		List<DiscoveryNode> connectedNodes = client.connectedNodes();
		System.out.println(connectedNodes.size());
		for (DiscoveryNode discoveryNode : connectedNodes)
		{
			System.out.println("集群节点："+discoveryNode.getHostName());
		};
	}

	/**
	 * 创建索引：use ElasticSearch helpers
	 * 
	 * @throws IOException
	 */
	@Test
	public void test1() throws IOException {
//		IndexResponse response = client
//				.prepareIndex("twitter", "tweet", "2")
//				.setSource(
//						jsonBuilder().startObject().field("user", "kimchy")
//								.field("postDate", new Date())
//								.field("message", "trying out Elasticsearch")
//								.endObject()).get();
//		System.out.println(response.getId());
//		client.close();
		
    	IndexResponse response = client.prepareIndex("twitter", "tweet", "3")
        .setSource( jsonBuilder().startObject()
                        .field("user", "kimchy")
                        .field("postDate", new Date())
                        .field("message", "trying out Elasticsearch")
                    .endObject()
                  )
        .get();
System.out.println("=创建索引==");
client.close();
	}

	/**
	 * 创建索引：do it yourself
	 * 
	 * @throws IOException
	 */
	@Test
	public void test2() throws IOException {
		String json = "{" + "\"user\":\"kimchy\","
				+ "\"postDate\":\"2013-01-30\","
				+ "\"message\":\"trying out Elasticsearch\"" + "}";
		IndexResponse response = client.prepareIndex("twitter", "tweet")
				.setSource(json).get();
		System.out.println(response.getId());
		client.close();
	}

	/**
	 * 创建索引：use map
	 * 
	 * @throws IOException
	 */
	@Test
	public void test3() throws IOException {
		Map<String, Object> json = new HashMap<String, Object>();
		json.put("user", "kimchy");
		json.put("postDate", new Date());
		json.put("message", "trying out Elasticsearch");

		IndexResponse response = client.prepareIndex("twitter", "tweet")
				.setSource(json).get();
		System.out.println(response.getId());
		client.close();
	}

	/**
	 * 创建索引：serialize your beans
	 * 
	 * @throws IOException
	 */
	@Test
	public void test4() throws IOException {
//		User user = new User();
//		user.setUser("kimchy");
//		user.setPostDate(new Date());
//		user.setMessage("trying out Elasticsearch");
//
//		// instance a json mapper
//		ObjectMapper mapper = new ObjectMapper(); // create once, reuse
//
//		// generate json
//		byte[] json = mapper.writeValueAsBytes(user);
//
//		IndexResponse response = client.prepareIndex("twitter", "tweet")
//				.setSource(json).get();
//		System.out.println(response.getId());
//		client.close();
	}

	/**
	 * 查询索引：get
	 * 
	 * @throws IOException
	 */
	@Test
	public void test5() throws IOException {
		GetResponse response = client.prepareGet("twitter", "tweet", "1").get();
		System.out.println(response.getSourceAsString());

		client.close();
	}

	/**
	 * 删除索引：delete
	 * 
	 * @throws IOException
	 */
	@Test
	public void test6() throws IOException {
		client.prepareDelete("twitter", "tweet", "1").get();
		client.close();
	}

	/**
	 * 更新索引：Update API-UpdateRequest
	 * 
	 * @throws IOException
	 * @throws ExecutionException
	 * @throws InterruptedException
	 */
	@Test
	public void test7() throws IOException, InterruptedException,
			ExecutionException {
		UpdateRequest updateRequest = new UpdateRequest();
		updateRequest.index("twitter");
		updateRequest.type("tweet");
		updateRequest.id("AVnJTVfSc9XhQxkiDeIK");
		updateRequest.doc(jsonBuilder().startObject().field("gender", "male")
				.endObject());
		client.update(updateRequest).get();
		System.out.println(updateRequest.version());
		client.close();
	}

	/**
	 * 更新索引：Update API-prepareUpdate()-doc
	 * 
	 * @throws IOException
	 * @throws ExecutionException
	 * @throws InterruptedException
	 */
	@Test
	public void test8() throws IOException, InterruptedException,
			ExecutionException {
		client.prepareUpdate("twitter", "tweet", "AVnJTqaLc9XhQxkiDeIT")
				.setDoc(jsonBuilder().startObject().field("gender", "female")
						.endObject()).get();
		client.close();
	}

	/**
	 * 更新索引：Update API-prepareUpdate()-script
	 * 需要开启：script.engine.groovy.inline.update: on
	 * 
	 * @throws IOException
	 * @throws ExecutionException
	 * @throws InterruptedException
	 */
	@Test
	public void test9() throws IOException, InterruptedException,
			ExecutionException {
		client.prepareUpdate("twitter", "tweet", "AVnJTHWXc9XhQxkiDeIE")
				.setScript(
						new Script("ctx._source.gender = \"female\"",
								ScriptService.ScriptType.INLINE, null, null))
				.get();
		client.close();
	}

	/**
	 * 更新索引：Update API-UpdateRequest-upsert
	 * 
	 * @throws IOException
	 * @throws ExecutionException
	 * @throws InterruptedException
	 */
	@Test
	public void test10() throws IOException, InterruptedException,
			ExecutionException {
		IndexRequest indexRequest = new IndexRequest("twitter", "tweet", "1")
										.source(jsonBuilder()
										.startObject()
										.field("name", "Joe Smith")
										.field("gender", "male")
										.endObject());
		UpdateRequest updateRequest = new UpdateRequest("twitter", "tweet", "1")
										.doc(jsonBuilder()
										.startObject()
										.field("gender", "female")
										.endObject()).upsert(indexRequest);
		client.update(updateRequest).get();
		client.close();
	}
	
	/**
	 * 批量查询索引：Multi Get API
	 * 
	 * @throws IOException
	 * @throws ExecutionException
	 * @throws InterruptedException
	 */
	@Test
	public void test11() throws IOException, InterruptedException,
			ExecutionException {
		MultiGetResponse multiGetItemResponses = client.prepareMultiGet()
			    .add("twitter", "tweet", "1")           
			    .add("twitter", "tweet", "AVnJTqaLc9XhQxkiDeIT", "AVnJTHWXc9XhQxkiDeIE", "AVnJTVfSc9XhQxkiDeIK") 
			    .add("djt2", "user", "1")          
			    .get();

			for (MultiGetItemResponse itemResponse : multiGetItemResponses) { 
			    GetResponse response = itemResponse.getResponse();
			    if (response.isExists()) {                      
			        String json = response.getSourceAsString(); 
			        System.out.println(json);
			    }
			}
		client.close();
	}
	
	/**
	 * 批量操作索引：Bulk API
	 * 
	 * @throws IOException
	 * @throws ExecutionException
	 * @throws InterruptedException
	 */
	@Test
	public void test12() throws IOException, InterruptedException,
			ExecutionException {
		BulkRequestBuilder bulkRequest = client.prepareBulk();

		// either use client#prepare, or use Requests# to directly build index/delete requests
		bulkRequest.add(client.prepareIndex("twitter", "tweet", "3")
		        .setSource(jsonBuilder()
		                    .startObject()
		                        .field("user", "kimchy")
		                        .field("postDate", new Date())
		                        .field("message", "trying out Elasticsearch")
		                    .endObject()
		                  )
		        );

		bulkRequest.add(client.prepareIndex("twitter", "tweet", "2")
		        .setSource(jsonBuilder()
		                    .startObject()
		                        .field("user", "kimchy")
		                        .field("postDate", new Date())
		                        .field("message", "another post")
		                    .endObject()
		                  )
		        );
		DeleteRequestBuilder prepareDelete = client.prepareDelete("twitter", "tweet", "AVnJTVfSc9XhQxkiDeIK");
		bulkRequest.add(prepareDelete);
		
		
		BulkResponse bulkResponse = bulkRequest.get();
		//批量操作：其中一个操作失败不影响其他操作成功执行
		if (bulkResponse.hasFailures()) {
		    // process failures by iterating through each bulk response item
			BulkItemResponse[] items = bulkResponse.getItems();
			for (BulkItemResponse bulkItemResponse : items) {
				System.out.println(bulkItemResponse.getFailureMessage());
			}
		}else{
			System.out.println("bulk process success!");
		}
		client.close();
	}
	
	/**
	 * 批量操作索引：Using Bulk Processor
	 * 优化：先关闭副本，再添加副本，提升效率
	 * @throws IOException
	 * @throws ExecutionException
	 * @throws InterruptedException
	 */
	@Test
	public void test13() throws IOException, InterruptedException,
			ExecutionException {
		
		BulkProcessor bulkProcessor = BulkProcessor.builder(
		        client,  
		        new BulkProcessor.Listener() {
					
					public void beforeBulk(long executionId, BulkRequest request) {
						// TODO Auto-generated method stub
						System.out.println(request.numberOfActions());
					}
					
					public void afterBulk(long executionId, BulkRequest request,
							Throwable failure) {
						// TODO Auto-generated method stub
						System.out.println(failure.getMessage());
					}
					
					public void afterBulk(long executionId, BulkRequest request,
							BulkResponse response) {
						// TODO Auto-generated method stub
						System.out.println(response.hasFailures());
					}
				})
		        .setBulkActions(1000) // 每个批次的最大数量
		        .setBulkSize(new ByteSizeValue(1, ByteSizeUnit.GB))// 每个批次的最大字节数
		        .setFlushInterval(TimeValue.timeValueSeconds(5))// 每批提交时间间隔
		        .setConcurrentRequests(1) //设置多少个并发处理线程
		        //可以允许用户自定义当一个或者多个bulk请求失败后,该执行如何操作
		        .setBackoffPolicy(
		            BackoffPolicy.exponentialBackoff(TimeValue.timeValueMillis(100), 3)) 
		        .build();
		String json = "{" +
		        "\"user\":\"kimchy\"," +
		        "\"postDate\":\"2013-01-30\"," +
		        "\"message\":\"trying out Elasticsearch\"" +
		    "}";
		
		for (int i = 0; i < 1000; i++) {
			bulkProcessor.add(new IndexRequest("djt6", "user").source(json));
		}
		//阻塞至所有的请求线程处理完毕后，断开连接资源
		bulkProcessor.awaitClose(3, TimeUnit.MINUTES);
		client.close();
	}
	/**
	 * SearchType使用方式
	 * @throws Exception
	 */
	@Test
	public void test14() throws Exception {
		SearchResponse response = client.prepareSearch("djt")  
		        .setTypes("user")  
		        //.setSearchType(SearchType.DFS_QUERY_THEN_FETCH) 
		        .setSearchType(SearchType.QUERY_AND_FETCH)
		        .execute()  
		        .actionGet();  
		SearchHits hits = response.getHits();
		System.out.println(hits.getTotalHits());
	}
}
