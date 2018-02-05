package com.nebo.elasticsearch;
import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Date;
import java.util.List;

import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.junit.Before;
import org.junit.Test;

public class TransportClientTest {
	 
   private  TransportClient client;
    @Before
 	public  void test_1() throws UnknownHostException {
 		
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
		}
 
 		 
 	}
    
    @Test
    public void test_2() throws IOException {
    	
//    	System.out.println("kaishi1");
//    	IndexResponse response = client.prepareIndex("twitter", "tweet", "3")
//    	        .setSource( jsonBuilder().startObject()
//    	                        .field("user", "kimchy")
//    	                        .field("postDate", new Date())
//    	                        .field("message", "trying out Elasticsearch")
//    	                    .endObject()
//    	                  )
//    	        .get();
//    	System.out.println("=创建索引==");
//    	client.close();
    	
    	IndexResponse response = client
				.prepareIndex("twitter", "tweet", "4")
				.setSource(
						jsonBuilder().startObject().field("user", "kimchy")
								.field("postDate", new Date())
								.field("message", "trying out Elasticsearch")
								.endObject()).get();
		System.out.println(response.getId());
		client.close();	
//    	IndexResponse response = client
//				.prepareIndex("twitter", "tweet", "1")
//				.setSource(
//						jsonBuilder().startObject().field("user", "kimchy")
//								.field("postDate", new Date())
//								.field("message", "trying out Elasticsearch")
//								.endObject()).get();
//		System.out.println(response.getId());
//		client.close();
    }
    
    @Test
	public void test3() throws IOException {
		String json = "{" + "\"user\":\"kimchy\","
				+ "\"postDate\":\"2013-01-30\","
				+ "\"message\":\"trying out Elasticsearch\"" + "}";
		IndexResponse response = client.prepareIndex("twitter", "tweet")
				.setSource(json).get();
		System.out.println(response.getId());
		client.close();
	}

	 

}
