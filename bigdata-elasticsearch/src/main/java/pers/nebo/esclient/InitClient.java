package pers.nebo.esclient;
import org.apache.http.HttpHost;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
public class InitClient {

    public static RestHighLevelClient getClient(){
        RestHighLevelClient client = new RestHighLevelClient(
                RestClient.builder(
                        new HttpHost("xxxx", 9200, "http")));
        return client;
    }
}