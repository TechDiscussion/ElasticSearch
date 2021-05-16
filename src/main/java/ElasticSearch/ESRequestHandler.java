package ElasticSearch;

import com.amazonaws.auth.AWS4Signer;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import com.amazonaws.http.AWSRequestSigningApacheInterceptor;
import org.apache.http.HttpHost;
import org.apache.http.HttpRequestInterceptor;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Map;

public class ESRequestHandler {
    // Default Elastic Search cluster to use.
    private static String AES_END_POINT = "https://search-techvault-es-dev-i4buxwzwtdyasilq6bwgryidva.us-east-2.es.amazonaws.com";
    private static String SERVICE_NAME = "es";
    private static String REGION = "us-east-2";

    private static RestHighLevelClient esClient;
    private static final AWSCredentialsProvider credentialsProvider = new DefaultAWSCredentialsProviderChain();

    // Creates a RestHighLevelClient for a given endpoint, service name and region.
    private static RestHighLevelClient esClient(String aesEndpoint, String serviceName, String region) {
        AWS4Signer signer = new AWS4Signer();
        signer.setServiceName(serviceName);
        signer.setRegionName(region);
        HttpRequestInterceptor interceptor = new AWSRequestSigningApacheInterceptor(serviceName, signer, credentialsProvider);
        return new RestHighLevelClient(RestClient.builder(HttpHost.create(aesEndpoint)).setHttpClientConfigCallback(hacb -> hacb.addInterceptorLast(interceptor)));
    }

    // Default initializer
    public ESRequestHandler() {
        esClient = esClient(AES_END_POINT, SERVICE_NAME, REGION);
    }

    // Custom initializer.
    public ESRequestHandler(String aesEndpoint, String serviceName, String region) {
        esClient = esClient(aesEndpoint, serviceName, region);
    }

    // Creates a bulk post request
    public static void CreateBulkPostRequest(ArrayList<Map<String, Object>> documents, String index, String type) throws IOException {
        BulkRequest bulk_request = new BulkRequest();
        int intId = 0;
        for (Map<String, Object> document : documents) {
            String strId = String.valueOf(intId++);
            IndexRequest request = new IndexRequest(index, type, strId).source(document);
            bulk_request.add(request);
        }
        DecodeBulkResponseForPost(esClient.bulk(bulk_request, RequestOptions.DEFAULT));
    }

    // Decodes BulkResponse.
    public static void DecodeBulkResponseForPost(BulkResponse bulkResponse) {
        int totalCreates = 0;
        int totalUpdates = 0;
        int totalRequests = 0;
        for (BulkItemResponse bulkItemResponse : bulkResponse) {
            totalRequests += 1;
            DocWriteResponse itemResponse = bulkItemResponse.getResponse();
            if (itemResponse != null) {
                if (itemResponse.getResult() == DocWriteResponse.Result.CREATED) {
                    totalCreates += 1;
                } else if (itemResponse.getResult() == DocWriteResponse.Result.UPDATED) {
                    totalUpdates += 1;
                }
            }
        }
        System.out.format("Total documents created : %d %n", totalCreates);
        System.out.format("Total documents updated : %d %n", totalUpdates);
        System.out.format("Total documents requested : %d %n", totalRequests);
    }
}
