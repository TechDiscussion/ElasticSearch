package ElasticSearch;

import org.apache.http.HttpHost;
import org.apache.http.HttpRequestInterceptor;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import com.amazonaws.auth.AWS4Signer;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.http.AWSRequestSigningApacheInterceptor;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;


public class  Application {
    private static String serviceName = "es";
    private static String region = "us-east-2";
    private static String aesEndpoint = "https://search-techvault-es-dev-i4buxwzwtdyasilq6bwgryidva.us-east-2.es.amazonaws.com";
    private static String index = "test_4";
    private static String type = "blog";
    private static String companyKey = "company";

    static final AWSCredentialsProvider credentialsProvider = new DefaultAWSCredentialsProviderChain();

    public static void main(String[] args) throws IOException {
        System.out.println("Welcome to ES - 1");
        ArrayList<Map<String, Object>> documents = ReadBlogDataFromJson();
        BulkResponse response = CreateBulkPostRequest(documents);
        DecodeBulkResponse(response);
        System.out.println("Exiting the program");
        System.exit(0);
    }

    // Adds the interceptor to the ES REST client
    public static RestHighLevelClient esClient(String serviceName, String region) {
        AWS4Signer signer = new AWS4Signer();
        signer.setServiceName(serviceName);
        signer.setRegionName(region);
        HttpRequestInterceptor interceptor = new AWSRequestSigningApacheInterceptor(serviceName, signer, credentialsProvider);
        return new RestHighLevelClient(RestClient.builder(HttpHost.create(aesEndpoint)).setHttpClientConfigCallback(hacb -> hacb.addInterceptorLast(interceptor)));
    }

    // Decodes BulkResponse.
    public static void DecodeBulkResponse(BulkResponse bulkResponse) {
        for (BulkItemResponse bulkItemResponse : bulkResponse) {
            DocWriteResponse itemResponse = bulkItemResponse.getResponse();
            switch (bulkItemResponse.getOpType()) {
                case CREATE:
                case INDEX:
                    IndexResponse indexResponse = (IndexResponse) itemResponse;
                    System.out.println(indexResponse);
                    break;
                case UPDATE:
                case DELETE:
            }
        }
    }

    // Creates a bulk post request
    public static BulkResponse CreateBulkPostRequest(ArrayList<Map<String, Object>> documents) throws IOException {
        BulkRequest bulk_request = new BulkRequest();
        int intId = 0;
        for (Map<String, Object> document : documents) {
            String strId = String.valueOf(intId++);
            IndexRequest request = new IndexRequest(index, type, strId).source(document);
            bulk_request.add(request);
        }
        RestHighLevelClient esClient = esClient(serviceName, region);
        BulkResponse response = esClient.bulk(bulk_request, RequestOptions.DEFAULT);
        return response;
    }

    // Read blog data from JSON (New).
    public static ArrayList<Map<String, Object>> ReadBlogDataFromJson() throws IOException {
        JSONParser jsonParser = new JSONParser();
        ArrayList<Map<String, Object>> all_pages = new ArrayList<Map<String, Object>>();
        try {
            JSONObject jsonObject = (JSONObject) jsonParser.parse(new FileReader("/Users/satyakesav/Documents/TechVault/data/blogs.json"));
            ArrayList pages = (ArrayList) jsonObject.get("data");
            for (Object page : pages) {
                HashMap<String, Object> page_map = (HashMap<String, Object>) page;
                all_pages.add(page_map);
            }
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (ParseException e) {
            e.printStackTrace();
        }
        return all_pages;
    }

    // Reads the blog data
    public static ArrayList<Map<String, Object>> ReadLocalJsonData() throws IOException {
        JSONParser jsonParser = new JSONParser();
        ArrayList<Map<String, Object>> all_pages = new ArrayList<Map<String, Object>>();
        try {
            JSONObject jsonObject = (JSONObject) jsonParser.parse(new FileReader("/Users/satyakesav/Documents/TechVault/data/blogs.json"));
            for (Object keyObject : jsonObject.keySet()) {
                // Company name
                String company = (String) keyObject;
                // List of company blog pages
                ArrayList pages = (ArrayList) jsonObject.get(keyObject);

                for (Object page : pages) {
                    HashMap<String, Object> page_details = (HashMap<String, Object>) page;
                    Map<String, Object> document = new HashMap<>();
                    document.put(companyKey, keyObject);
                    for (Map.Entry page_detail : page_details.entrySet()) {
                        String key = (String) page_detail.getKey();
                        Object value = (String) page_detail.getValue();
                        document.put(key, value);
                    }
                    all_pages.add(document);
                }
            }
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (ParseException e) {
            e.printStackTrace();
        }
        return all_pages;
    }
}