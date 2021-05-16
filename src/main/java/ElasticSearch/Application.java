package ElasticSearch;

import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import ElasticSearch.ESRequestHandler;

public class  Application {
    public static void main(String[] args) throws IOException {
        System.out.println("Welcome to Elastic Search");

        // Read the blog data from local file.
        ArrayList<Map<String, Object>> documents = ReadBlogDataFromJson();
        if (documents.isEmpty()) {
            System.out.println("Data was not read successfully");
            System.exit(0);
        }
        System.out.println("Data read successfully");

        // Create a ESRequestHandler.
        ESRequestHandler esRequestHandler = new ESRequestHandler();

        // Send a bulk create request.
        esRequestHandler.CreateBulkPostRequest(documents, "test-6", "blog");

        // End
        System.out.println("Good Bye!");
        System.exit(0);
    }

    // Reads blogs from json file.
    public static ArrayList<Map<String, Object>> ReadBlogDataFromJson() throws IOException {
        JSONParser jsonParser = new JSONParser();
        ArrayList<Map<String, Object>> allSamples = new ArrayList<Map<String, Object>>();
        try {
            JSONObject jsonObject = (JSONObject) jsonParser.parse(new FileReader("/Users/satyakesav/Documents/TechVault/data/blogs.json"));
            ArrayList pages = (ArrayList) jsonObject.get("data");
            for (Object page : pages) {
                HashMap<String, Object> pageDetailsMap = (HashMap<String, Object>) page;
                allSamples.add(pageDetailsMap);
            }
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (ParseException e) {
            e.printStackTrace();
        }
        return allSamples;
    }
}