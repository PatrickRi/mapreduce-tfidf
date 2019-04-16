import org.json.JSONObject;

public class JsonParser {
    public static void main(String[] args) {
        String value = "{\"reviewerID\": \"A2ZFFXGLJUHD76\", \"asin\": \"B00LYPUPZK\", \"reviewerName\": \"Dan Bernstein\",\n" +
                "\"helpful\": [0, 0], \"reviewText\": \"Fake!\", \"overall\": 1.0, \"summary\": \"One Star\",\n" +
                "\"unixReviewTime\": 1405900800, \"reviewTime\": \"07 21, 2014\", \"category\": \"Health_and_Personal_Care\"}";
        JSONObject jsonObject = new JSONObject(value);
        String reviewText = jsonObject.getString("reviewText");
        String docId = jsonObject.getString("asin");
        System.out.print(reviewText);
    }
}
