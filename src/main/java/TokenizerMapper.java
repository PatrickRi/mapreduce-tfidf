import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.json.JSONObject;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class TokenizerMapper extends Mapper<Object, Text, Text, DocIdFreq> {

    public static Pattern CAPTURE_WORDS = Pattern.compile("([\\p{L}0-9][\\p{L}0-9]*)", Pattern.UNICODE_CASE);

    /**
     * Stopwords
     * hdfs:///user/elmar/amazon-reviews/full/complete/reviewscombined.json
     * <p>
     * {"reviewerID": "A2ZFFXGLJUHD76", "asin": "B00LYPUPZK", "reviewerName": "Dan Bernstein",
     * "helpful": [0, 0], "reviewText": "Fake!", "overall": 1.0, "summary": "One Star",
     * "unixReviewTime": 1405900800, "reviewTime": "07 21, 2014", "category": "Health_and_Personal_Care"}
     * <p>
     * TF_B = f(d,f)
     * IDF_B2 = ln (N  / ft)
     * f_d,f ... Number of occurences of term t in d
     * N ... Number of documents
     * ft ... number of documents containing term t
     * <p>
     * result: Term, <DocId, Frequency>
     *
     * @param key
     * @param value
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        System.out.println("CUSTOM IN M2 Key=" + key.toString() + ", Value=" + value.toString().substring(0, 20));
        //count the total number of documents
        HashMap<String, DocIdFreq> terms = new HashMap<>();
        JSONObject jsonObject = new JSONObject(value.toString());
        String reviewText = jsonObject.getString("reviewText");
        String category = jsonObject.getString("category");
        //TODO THIS IS NOT UNIQUE! (e.g. 6304429150)
        String docId = jsonObject.getString("asin");

        //1.1 - Tokenization to unigrams
        for (String token : extractTokens(reviewText.toLowerCase())) {
            // 1.2 lowercased
            //1.3 - Stopword-filtering
//                if (!stopwords.contains(token)) {
            if (terms.containsKey(token)) {
                terms.get(token).frequency.set(terms.get(token).frequency.get() + 1);
            } else {
                terms.put(token, new DocIdFreq(new Text(docId), new LongWritable(1), new Text(category)));
            }
//                }
        }

        // add terms to map
        for (Map.Entry<String, DocIdFreq> entry : terms.entrySet()) {
            context.write(new Text(entry.getKey()), entry.getValue());
        }
    }

    public static List<String> extractTokens(String lcLince) {
        Matcher matcher = CAPTURE_WORDS.matcher(lcLince);
        List<String> result = new ArrayList<>();
        while (matcher.find()) {
            result.add(matcher.group().replaceAll("[\\.,]+$", ""));
        }
        return result;
    }
}