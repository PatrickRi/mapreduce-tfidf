import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.json.JSONObject;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Splits the reviewText field of JSON documents into single tokens, and additionally extracts the category for later.
 * Not any of the fields per document are unique, therefore we use a compound key: "asin+unixReviewTime+reviewerId".
 * KEYIN: Object (index)
 * VALUEIN: line of Text
 * KEYOUT: Text - extracted token/term
 * VALUEOUT: DocIdFreq, encapsulating documentId, frequency of the term, and the document category (for seocnd part
 * of the assignment)
 */
public class TokenizerMapper extends Mapper<Object, Text, Text, DocIdFreq> {

    public static Pattern CAPTURE_WORDS = Pattern.compile("([\\p{L}0-9][\\p{L}0-9]*)", Pattern.UNICODE_CASE);
    private static Set<String> stopwords = new HashSet<>();
    private static Text outputKey = new Text();
    private static Long sequence = 0L;

    /**
     * Parses stopwords file
     *
     * @param context context
     * @throws IOException          ex
     * @throws InterruptedException ex
     */
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);
        stopwords = parseFile("stopwords.txt");
    }

    /**
     * @param key     arbitrary line number
     * @param value   whole line of text, a JSON document
     * @param context context
     * @throws IOException          ex
     * @throws InterruptedException ex
     */
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        //count the total number of documents
        HashMap<String, DocIdFreq> terms = new HashMap<>();
        JSONObject jsonObject = new JSONObject(value.toString());
        String reviewText = jsonObject.getString("reviewText");
        String category = jsonObject.getString("category");
        sequence = sequence + 1;
        String docId;
        Long unixReviewTime;
        String reviewerId;
        try {
            reviewerId = jsonObject.getString("reviewerID");
        } catch (Exception ex) {
            ex.printStackTrace();
            reviewerId = sequence.toString();
        }
        try {
            docId = jsonObject.getString("asin");
        } catch (Exception ex) {
            ex.printStackTrace();
            docId = sequence.toString();
        }
        try {
            unixReviewTime = jsonObject.getLong("unixReviewTime");
        } catch (Exception ex) {
            ex.printStackTrace();
            unixReviewTime = sequence;
        }

        //1.1 - Tokenization to unigrams
        // 1.2 lowercase
        for (String token : extractTokens(reviewText.toLowerCase())) {
            //1.3 - Stopword-filtering
            if (!stopwords.contains(token)) {
                if (terms.containsKey(token)) {
                    terms.get(token).frequency.set(terms.get(token).frequency.get() + 1);
                } else {
                    DocIdFreq docIdFreq = new DocIdFreq();
                    docIdFreq.docId = new Text(docId + ";" + unixReviewTime + ";" + reviewerId);
                    docIdFreq.frequency = new LongWritable(1);
                    docIdFreq.category = new Text(category);
                    terms.put(token, docIdFreq);
                }
            }
        }
        // add terms to map
        for (Map.Entry<String, DocIdFreq> entry : terms.entrySet()) {
            outputKey.set(entry.getKey());
            context.write(outputKey, entry.getValue());
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

    private Set<String> parseFile(String uri) {
        Set<String> list = new HashSet<>();
        BufferedReader fis = null;
        try {
            fis = new BufferedReader(new InputStreamReader(getClass().getResourceAsStream(uri),
                                                           StandardCharsets.UTF_8));
            String pattern;
            while ((pattern = fis.readLine()) != null) {
                list.add(pattern);
            }
        } catch (IOException ioe) {
            throw new RuntimeException(ioe);
        } finally {
            try {
                if (fis != null) {
                    fis.close();
                }
            } catch (Exception ex) {
                throw new RuntimeException(ex);
            }
        }
        return list;
    }
}