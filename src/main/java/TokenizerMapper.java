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
 * KEYIN: Object (index)
 * VALUEIN: line of Text
 * KEYOUT: Text - extracted token/term
 * VALUEOUT: DocIdFreq, encapsulating documentId, frequency of the term, and the document category (for seocnd part of the assignment)
 */
public class TokenizerMapper extends Mapper<Object, Text, Text, DocIdFreq> {

    public static Pattern CAPTURE_WORDS = Pattern.compile("([\\p{L}0-9][\\p{L}0-9]*)", Pattern.UNICODE_CASE);
    private static Set<String> stopwords = new HashSet<>();
    private static Text outputKey = new Text();

    public static void main(String... args) throws IOException, InterruptedException {
        new TokenizerMapper().setup(null);
    }

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
        stopwords = parseFile("stopswords.txt");
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
        String docId = jsonObject.getString("asin");

        //1.1 - Tokenization to unigrams
        // 1.2 lowercase
        for (String token : extractTokens(reviewText.toLowerCase())) {
            //1.3 - Stopword-filtering
            if (!stopwords.contains(token)) {
                if (terms.containsKey(token)) {
                    terms.get(token).frequency.set(terms.get(token).frequency.get() + 1);
                } else {
                    terms.put(token, new DocIdFreq(new Text(docId), new LongWritable(1), new Text(category)));
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

    private static Set<String> parseFile(String uri) {
        Set<String> list = new HashSet<>();
        BufferedReader fis = null;
        try {
            fis = new BufferedReader(new InputStreamReader(Map.class.getResourceAsStream(uri), StandardCharsets.UTF_8));
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