import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Tokenizes the first reviewText (hradcoded).
 * Reads a SequenceFile containing a term and a DocIdFreqArray, which are then filtered using
 * the extracted tokens, and the hardcoded document id. The relevant data is then emitted
 * all using the same key "KEY".
 * KEYIN: Text - category
 * VALUEIN: Text - term
 * KEYOUT: Text - constant key "KEY"
 * VALUEOUT: Text - term
 */
public class Output1Mapper extends Mapper<Text, DocIdFreqArray, Text, Output1Data> {

    private Text key = new Text("0981850006;1259798400;A2VNYWOPJ13AFP");
    private List<String> tokens = new ArrayList<>();
    private Text outputKey = new Text("KEY");
    private Output1Data outputValue = new Output1Data();

    /**
     * Tokenizes the reviewText and saves it.
     *
     * @param context context
     * @throws IOException          ex
     * @throws InterruptedException ex
     */
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);
        tokens = getTokensFromFirstLine();
    }

    /**
     * Tokenizes the first reviewText.
     *
     * @return unique list of tokens
     */
    public static List<String> getTokensFromFirstLine() {
        List<String> result = new ArrayList<>();
        String reviewText = "This was a gift for my other husband.  He's making us things from it all the time and we" +
                " love the food.  Directions are simple, easy to read and interpret, and fun to make.  We all love " +
                "different kinds of cuisine and Raichlen provides recipes from everywhere along the barbecue trail as" +
                " he calls it. Get it and just open a page.  Have at it.  You'll love the food and it has provided us" +
                " with an insight into the culture that produced it. It's all about broadening horizons. Yum!!";
        List<String> l = TokenizerMapper.extractTokens(reviewText.toLowerCase());
        for (String token : l) {
            if (!result.contains(token)) {
                result.add(token);
            }
        }
        return result;
    }

    /**
     * Filters the terms using the extracted tokens, and only emits one instance of Output1Data using the hardcoded
     * document id as filter for the DocIdFreqArray.
     *
     * @param term    a term
     * @param value   an array of all DocIdFreq objects, belonging to the term
     * @param context context
     * @throws IOException          ex
     * @throws InterruptedException ex
     */
    @Override
    protected void map(Text term, DocIdFreqArray value, Context context) throws IOException, InterruptedException {
        if (tokens.contains(term.toString())) {
            for (Writable w : value.get()) {
                DocIdFreq freq = (DocIdFreq) w;
                if (freq.docId.equals(key)) {
                    outputValue.term.set(term);
                    outputValue.tfidf.set(freq.tfidf.get());
                    context.write(outputKey, outputValue);
                }
            }
        }
    }
}
