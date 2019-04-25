import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Merges all relevant data in the correct order into a string or format: term_i:weight_i.
 * KEYIN: Text - term/token
 * VALUEIN: Output1Data, encapsulating term and tf_idf score
 * KEYOUT: Text - Summary in form term_i:weight_i
 * VALUEOUT: NullWritable - no KEY:VALUE structure needed here
 */
public class Output1Reducer extends Reducer<Text, Output1Data, Text, NullWritable> {

    private NullWritable outputNull = NullWritable.get();
    private List<String> tokens = new ArrayList<>();

    /**
     * Tokenizes the reviewText and saves it.
     *
     * @param context context
     * @throws IOException          ex
     * @throws InterruptedException ex
     */
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        this.tokens = Output1Mapper.getTokensFromFirstLine();
    }

    /**
     * Take all values, order them using the extracted tokens from the hardcoded reviewText and emit them in format
     * term_i:weight_i
     *
     * @param key     the term
     * @param values  all respective values
     * @param context context
     * @throws IOException          ex
     * @throws InterruptedException ex
     */
    @Override
    protected void reduce(Text key, Iterable<Output1Data> values, Context context) throws IOException,
            InterruptedException {
        Map<String, Double> map = new HashMap<>();
        for (Output1Data data : values) {
            map.put(data.term.toString(), data.tfidf.get());
        }
        StringBuilder sb = new StringBuilder();
        for (String token : tokens) {
            if (map.containsKey(token)) {
                sb.append(token);
                sb.append(":");
                sb.append(map.get(token));
                sb.append(" ");
            }
        }
        context.write(new Text(sb.toString()), outputNull);
    }
}
