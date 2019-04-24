import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.TreeSet;

/**
 * Splits the reviewText field of JSON documents into single tokens, and additionally extracts the category for later.
 * KEYIN: Text - arbitrary key "KEY"
 * VALUEIN: Text - term
 * KEYOUT: NullWritable - nothing, as we do not need a key here, we just write out the value
 * VALUEOUT: Text - all terms concatenated, separated by a single whitespace
 */
public class MergeReducer extends Reducer<Text, Text, NullWritable, Text> {

    private Text outputValue = new Text();

    /**
     * @param key     key
     * @param values  all values
     * @param context context
     * @throws IOException          ex
     * @throws InterruptedException ex
     */
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        TreeSet<String> set = new TreeSet<>();
        for (Text val : values) {
            set.add(val.toString());
        }
        StringBuilder sb = new StringBuilder();
        for (String s : set) {
            sb.append(s);
            sb.append(" ");
        }
        outputValue.set(sb.toString());
        context.write(NullWritable.get(), outputValue);
    }
}
