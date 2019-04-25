import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Gets a line (category - terms) and forwards it to the reducer, using a constant key "KEY", to achieve that
 * all values are combined by a single reducer.
 * KEYIN: Text - category
 * VALUEIN: Text - term
 * KEYOUT: Text - constant key "KEY"
 * VALUEOUT: Text - term
 */
public class MergeMapper extends Mapper<Object, Text, Text, Text> {

    private Text outputValue = new Text();
    private Text outputKey = new Text("KEY");

    /**
     * @param key     category
     * @param value   term
     * @param context context
     * @throws IOException          ex
     * @throws InterruptedException ex
     */
    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String[] values = value.toString().split("\\s");
        if (values.length > 1) {
            for (int i = 1; i < values.length; i++) {
                String s = values[i].trim();
                if (!s.isEmpty()) {
                    outputValue.set(values[i]);
                    context.write(outputKey, outputValue);
                }
            }
        }
    }
}
