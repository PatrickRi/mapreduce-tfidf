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
public class MergeMapper extends Mapper<Text, Text, Text, Text> {

    /**
     * @param key     category
     * @param value   term
     * @param context context
     * @throws IOException          ex
     * @throws InterruptedException ex
     */
    @Override
    protected void map(Text key, Text value, Context context) throws IOException, InterruptedException {
        context.write(new Text("KEY"), value);
    }
}
