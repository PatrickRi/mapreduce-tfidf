import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Generates a tuple (term,NaN) for every term read from the merged list of top 200 terms from the chi2 calculation.
 * KEYIN: Object - index
 * VALUEIN: Text - term
 * KEYOUT: Text - term
 * VALUEOUT: Text - NaN - indicating its origin from the merged chi2 list
 */
public class MergedListMapper extends Mapper<Object, Text, Text, Text> {

    private Text nan = new Text();

    /**
     * @param key     index
     * @param value   term
     * @param context context
     * @throws IOException          ex
     * @throws InterruptedException ex
     */
    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        context.write(value, nan);
    }
}
