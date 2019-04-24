import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Performing a reduce join operation, the term is only written into the context if it is present on both sides, once
 * indicated through the presence of 2 values, and one having a payload, ad one having NaN as payload.
 * KEYIN: Text - term
 * VALUEIN: Text - values from left (TfIdf-part1) and right(chi2-part2)
 * KEYOUT: Text - term
 * VALUEOUT: Text - weight
 */
public class FilterTfIdfReducer extends Reducer<Text, Text, Text, Text> {

    /**
     * @param key     term
     * @param values  values
     * @param context context
     * @throws IOException          ex
     * @throws InterruptedException ex
     */
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        List<Text> list = new ArrayList<>();
        for (Text val : values) {
            list.add(val);
        }
        //size should be 2 -> 1 from TfIdf(Phase 2), 1 from merge (Phase 4)
        if (list.size() == 2) {
            String payload = "";
            for (Text val : list) {
                String s = val.toString();
                payload = payload + s;
                //If value is not NaN from merged chi2 file, write result into context
                if (!s.isEmpty() && !s.equals("NaN")) {
                    context.write(key, val);
                }
            }
            //throw new RuntimeException("PAYLOAD: KEY: " + key.toString() + "; SIZE: " + list.size() + "; PAYLOAD: " + payload);
        }
    }
}

