import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class FilterTfIdfReducer extends Reducer<Text, Text, Text, Text> {

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

