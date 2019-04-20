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
        if (list.size() == 2) {
            String payload = "";
            for (Text val : list) {
                String s = val.toString();
                payload = payload + s;
                if (!s.isEmpty() && !s.equals("NaN")) {
                    context.write(key, val);
                }
            }
            throw new RuntimeException("PAYLOAD: KEY: " + key.toString() + "; SIZE: " + list.size() + "; PAYLOAD: " + payload);
        }
    }
}

