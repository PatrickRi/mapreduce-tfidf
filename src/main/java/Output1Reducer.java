import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Output1Reducer extends Reducer<Text, Output1Data, Text, NullWritable> {

    private NullWritable outputNull = NullWritable.get();
    private List<String> tokens = new ArrayList<>();

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        this.tokens = Output1Mapper.getTokensFromFirstLine();
    }

    @Override
    protected void reduce(Text key, Iterable<Output1Data> values, Context context) throws IOException, InterruptedException {
        Map<String, Double> map = new HashMap<>();
        for(Output1Data data : values) {
           map.put(data.term.toString(), data.tfidf.get());
        }
        StringBuilder sb = new StringBuilder();
        for(String token : tokens) {
            if(map.containsKey(token)) {
                sb.append(token);
                sb.append(":");
                sb.append(map.get(token));
                sb.append(" ");
            }
        }
        context.write(new Text(sb.toString()), outputNull);
    }
}
