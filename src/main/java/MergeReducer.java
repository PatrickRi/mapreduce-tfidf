import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.TreeSet;

public class MergeReducer extends Reducer<Text, Text, NullWritable, Text> {

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        TreeSet<String> set = new TreeSet<>();
        for(Text val : values) {
            set.add(val.toString());
        }
        StringBuilder sb = new StringBuilder();
        for(String s : set) {
            sb.append(s);
            sb.append(" ");
        }
        context.write(NullWritable.get(), new Text(sb.toString()));
    }
}
