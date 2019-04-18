import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class Chi2Reducer extends Reducer<Text, Chi2DataMapperArray, Text, Text> {

    public long documentCounter = 0;
    @Override
    public void setup(Context context) throws IOException, InterruptedException{
        super.setup(context);
        Configuration conf = context.getConfiguration();
        this.documentCounter = conf.getLong("TOTAL_DOCUMENTCOUNT", -1);
    }

    public void reduce(Text category, Chi2DataMapperArray values, Context context) throws IOException, InterruptedException {
        System.out.println("CUSTOM IN R3A " + category.toString());
        for (Writable val : values.get()) {
            Chi2DataMapper dataMapper = (Chi2DataMapper) val;
            System.out.println("CUSTOM IN R3B " + dataMapper.term.toString() + ":A=" + dataMapper.A.toString() + ":B=" + dataMapper.B.toString());

        }
        // calc C

        // calc D = N -(A+B+C)
        context.write(new Text("Hello"), new Text("World"));
    }

}