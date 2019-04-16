import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.json.JSONObject;

import java.io.*;


public class CatCountJob {
    public static class CategoryMapper extends Mapper<Object, Text, Text, LongWritable> {

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            JSONObject jsonObject = new JSONObject(value.toString());
            String category = jsonObject.getString("category");
            if (category == null || category.trim().isEmpty()) {
                category = "NA";
            }
            context.write(new Text(category), new LongWritable(1));
        }
    }

    public static class SumReducer extends Reducer<Text, LongWritable, Text, LongWritable> {
        public void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
            long count = 0;
            for(LongWritable val: values) {
                count += val.get();
            }
            context.write(key, new LongWritable(count));
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "PR - VSM");
        job.setNumReduceTasks(2);
        job.setJarByClass(CatCountJob.class);
        job.setMapperClass(CategoryMapper.class);
        job.setCombinerClass(SumReducer.class);
        job.setReducerClass(SumReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
