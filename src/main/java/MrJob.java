import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.StringUtils;
import org.json.JSONObject;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.StringTokenizer;

public class MrJob {

    private static Set<String> stopwords = new HashSet<>();

    public static class TokenizerMapper extends Mapper<Object, Text, Text, IntWritable> {

        private final static IntWritable ONE = new IntWritable(1);

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            JSONObject jsonObject = new JSONObject(value.toString());
            String reviewText = jsonObject.getString("reviewText");
            //1.1 - Tokenization to unigrams
            StringTokenizer itr = new StringTokenizer(reviewText);
            while (itr.hasMoreTokens()) {
                //1.2 Casefolding
                String token = itr.nextToken().toLowerCase();
                //1.3 - Stopword-filtering
                if (!stopwords.contains(token)) {
                    Text word = new Text();
                    word.set(token);
                    context.write(word, ONE);
                }
            }
        }
    }

    public static class IntSumReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {

            //TF_B= r(dt)=f(dt)
            //IDF_B2 = loge (N/ft)
            //TF_B * IDF_B2
            // fdt = number of term t in document d
            // N = number of documents
            // ft = number of documents with term t
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            context.write(key, new IntWritable(sum));
        }
    }

    private static Set<String> parseStopwords() {
        return parseFile("/stopwords.txt");
    }


    private static Set<String> parseFile(String uri) {
        Set<String> list = new HashSet<>();
        BufferedReader fis = null;
        try {
            fis = new BufferedReader(new InputStreamReader(Map.class.getResourceAsStream(uri), "UTF-8"));
            String pattern;
            while ((pattern = fis.readLine()) != null) {
                list.add(pattern);
            }
        } catch (IOException ioe) {
            System.err.println("Caught exception parsing file '"
                    + uri + "' : " + StringUtils.stringifyException(ioe));
        } finally {
            try {
                if (fis != null) {
                    fis.close();
                }
            } catch (Exception ex) {
                System.err.println("Caught exception closing file '"
                        + uri + "' : " + StringUtils.stringifyException(ex));
            }
        }
        return list;
    }

    public static void main(String[] args) throws Exception {
        stopwords = parseStopwords();
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "PR - VSM");
        job.setNumReduceTasks(2);
        job.setJarByClass(MrJob.class);
        job.setMapperClass(TokenizerMapper.class);
        job.setCombinerClass(IntSumReducer.class);
        job.setReducerClass(IntSumReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
