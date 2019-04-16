import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Cluster;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.StringUtils;
import org.json.JSONObject;

import java.io.*;
import java.util.*;


public class MrJob {
    public static class DocIdFreq implements WritableComparable<DocIdFreq> {
        public DocIdFreq() {

        }
        public DocIdFreq(Text docId, LongWritable frequency) {
            this.docId = docId;
            this.frequency = frequency;
        }
        public Text docId = new Text("");
        public LongWritable frequency = new LongWritable(0);
        public DoubleWritable tfidf = new DoubleWritable(0d);

        @Override
        public int compareTo(DocIdFreq o) {
            return o.docId.compareTo(this.docId);
        }

        @Override
        public void write(DataOutput dataOutput) throws IOException {
            this.docId.write(dataOutput);
            this.frequency.write(dataOutput);
            this.tfidf.write(dataOutput);
        }

        @Override
        public void readFields(DataInput dataInput) throws IOException {
            this.docId.readFields((dataInput));
            this.frequency.readFields((dataInput));
            this.tfidf.readFields((dataInput));
        }
    }
    private static Set<String> stopwords = new HashSet<>();
    public enum Count {
        TOTAL_DOCUMENTS
    }

    public static class TokenizerMapper extends Mapper<Object, Text, Text, DocIdFreq> {

        /**
         * Stopwords
         * hdfs:///user/elmar/amazon-reviews/full/complete/reviewscombined.json
         *
         * {"reviewerID": "A2ZFFXGLJUHD76", "asin": "B00LYPUPZK", "reviewerName": "Dan Bernstein",
         * "helpful": [0, 0], "reviewText": "Fake!", "overall": 1.0, "summary": "One Star",
         * "unixReviewTime": 1405900800, "reviewTime": "07 21, 2014", "category": "Health_and_Personal_Care"}
         *
         *  TF_B = f(d,f)
         *  IDF_B2 = ln (N  / ft)
         *
         *  N ... Number of documents
         *  ft ... number of documents containing term t
         *
         *  result: Term, <DocId, Frequency>
         *
         *
         * @param key
         * @param value
         * @param context
         * @throws IOException
         * @throws InterruptedException
         */
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
<<<<<<< Updated upstream
            //count the total number of documents
            context.getCounter(Count.TOTAL_DOCUMENTS).increment(1);

=======
            HashMap<String, DocIdFreq> terms = new HashMap<>();
>>>>>>> Stashed changes
            JSONObject jsonObject = new JSONObject(value.toString());
            String reviewText = jsonObject.getString("reviewText");
            String docId = jsonObject.getString("asin");
            context.getCounter(Count.TOTAL_DOCUMENTS).increment(1);
            //1.1 - Tokenization to unigrams
            StringTokenizer itr = new StringTokenizer(reviewText);
            while (itr.hasMoreTokens()) {
                //1.2 Casefolding
                String token = itr.nextToken().toLowerCase();
                //1.3 - Stopword-filtering
//                if (!stopwords.contains(token)) {
                    if (terms.containsKey(token)) {
                        terms.get(token).frequency.set(terms.get(token).frequency.get() + 1);
                    } else {
                        terms.put(token, new DocIdFreq(new Text(docId), new LongWritable(1)));
                    }
//                }
            }
            // add terms to map
            for (Map.Entry<String, DocIdFreq> entry : terms.entrySet()) {
                context.write(new Text(entry.getKey()), entry.getValue());
            }
        }
    }

    public static class IntSumReducer extends Reducer<Text, DocIdFreq, Text, DocIdFreq> {

        public static long documentCounter = 0;
        @Override
        public void setup(Context context) throws IOException, InterruptedException{
            Configuration conf = context.getConfiguration();
            Cluster cluster = new Cluster(conf);
            Job currentJob = cluster.getJob(context.getJobID());
            documentCounter = currentJob.getCounters().findCounter(Count.TOTAL_DOCUMENTS).getValue();
        }

        public void reduce(Text key, Iterable<DocIdFreq> values, Context context) throws IOException, InterruptedException {

            //TF_B= r(dt)=f(dt)
            //IDF_B2 = loge (N/ft)
            //TF_B * IDF_B2
            // fdt = number of term t in document d
            // N = number of documents
            // ft = number of documents with term t
            HashSet<DocIdFreq> newSet = new HashSet<DocIdFreq>();
            int f_t = 0;
            for (DocIdFreq val : values) {
                f_t++;
                newSet.add(val);
            }

            for(DocIdFreq val: newSet) {
                double TF_B = val.frequency.get();
                double IDF_B2 = Math.log(documentCounter / f_t);
                val.tfidf = new DoubleWritable(TF_B * IDF_B2);
                context.write(key, val);
//                throw new RuntimeException("Reducer Exception " + key.toString());
            }
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
//        stopwords = parseStopwords();
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "PR - VSM");
        job.setNumReduceTasks(2);
        job.setJarByClass(MrJob.class);
        job.setMapperClass(TokenizerMapper.class);
        job.setCombinerClass(IntSumReducer.class);
        job.setReducerClass(IntSumReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DocIdFreq.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
