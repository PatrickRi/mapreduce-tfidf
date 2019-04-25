import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class Output1Mapper extends Mapper<Text, DocIdFreqArray, Text, Output1Data> {

    private Text key = new Text("0981850006;1259798400;A2VNYWOPJ13AFP");
    private List<String> tokens = new ArrayList<>();
    private Text outputKey = new Text("KEY");
    private Output1Data outputValue = new Output1Data();

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);
        tokens = getTokensFromFirstLine();
    }

    public static List<String> getTokensFromFirstLine() {
        List<String> result = new ArrayList<>();
        String reviewText = "This was a gift for my other husband.  He's making us things from it all the time and we love the food.  Directions are simple, easy to read and interpret, and fun to make.  We all love different kinds of cuisine and Raichlen provides recipes from everywhere along the barbecue trail as he calls it. Get it and just open a page.  Have at it.  You'll love the food and it has provided us with an insight into the culture that produced it. It's all about broadening horizons. Yum!!";
        List<String> l = TokenizerMapper.extractTokens(reviewText.toLowerCase());
        for (String token : l) {
            if (!result.contains(token)) {
                result.add(token);
            }
        }
        return result;
    }

    @Override
    protected void map(Text term, DocIdFreqArray value, Context context) throws IOException, InterruptedException {
        if(tokens.contains(term.toString())) {
            for(Writable w : value.get()) {
                DocIdFreq freq = (DocIdFreq) w;
                if(freq.docId.equals(key)) {
                    outputValue.term.set(term);
                    outputValue.tfidf.set(freq.tfidf.get());
                    context.write(outputKey, outputValue);
                }
            }
        }
    }
}
