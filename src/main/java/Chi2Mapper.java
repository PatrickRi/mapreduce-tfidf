import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;


public class Chi2Mapper extends Mapper<Text, Iterable<DocIdFreq>, Text, Text> {

    public class Chi2AB {
        public long A = 0l;
        public long B = 0l;

        public Chi2AB() {

        }
        public Chi2AB(long A, long B) {
            this.A = A;
            this.B = B;
        }
    }

    public void map(Text term, Iterable<DocIdFreq> values, Context context) throws IOException, InterruptedException {
        System.out.println("CUSTOM IN M3 Key=" + term.toString());

        HashMap<Text, HashMap<Text, Chi2AB>> categories = new HashMap<>();
        for (DocIdFreq val : values) {
            if (categories.containsKey(val.category)) {
                HashMap<Text, Chi2AB> categoryData = categories.get(val.category);
                if (categoryData.containsKey(term)) {
                    categoryData.get(term).A++; // number of documents containing t in c
                } else {
                    categoryData.put(term, new Chi2AB(1l, 0l));
                }
            } else {
                HashMap<Text, Chi2AB> terms = new HashMap<>();
                terms.put(term, new Chi2AB(1, 0));
                categories.put(val.category, terms);
            }
        }
        for (Map.Entry<Text, HashMap<Text, Chi2AB>> category: categories.entrySet()) {
            for (Map.Entry<Text, Chi2AB> currentTerm: category.getValue().entrySet()) {
                currentTerm.getValue().B = this.countDocumentsWithTermNotInCategory(categories, currentTerm.getKey(), category.getKey());
            }
        }

        for (Map.Entry<Text, HashMap<Text, Chi2AB>> category: categories.entrySet()){
            StringBuffer terms = new StringBuffer();
            for (Text termString : category.getValue().keySet()) {
                terms.append(termString.toString());
                terms.append(",");
            }
            context.write(category.getKey(), new Text(terms.toString()));
        }
    }

    private long countDocumentsWithTermNotInCategory(HashMap<Text, HashMap<Text, Chi2AB>> categories, Text term, Text category) {
        long count = 0;
        for ( Text categoryKey: categories.keySet()) {
            if (!category.toString().equals(categories.toString())) { // all categories except current
                HashMap<Text, Chi2AB> categoryData = categories.get(categoryKey);
                if (categoryData.containsKey(term)) { // find term
                    count += categoryData.get(term).A; // number of documents containing t but not in c
                }
            }
        }
        return count;
    }
}