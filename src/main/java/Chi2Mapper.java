import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Mapper;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;


public class Chi2Mapper extends Mapper<Text, DocIdFreqArray, Text, Chi2DataMapperArray> {

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

    public void map(Text term, DocIdFreqArray values, Context context) throws IOException, InterruptedException {
        System.out.println("CUSTOM IN M3 Key=" + term.toString());

        HashMap<Text, HashMap<Text, Chi2AB>> categories = new HashMap<>();
        for (Writable val : values.get()) {
            DocIdFreq docIdFreq = (DocIdFreq) val;
            if (categories.containsKey(docIdFreq.category)) {
                HashMap<Text, Chi2AB> categoryData = categories.get(docIdFreq.category);
                if (categoryData.containsKey(term)) {
                    categoryData.get(term).A++; // number of documents containing t in c
                } else {
                    categoryData.put(term, new Chi2AB(1l, 0l));
                }
            } else {
                HashMap<Text, Chi2AB> terms = new HashMap<>();
                terms.put(term, new Chi2AB(1, 0));
                categories.put(docIdFreq.category, terms);
            }
        }
        for (Map.Entry<Text, HashMap<Text, Chi2AB>> category : categories.entrySet()) {
            for (Map.Entry<Text, Chi2AB> currentTerm : category.getValue().entrySet()) {
                currentTerm.getValue().B = this.countDocumentsWithTermNotInCategory(categories, currentTerm.getKey(), category.getKey());
            }
        }

//        for (Map.Entry<Text, HashMap<Text, Chi2AB>> category: categories.entrySet()){
//            StringBuffer terms = new StringBuffer();
//            for (Text termString : category.getValue().keySet()) {
//                terms.append(termString.toString());
//                terms.append(",");
//            }
//            new DocIdFreqArray(resultSet.toArray(new DocIdFreq[resultSet.size()]))
//      }

        for (Map.Entry<Text, HashMap<Text, Chi2AB>> category: categories.entrySet()) {
            ArrayList<Chi2DataMapper> resultList = new ArrayList<>();
            for (Text term1 : category.getValue().keySet()) {
                resultList.add(new Chi2DataMapper(term1, category.getValue().get(term).A, category.getValue().get(term).B));
            }
            context.write(category.getKey(), new Chi2DataMapperArray(resultList.toArray(new Chi2DataMapper[resultList.size()])));
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