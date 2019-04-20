import org.apache.hadoop.io.ArrayWritable;

public class Chi2DataMapperArray extends ArrayWritable {
    public Chi2DataMapperArray() {
        super(Chi2DataMapper.class);
    }

    public Chi2DataMapperArray(Chi2DataMapper[] values) {
        super(Chi2DataMapper.class, values);
    }

}