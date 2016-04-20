
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class SecondMapreducerClass {
    public static class SecondMapper extends Mapper<LongWritable, Text, Text, Text> {
        public void map(LongWritable key, Text value, Context context){
            String line = value.toString();
            String newKey = line.toString().split("\t")[0].split("-")[0].split(" ")[0];
            String val = line.toString().split("\t")[1];
            try {
                context.write(new Text(newKey),new Text(val));
            } catch (IOException e) {
                e.printStackTrace();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }


    /**
     * input: <key, value>, key = Text, value: number of occurrence
     * output: <key, value>, key = Text, value = number of occurrence
     * */

    public static class SecondReducer extends Reducer<Text, Text, Text, Text> {
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            List<Double> list = new ArrayList<Double>();
            for (Text val : values) {
                list.add(Double.parseDouble(val.toString()));
            }
            Double rateOfReturnSum = 0.0;
            int i=0;

            for (Double doub:list) {
            	rateOfReturnSum += doub;
                i++;
            }

            Double avg = rateOfReturnSum/i;
            Double mean_sqr_diff = 0.0;

            for (Double d:list) {
                mean_sqr_diff = mean_sqr_diff + Math.pow((d - avg), 2);
            }

            double volatility = Math.sqrt(mean_sqr_diff/i);
            String temp = Double.toString(volatility);

            try {
                context.write(new Text(temp + " " + key), new Text(temp));
            } catch (IOException e) {
                e.printStackTrace();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }
}