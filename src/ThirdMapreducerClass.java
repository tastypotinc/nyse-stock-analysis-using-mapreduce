//   Written by Bharat Chand Goli
//	 NYSE stock analysis using Mapreduce
// 	 Running successfully on the cluster with 3 nodes (master - slave1 - slave2)
//	 Third job mapper and reducer class

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class ThirdMapreducerClass {

    public static class ThirdMapper extends Mapper<LongWritable, Text, Text, Text> {
        public void map(LongWritable key, Text value, Context context) {
            String line = value.toString();
            String newKey = line.toString().split("\t")[0];

            try {
                context.write(new Text("unUsed"), new Text(newKey));
            } catch (IOException e) {
                e.printStackTrace();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

    }

    public static class ThirdReducer extends Reducer<Text, Text, Text, Text> {
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        List<String> list = new ArrayList<String>();
            for(Text val : values){
                list.add(val.toString());
            }

            Collections.sort(list);

            try {

                context.write(new Text("Top 10 lowest volatility companies"), new Text(""));
                context.write(new Text("$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$"), new Text(""));
                for(int i = 0; i < 10; i++){
                    context.write(new Text(list.get(i).split(" ")[1]), new Text(list.get(i).split(" ")[0]));
                }

                context.write(new Text("\n Top 10 highest volatility companies"), new Text(""));
                context.write(new Text("$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$"), new Text(""));

                for(int i = list.size() - 1; i >= list.size() - 10; i--){
                    context.write(new Text(list.get(i).split(" ")[1]), new Text(list.get(i).split(" ")[0]));
                }
            } catch (IOException e) {
                e.printStackTrace();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }
}



