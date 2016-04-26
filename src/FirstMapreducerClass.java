//   Written by Bharat Chand Goli
//	 NYSE stock analysis using Mapreduce
// 	 Running successfully on the cluster with 3 nodes (master - slave1 - slave2)
//	 First job mapper and reducer class

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import java.io.IOException;
import java.util.*;


public class FirstMapreducerClass {
        public static class FirstMapper extends Mapper<LongWritable, Text, Text, Text> {
            public int counter = 0;
            public void map(LongWritable key, Text value, Context context){
                    context.getInputSplit();
                    String fileName = ((FileSplit) context.getInputSplit()).getPath().getName();
                    String line = value.toString();
                    String empty = "";
                    String[] str = new String[2];
                    if(counter > 0) {
                        try {
                            String[] temp = line.split(",");
                            str = temp[0].split("-");
                            empty = str[2] + " " + temp[6];
                        }
                        catch (Exception e)
                        {
                            System.out.println(line + counter);
                        }
                    }
                counter++;
                try {
                    if(counter > 1)
                        context.write(new Text(fileName.split("\\.")[0] + "-" + str[0] + "-" + str[1]),new Text(empty));
                } catch (IOException e) {
                    e.printStackTrace();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }



    public static class FirstReducer extends Reducer<Text, Text, Text, Text> {

        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            Iterator<Text> iter = values.iterator();
            HashMap<Integer, Double> dayValue = new HashMap<Integer, Double>();

            while(iter.hasNext()){
                String s = iter.next().toString();
                dayValue.put(Integer.parseInt(s.split(" ")[0]), Double.parseDouble(s.split(" ")[1]));
            }

            TreeMap<Integer, Double> treeMap = new TreeMap<Integer, Double>(dayValue);

            java.util.Map.Entry<Integer, Double> first = treeMap.firstEntry();
            java.util.Map.Entry<Integer, Double> last = treeMap.lastEntry();

            double rateOfReturn = (last.getValue() - first.getValue())/first.getValue();

            Text value = new Text(String.valueOf(rateOfReturn));
            try {
                context.write(key, value);
            } catch (IOException e) {
                e.printStackTrace();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }
}
