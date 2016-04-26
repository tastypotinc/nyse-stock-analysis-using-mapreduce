//   Written by Bharat Chand Goli
//	 NYSE stock analysis using Mapreduce
// 	 Running successfully on the cluster with 3 nodes (master - slave1 - slave2) --> h-usr
//	 Main Driver code

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Main {

    public static void main(String[] args) throws Exception {
        Job firstJob = Job.getInstance();
        Job secondJob = Job.getInstance();
        Job thirdJob = Job.getInstance();

        firstJob.setJarByClass(FirstMapreducerClass.class);
        secondJob.setJarByClass(SecondMapreducerClass.class);
        thirdJob.setJarByClass(ThirdMapreducerClass.class);

        firstJob.setMapperClass(FirstMapreducerClass.FirstMapper.class);
        firstJob.setReducerClass(FirstMapreducerClass.FirstReducer.class);

        secondJob.setMapperClass(SecondMapreducerClass.SecondMapper.class);
        secondJob.setReducerClass(SecondMapreducerClass.SecondReducer.class);

        thirdJob.setMapperClass(ThirdMapreducerClass.ThirdMapper.class);
        thirdJob.setReducerClass(ThirdMapreducerClass.ThirdReducer.class);

        firstJob.setMapOutputKeyClass(Text.class);
        firstJob.setMapOutputValueClass(Text.class);

        secondJob.setMapOutputKeyClass(Text.class);
        secondJob.setMapOutputValueClass(Text.class);

        thirdJob.setMapOutputKeyClass(Text.class);
        thirdJob.setMapOutputValueClass(Text.class);


        FileInputFormat.addInputPath(firstJob, new Path(args[0]));
        FileOutputFormat.setOutputPath(firstJob, new Path("Intermediate_Data_1_"+args[1]));

        FileInputFormat.addInputPath(secondJob, new Path("Intermediate_Data_1_"+args[1]));
        FileOutputFormat.setOutputPath(secondJob, new Path("Intermediate_Data_2_"+args[1]));

        FileInputFormat.addInputPath(thirdJob, new Path("Intermediate_Data_2_"+args[1]));
        FileOutputFormat.setOutputPath(thirdJob, new Path("Final_Data_"+args[1]));

        firstJob.waitForCompletion(true);
        secondJob.waitForCompletion(true);
        thirdJob.waitForCompletion(true);


    }
}