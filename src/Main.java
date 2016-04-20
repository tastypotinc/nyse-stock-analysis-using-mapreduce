import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Main {
    public static void main(String[] args) throws Exception {
        Job firstJob = Job.getInstance();
        Job secondJob = Job.getInstance();
        
        firstJob.setJarByClass(FirstMapreducerClass.class);
        secondJob.setJarByClass(SecondMapreducerClass.class);
        
        firstJob.setMapperClass(FirstMapreducerClass.FirstMapper.class);
        firstJob.setReducerClass(FirstMapreducerClass.FirstReducer.class);
        
        secondJob.setMapperClass(SecondMapreducerClass.SecondMapper.class);
        secondJob.setReducerClass(SecondMapreducerClass.SecondReducer.class);

        firstJob.setMapOutputKeyClass(Text.class);
        firstJob.setMapOutputValueClass(Text.class);
        
        secondJob.setMapOutputKeyClass(Text.class);
        secondJob.setMapOutputValueClass(Text.class);

        FileInputFormat.addInputPath(firstJob, new Path(args[0]));
        FileOutputFormat.setOutputPath(firstJob, new Path("First_Job_output"+args[1]));
        
        FileInputFormat.addInputPath(secondJob, new Path("Intermediate_Data_1_"+args[1]));
        FileOutputFormat.setOutputPath(secondJob, new Path("Intermediate_Data_2_"+args[1]));
        
        secondJob.waitForCompletion(true);

        firstJob.waitForCompletion(true);
    }
}