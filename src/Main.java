import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Main {
    public static void main(String[] args) throws Exception {
        Job firstJob = Job.getInstance();
        
        firstJob.setJarByClass(FirstMapreducerClass.class);
        
        firstJob.setMapperClass(FirstMapreducerClass.FirstMapper.class);
        firstJob.setReducerClass(FirstMapreducerClass.FirstReducer.class);

        firstJob.setMapOutputKeyClass(Text.class);
        firstJob.setMapOutputValueClass(Text.class);

        FileInputFormat.addInputPath(firstJob, new Path(args[0]));
        FileOutputFormat.setOutputPath(firstJob, new Path("First_Job_output"+args[1]));

        firstJob.waitForCompletion(true);
    }
}