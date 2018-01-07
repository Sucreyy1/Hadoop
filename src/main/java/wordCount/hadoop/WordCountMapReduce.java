package wordCount.hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 *
 * Created by hasee on 2017/12/1.
 */
public class WordCountMapReduce {

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        //
        Configuration configuration = new Configuration();
        Job job = Job.getInstance(configuration, "word-count");
        job.setJarByClass(WordCountMapReduce.class);
        job.setMapperClass(WordCountMapper.class);
        job.setReducerClass(WordCountReduce.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.setInputPaths(job, new Path("D:\\test\\Nginx+keepalived.txt"));
        FileOutputFormat.setOutputPath(job, new Path("D:\\test\\output"));

        System.setProperty("hadoop.home.dir","D:\\test\\");
        boolean b = job.waitForCompletion(true);
        System.exit(b ? 0 : 1);

    }
}
