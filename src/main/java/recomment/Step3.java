package recomment;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;
import recomment.hdfs.HdfsDAO;

import java.io.IOException;
import java.util.Map;

/**
 * Created by hasee on 2017/12/8.
 */
public class Step3 {

    public static class Step3_UserVectorSplitterMapper extends MapReduceBase implements Mapper<LongWritable, Text, IntWritable, Text> {

        private final static IntWritable k = new IntWritable();
        private final static Text v = new Text();

        @Override
        public void map(LongWritable key, Text value, OutputCollector<IntWritable, Text> outputCollector, Reporter reporter) throws IOException {
            //1       102:3.0,103:2.5,101:5.0
            String[] tokens = Recommend.DELIMITER.split(value.toString());
            for (int i = 1; i < tokens.length; i++) {
                String[] vector = tokens[i].split(":");
                int itemID = Integer.parseInt(vector[0]);
                String score = vector[1];
                //得到 102 1:3.0  每个商品不同用户的评分
                k.set(itemID);
                v.set(tokens[0] + ":" + score);
                outputCollector.collect(k, v);
            }
        }
    }

    public static void run1(Map<String, String> path) throws IOException {
        JobConf conf = Recommend.config();

        String input = path.get("Step3Input1");
        String output = path.get("Step3Output1");

        HdfsDAO hdfs = new HdfsDAO(Recommend.HDFS, conf);
        hdfs.rmr(output);

        conf.setOutputKeyClass(IntWritable.class);
        conf.setOutputValueClass(Text.class);

        conf.setMapperClass(Step3_UserVectorSplitterMapper.class);

        conf.setInputFormat(TextInputFormat.class);
        conf.setOutputFormat(TextOutputFormat.class);

        FileInputFormat.setInputPaths(conf, new Path(input));
        FileOutputFormat.setOutputPath(conf, new Path(output));

        RunningJob job = JobClient.runJob(conf);
        while (!job.isComplete()) {
            job.waitForCompletion();
        }
    }

    public static class Step3_CooccurrenceColumnWrapperMapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, IntWritable> {

        private final static Text k = new Text();
        private final static IntWritable v = new IntWritable();

        @Override
        public void map(LongWritable key, Text value, OutputCollector<Text, IntWritable> outputCollector, Reporter reporter) throws IOException {
            String[] tokens = Recommend.DELIMITER.split(value.toString());
            k.set(tokens[0]);
            v.set(Integer.parseInt(tokens[1]));
            outputCollector.collect(k, v);
        }
    }

    public static void run2(Map<String, String> path) throws IOException {
        JobConf conf = Recommend.config();

        String input = path.get("Step3Input2");
        String output = path.get("Step3Output2");

        conf.setOutputValueClass(IntWritable.class);
        conf.setOutputKeyClass(Text.class);

        conf.setMapperClass(Step3_CooccurrenceColumnWrapperMapper.class);

        conf.setInputFormat(TextInputFormat.class);
        conf.setOutputFormat(TextOutputFormat.class);

        FileInputFormat.setInputPaths(conf, new Path(input));
        FileOutputFormat.setOutputPath(conf, new Path(output));

        RunningJob job = JobClient.runJob(conf);
        while (!job.isComplete()) {
            job.waitForCompletion();
        }
    }
}
