package matrix;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;

import java.io.IOException;
import java.util.Iterator;
import java.util.regex.Pattern;

/**
 *
 * Created by hasee on 2017/12/13.
 */
public class Step1 {

    private static Pattern DELIMITER = Pattern.compile("\t");

    public static class Step1_ReadData extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> {

        private static final Text v = new Text();

        @Override
        public void map(LongWritable key, Text value, OutputCollector<Text, Text> outputCollector, Reporter reporter) throws IOException {
            String[] data = Step1.DELIMITER.split(value.toString());
            String rows = data[0];
            String[] lines = data[1].split(",");
            for (int i = 0; i < lines.length; i++) {
                String column = lines[i].split("_")[0];
                String dataValue = lines[i].split("_")[1];
                v.set(rows + "_" + dataValue);
                outputCollector.collect(new Text(column), v);
            }
        }
    }


    public static class Step1_DealMatrix extends MapReduceBase implements Reducer<Text, Text, Text, Text> {

        private final static Text k = new Text();
        private final static Text v = new Text();

        @Override
        public void reduce(Text key, Iterator<Text> value, OutputCollector<Text, Text> outputCollector, Reporter reporter) throws IOException {
            StringBuilder sb = new StringBuilder();
            while (value.hasNext()) {
                sb.append(",").append(value.next());
            }
            String s = sb.toString().replaceFirst(",", "");
            v.set(s);
            outputCollector.collect(k, v);
        }
    }

    public static void run() throws IOException {
        JobConf conf = new JobConf();

        conf.set("mapred.remote.os","Linux");
        conf.set("yarn.resourcemanager.address","192.168.26.128:8032");
        conf.set("mapreduce.app-submission.cross-platform","true");
        conf.addResource("classpath:/hadoop/mapred-site.xml");
        conf.addResource("classpath:/hadoop/core-site.xml");
        conf.addResource("classpath:/hadoop/yarn-site.xml");
        conf.setJobName("test");
        System.setProperty("hadoop.home.dir","hdfs://192.168.26.128:9000/home/testsss");

        conf.setMapOutputKeyClass(Text.class);
        conf.setMapOutputValueClass(Text.class);

        conf.setOutputKeyClass(Text.class);
        conf.setOutputValueClass(Text.class);

        conf.setMapperClass(Step1_ReadData.class);
        conf.setCombinerClass(Step1_DealMatrix.class);
        conf.setReducerClass(Step1.Step1_DealMatrix.class);

        conf.setInputFormat(TextInputFormat.class);
        conf.setOutputFormat(TextOutputFormat.class);

        FileInputFormat.setInputPaths(conf, new Path("hdfs://192.168.26.128:9000/home/kms.sh"));
        FileOutputFormat.setOutputPath(conf, new Path("hdfs://192.168.26.128:9000/home/testsss"));

        RunningJob job = JobClient.runJob(conf);
        while (!job.isComplete()) {
            job.waitForCompletion();
        }
    }

    public static void main(String[] args) throws IOException {
        run();
    }
}
