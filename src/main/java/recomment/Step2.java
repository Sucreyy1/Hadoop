package recomment;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;
import recomment.hdfs.HdfsDAO;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map;

/**
 * 建立物品的同现矩阵
 * Created by yy on 2017/12/8.
 */
public class Step2 {

    public static class Step2_UserVectorToCoocurrenceMapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, IntWritable> {

        private final static Text k = new Text();
        private final static IntWritable v = new IntWritable(1);

        @Override
        public void map(LongWritable key, Text value, OutputCollector<Text, IntWritable> outputCollector, Reporter reporter) throws IOException {
            //将step1中的1       102:3.0,103:2.5,101:5.0数据读取并以","分割
            String[] tokens = Recommend.DELIMITER.split(value.toString());
            for (int i = 1; i < tokens.length; i++) {
                //得到电影的id
                String itemID = tokens[i].split(":")[0];
                for (int j = 1; j < tokens.length; j++) {
                    String itemID2 = tokens[j].split(":")[0];
                    //得到不同电影间的组合次数
                    k.set(itemID + ":" + itemID2);
                    outputCollector.collect(k, v);
                }
            }
        }
    }

    public static class Step2_UserVectorToConoccurrenceReduce extends MapReduceBase implements Reducer<Text, IntWritable, Text, IntWritable> {

        private IntWritable result = new IntWritable();

        @Override
        public void reduce(Text key, Iterator<IntWritable> value, OutputCollector<Text, IntWritable> outputCollector, Reporter reporter) throws IOException {
            //将不同电影组合次数求和，建立起同现矩阵
            int sum = 0;
            while (value.hasNext()) {
                sum += value.next().get();
            }
            result.set(sum);
            outputCollector.collect(key, result);
        }
    }

    public static void run(Map<String, String> path) throws IOException {
        JobConf conf = Recommend.config();
        //设置Step2读取和存放文件的位置
        String input = path.get("Step2Input");
        String output = path.get("Step2Output");

        HdfsDAO hdfs = new HdfsDAO(Recommend.HDFS, conf);
        hdfs.rmr(output);

        conf.setOutputKeyClass(Text.class);
        conf.setOutputValueClass(IntWritable.class);

        conf.setMapperClass(Step2_UserVectorToCoocurrenceMapper.class);
        conf.setCombinerClass(Step2_UserVectorToConoccurrenceReduce.class);
        conf.setReducerClass(Step2_UserVectorToConoccurrenceReduce.class);

        conf.setInputFormat(TextInputFormat.class);
        conf.setOutputFormat(TextOutputFormat.class);

        FileOutputFormat.setOutputPath(conf,new Path(output));
        FileInputFormat.setInputPaths(conf,new Path(input));

        RunningJob job = JobClient.runJob(conf);
        while (!job.isComplete()) {
            job.waitForCompletion();
        }
    }
}
