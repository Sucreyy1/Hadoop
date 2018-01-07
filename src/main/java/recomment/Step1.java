package recomment;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;
import recomment.hdfs.HdfsDAO;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map;

/**
 *  建立客户的评分矩阵
 * Created by yy on 2017/12/7.
 */
public class Step1 {

    public static class Step1_ToItemPreMapper extends MapReduceBase implements Mapper<Object,Text,IntWritable,Text>{

        private final static IntWritable k = new IntWritable();
        private final static Text v =new Text();

        @Override
        public void map(Object key, Text text, OutputCollector<IntWritable, Text> outputCollector, Reporter reporter) throws IOException {
            //将文件中的数据一行一行读取，并以","分割开
            String[] tokens = Recommend.DELIMITER.split(text.toString());
            //用户id
            int userId = Integer.parseInt(tokens[0]);
            //电影id
            String itemID = tokens[1];
            //用户评分
            String score = tokens[2];
            k.set(userId);
            v.set(itemID+":"+score);
            outputCollector.collect(k,v);
        }


    }

    public static class Step1_ToUserVectorReducer extends MapReduceBase implements Reducer<IntWritable,Text,IntWritable,Text>{

        private final static Text v =new Text();

        @Override
        public void reduce(IntWritable key, Iterator<Text> value, OutputCollector<IntWritable, Text> outputCollector, Reporter reporter) throws IOException {
            StringBuilder sb = new StringBuilder();
            //遍历每个传过来的userID和score，得到用户的评分矩阵
            while (value.hasNext()){
                sb.append(",").append(value.next());
            }
            v.set(sb.toString().replaceFirst(",",""));
            /*
                最终结果
                1       102:3.0,103:2.5,101:5.0
                2       101:2.0,102:2.5,103:5.0,104:2.0
                3       107:5.0,101:2.0,104:4.0,105:4.5
                4       101:5.0,103:3.0,104:4.5,106:4.0
                5       101:4.0,102:3.0,103:2.0,104:4.0,105:3.5,106:4.0
             */
            outputCollector.collect(key,v);
        }
    }


    public static void run(Map<String,String> path) throws IOException{
        JobConf conf = Recommend.config();

        String input = path.get("Step1Input");
        String output = path.get("Step1Output");

        //hdfs操作工具
        HdfsDAO hdfs = new HdfsDAO(Recommend.HDFS,conf);
        hdfs.rmr(input);
        hdfs.mkdirs(input);
        hdfs.copyFile(path.get("data"),input);

        //配置mapper输出的key/value的类型
        conf.setMapOutputKeyClass(IntWritable.class);
        conf.setMapOutputValueClass(Text.class);
        //配置reducer输出的key/value类型
        conf.setOutputKeyClass(IntWritable.class);
        conf.setOutputValueClass(Text.class);
        //配置mapper/reducer
        conf.setMapperClass(Step1_ToItemPreMapper.class);
        conf.setCombinerClass(Step1_ToUserVectorReducer.class);
        conf.setReducerClass(Step1_ToUserVectorReducer.class);
        //
        conf.setInputFormat(TextInputFormat.class);
        conf.setOutputFormat(TextOutputFormat.class);
        //文件输入输出路径
        FileInputFormat.setInputPaths(conf,new Path(input));
        FileOutputFormat.setOutputPath(conf,new Path(output));

        RunningJob job = JobClient.runJob(conf);
        while (!job.isComplete()){
            job.waitForCompletion();
        }
    }
}
