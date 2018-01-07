package wordCount.hadoop;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 *
 * Created by hasee on 2017/11/30.
 */
public class WordCountMapper extends Mapper<LongWritable,Text,Text,IntWritable> {

    /**
     * MapReduce框架每读到一行数据就会调用一次这个map方法
     * map的处理流程就是接受一个key，value，然后处理过后最后输出一个key value对
     * key默认情况下是mr矿机所读到一行文本的起始偏移量（Long类型），value默认情况下是mr框架所读到的一行的数据内容（String类型）
     * 因为mr程序的输出的数据需要在不同的机器间传输，所以必须是可序列化的。
     * @param key
     * @param value
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        //得到输出的每一行数据
        String line = value.toString();
        //通过空格分隔
        String[] words = line.split(" ");
        //循环遍历输出
        for (String word : words){
            context.write(new Text(word),new IntWritable(1));
        }
    }
}
