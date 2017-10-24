package com.lsl.bigdata.mr.samefriend;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.Arrays;

/**
 * 查找共同好友--》步骤2
 *
 * Created by lishanglai on 2017/10/23.
 */
public class FriendCount2 {

    static class FriendCountMapper extends Mapper<LongWritable,Text,Text,Text> {

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

            String line = value.toString();
            String[] values = line.split("\t");

            String friend = values[0];

            String[] userArr  = values[1].split(",");
            Arrays.sort(userArr);
            for (int i = 0;i < userArr.length-2;i++){

                for (int j = i + 1;j < userArr.length-1;j++){
                    String user_user = userArr[i] + "-" + userArr[j];
                    context.write(new Text(user_user),new Text(friend));
                }

            }
        }
    }

    static class FriendCountReducer extends Reducer<Text,Text,Text,Text> {
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

            String same = "";
            for (Text value:values){
                same += value + ",";
            }

            context.write(key,new Text(same));
        }
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();

        Job job = Job.getInstance(conf);

        //指定本程序的jar包所在的本地路径
        job.setJarByClass(FriendCount2.class);

        //指定本业务job要使用的mapper/reducer业务类
        job.setMapperClass(FriendCountMapper.class);
        job.setReducerClass(FriendCountReducer.class);

        //指定mapper输出类型的key，value类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        //指定最终输出的数据key，value类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        //指定job的输入原始文件所在目录
        FileInputFormat.setInputPaths(job,new Path(args[0]));
        //指定job的输出结果所在目录
        FileOutputFormat.setOutputPath(job,new Path(args[1]));

        boolean b = job.waitForCompletion(true);
        System.exit(b?0:1);
    }

}
