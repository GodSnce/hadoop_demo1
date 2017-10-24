package com.lsl.bigdata.mr.flowsum;

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

/**
 * 手机上下行流量汇总
 *
 * Created by lishanglai on 2017/10/23.
 */
public class FlowCount {


    static class FlowCountMapper extends Mapper<LongWritable,Text,Text,FlowBean>{

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            //将一行内容转化成String
            String line = value.toString();
            //切分字段
            String[] fields = line.split(" ");
            //取出手机号
            String phoneNbr = fields[1];
            //取出上行流量下行流量
            long upFlow = Long.parseLong(fields[2]);
            long dFlow = Long.parseLong(fields[3]);

            context.write(new Text(phoneNbr),new FlowBean(upFlow,dFlow));

        }
    }

    static class FlowCountReducer extends Reducer<Text,FlowBean,Text,FlowBean>{

        @Override
        protected void reduce(Text key, Iterable<FlowBean> values, Context context) throws IOException, InterruptedException {
            long sum_upFlow = 0;
            long sum_dFlow = 0;

            //遍历所有bean，将其中的上行流量，下行流量分别累加
            for (FlowBean bean:values){
                sum_upFlow += bean.getUpFlow();
                sum_dFlow += bean.getdFlow();
            }

            FlowBean resultBean = new FlowBean(sum_upFlow, sum_dFlow);
            context.write(key,resultBean);
        }
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();

        Job job = Job.getInstance(conf);

        //指定本程序的jar包所在的本地路径
        job.setJarByClass(FlowCount.class);

        //指定本业务job要使用的mapper/reducer业务类
        job.setMapperClass(FlowCountMapper.class);
        job.setReducerClass(FlowCountReducer.class);

        //指定mapper输出类型的key，value类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(FlowBean.class);

        //指定最终输出的数据key，value类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(FlowBean.class);

        //指定job的输入原始文件所在目录
        FileInputFormat.setInputPaths(job,new Path(args[0]));
        //指定job的输出结果所在目录
        FileOutputFormat.setOutputPath(job,new Path(args[1]));

        boolean b = job.waitForCompletion(true);
        System.exit(b?0:1);
    }

}
