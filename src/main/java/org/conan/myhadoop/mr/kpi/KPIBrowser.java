package org.conan.myhadoop.mr.kpi;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.IOException;

public class KPIBrowser {

    public static class KPIBrowserMapper extends Mapper<Object, Text, Text, IntWritable> {
        private IntWritable one = new IntWritable(1);
        private Text word = new Text();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            KPI kpi = KPI.filterBroswer(value.toString());
            if (kpi.isValid()) {
                word.set(kpi.getHttp_user_agent());
                context.write(word, one);
            }
        }
    }

    public static class KPIBrowserReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

        @Override
        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            context.write(key, new IntWritable(sum));
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration _conf = new Configuration();
        String otherArgs[] = (new GenericOptionsParser(_conf, args)).getRemainingArgs();
        if (otherArgs.length != 2) {
            System.err.println("Usage: org.conan.myhadoop.mr.kpi.KPIBrowser <in> <out>");
            System.exit(2);
        }

        String input = otherArgs[0];
        String output = otherArgs[1];
        Job job = new Job(_conf, "KPIBrowser");
        job.setJarByClass(KPIBrowser.class);
        job.setMapperClass(KPIBrowserMapper.class);
        job.setReducerClass(KPIBrowserReducer.class);
        job.setCombinerClass(KPIBrowserReducer.class);//对数据进行早期聚合，结果不再是(is,1,1,1) 而是(is,3) 不能简单的+1，得求和
        job.setOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setJobName("KPIBrowser");
      /*  conf.addResource("classpath:/hadoop/core-site.xml");
        conf.addResource("classpath:/hadoop/hdfs-site.xml");
        conf.addResource("classpath:/hadoop/mapred-site.xml");*/
        FileInputFormat.setInputPaths(job, new Path(input));
        FileOutputFormat.setOutputPath(job, new Path(output));

//        JobClient.runJob(conf);
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

}
