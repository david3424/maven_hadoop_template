package org.conan.myhadoop.mr.kpi;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.IOException;
import java.util.Iterator;

public class KPIClient { 

    public static class KPIClientMapper extends MapReduceBase implements Mapper<Object, Text, Text, IntWritable> {
        private IntWritable one = new IntWritable(1);
        private Text word = new Text();

        @Override
        public void map(Object key, Text value, OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException {
            KPI kpi = KPI.filterBroswer(value.toString());
            if (kpi.isValid()) {
                word.set(kpi.getClient_type());
                output.collect(word, one);
            }
        }
    }

    public static class KPIClientReducer extends MapReduceBase implements Reducer<Text, IntWritable, Text, IntWritable> {
        private IntWritable result = new IntWritable();

        @Override
        public void reduce(Text key, Iterator<IntWritable> values, OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException {
            int sum = 0;
            while (values.hasNext()) {
                sum += values.next().get();
            }
            result.set(sum);
            output.collect(key, result);
        }
    }

    public static void main(String[] args) throws Exception {
//        String input = "hdfs://master:49000/user/hdfs/log_kpi/";
//        String output = "hdfs://master:49000/user/hdfs/log_kpi/pv";


        Configuration conf = new Configuration();
        String otherArgs[] = (new GenericOptionsParser(conf, args)).getRemainingArgs();
        if (otherArgs.length != 2)
        {
            System.err.println("Usage: org.conan.myhadoop.mr.kpiKPIClient <in> <out>");
            System.exit(2);
        }

        String input = otherArgs[0];
        String output = otherArgs[1] ;

        JobConf job = new JobConf(KPIClient.class);
        job.setJobName("KPIClient");
        job.addResource("classpath:/hadoop/core-site.xml");
        job.addResource("classpath:/hadoop/hdfs-site.xml");
        job.addResource("classpath:/hadoop/mapred-site.xml");

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        job.setMapperClass(KPIClientMapper.class);
        job.setCombinerClass(KPIClientReducer.class);
        job.setReducerClass(KPIClientReducer.class);

        job.setInputFormat(TextInputFormat.class);
        job.setOutputFormat(TextOutputFormat.class);

        FileInputFormat.setInputPaths(job, new Path(input));
        FileOutputFormat.setOutputPath(job, new Path(output));

        JobClient.runJob(job);
        System.exit(0);
    }

}
