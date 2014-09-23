package org.conan.myhadoop.mr.kpi.day;

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

/**
 * 新api方式计算
 */
public class KPIDayBrowser {


    public static class KPIDayBrowserMapper extends Mapper<Object, Text, Text, IntWritable> {
        private Text word = new Text();

        @Override
        protected void setup(Context context) throws IOException,
                InterruptedException {
         /*   parameterBean = new ParameterBean();
            // 读取块,取得当前正在处理的log文件的名称
            InputSplit inputSplit = context.getInputSplit();
            String logFilePathAndName = ((FileSplit) inputSplit).getPath()
                    .toString();
            parameterBean.setLogFilePathAndName(logFilePathAndName);
            String day = ETLUnit.getKPILogFileDayFromPath(logFilePathAndName);
            parameterBean.setDay(day);
            String hour = ETLUnit.getKPILogFileHourFromPath(logFilePathAndName);
            parameterBean.setHour(hour);
            System.out.println("============logFilePathAndName is :" + logFilePathAndName);
            System.out.println("============day is :" +day);*/
        }

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String mat[] = value.toString().split("\t");
            if (mat.length==2) {
                word.set(mat[0]);
                context.write(word,new IntWritable(Integer.parseInt(mat[1])));
            }
        }
    }

    public static class KPIDayBrowserReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

        @Override
        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
                // 输出
            context.write(key,new IntWritable(sum));
            }
    }

    public static void main(String[] args) throws Exception {
        Configuration _conf = new Configuration();
        String otherArgs[] = (new GenericOptionsParser(_conf, args)).getRemainingArgs();
        if (otherArgs.length != 2) {
            System.err.println("Usage: org.conan.myhadoop.mr.kpi.day.KPIDayBrowser <in> <out>");
            System.exit(2);
        }
        String input = otherArgs[0];
        String output = otherArgs[1];
        Job job = new Job(_conf, "KPIDayBrowser");
        job.setJarByClass(KPIDayBrowser.class);
        job.setMapperClass(KPIDayBrowserMapper.class);
        job.setReducerClass(KPIDayBrowserReducer.class);
        job.setCombinerClass(KPIDayBrowserReducer.class);//对数据进行早期聚合，结果不再是(is,1,1,1) 而是(is,3) 不能简单的+1，得求和
        job.setOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setJobName("KPIDayBrowser");
      /*  conf.addResource("classpath:/hadoop/core-site.xml");
        conf.addResource("classpath:/hadoop/hdfs-site.xml");
        conf.addResource("classpath:/hadoop/mapred-site.xml");*/
        FileInputFormat.setInputPaths(job, new Path(input));
        FileOutputFormat.setOutputPath(job, new Path(output));

//        JobClient.runJob(conf);
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

}
