package org.conan.myhadoop.mr.kpi;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.util.GenericOptionsParser;
import org.conan.myhadoop.mr.common.ETLUnit;
import org.conan.myhadoop.mr.common.ParameterBean;

import java.io.IOException;

/**
 * 新api方式计算
 */
public class KPIBrowser {


    public static class KPIBrowserMapper extends Mapper<Object, Text, Text, IntWritable> {
        private IntWritable one = new IntWritable(1);
        private Text word = new Text();
        ParameterBean parameterBean = null;// 用来存放etl解析器的参数

        @Override
        protected void setup(Context context) throws IOException,
                InterruptedException {
            parameterBean = new ParameterBean();
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
            System.out.println("============day is :" +day);
            System.out.println("============hour is :" +hour);
        }

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            KPI kpi = KPI.filterBroswer(value.toString());
            if (kpi.isValid()) {
                word.set(kpi.getHttp_user_agent()+"@"+parameterBean.getDay()+"/"+parameterBean.getHour());
                context.write(word, one);
            }
        }
    }

    public static class KPIBrowserReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        private MultipleOutputs<Text, IntWritable> mos;

        @Override
        protected void setup(Context context) throws IOException,
                InterruptedException {
            mos = new MultipleOutputs<Text, IntWritable>(context);
        }

        @Override
        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            String tmp[] = key.toString().split("@");
            String content = tmp[0];
            String fileOutPathAndName = tmp[1];
            for (IntWritable val : values) {
                sum += val.get();
            }
            if (null != content && content.trim().length() > 0 && null != fileOutPathAndName && fileOutPathAndName.trim().length() > 0) {
                key.set(content);
                // 输出
                mos.write(key, new IntWritable(sum), fileOutPathAndName);
            }
        }
        @Override
        protected void cleanup(Context context) throws IOException,
                InterruptedException {
            mos.close();
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration _conf = new Configuration();
        String otherArgs[] = (new GenericOptionsParser(_conf, args)).getRemainingArgs();
        if (otherArgs.length != 2) {
            System.err.println("Usage: org.conan.myhadoop.mr.kpi.KPIBrowser <in> <out>");
            System.exit(2);
        }
        String output = otherArgs[1];
        Job job = new Job(_conf, "KPIBrowser");
        job.setJarByClass(KPIBrowser.class);
        job.setMapperClass(KPIBrowserMapper.class);
        job.setReducerClass(KPIBrowserReducer.class);
        job.setCombinerClass(KPIBrowserReducer.class);//对数据进行早期聚合，结果不再是(is,1,1,1) 而是(is,3) 不能简单的+1，得求和
        job.setOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setJobName("KPIBrowser");
        // hadoop 文件路径
        FileSystem hdfs = FileSystem.get(_conf);
        // 多文件路径按逗号分隔
        String input[] = otherArgs[0].trim().split(",");

        // 如果切割开了，如果有多个输入文件
        if (input.length > 1) {
            // 循环通配符路径
            for (String anInput : input) {
                Path path = new Path(anInput.trim());
                FileStatus[] status = hdfs.globStatus(path);
                Path[] listedPaths = FileUtil.stat2Paths(status);
                // 循环加入完整路径
                for (Path p : listedPaths) {
                    FileInputFormat.addInputPath(job, p);
                }
            }
        } else {
            FileInputFormat.addInputPath(job, new Path(otherArgs[0].trim()));
        }
      /*  conf.addResource("classpath:/hadoop/core-site.xml");
        conf.addResource("classpath:/hadoop/hdfs-site.xml");
        conf.addResource("classpath:/hadoop/mapred-site.xml");*/
//        FileInputFormat.setInputPaths(job, new Path(input));
        FileOutputFormat.setOutputPath(job, new Path(output));

//        JobClient.runJob(conf);
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

}
