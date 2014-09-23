package org.conan.myhadoop.mr.kpi;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
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
import org.conan.myhadoop.mr.common.*;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

/**
 * Created with IntelliJ IDEA. User: liliangyang Date: 14-6-23 Time: 下午5:37
 * 用来解析每小时的log
 */
public class KPITest {

    public static class HourETLMap extends
            Mapper<Object, Text, Text, NullWritable> {
        // map输出的关键字
        private Text mapKey = new Text();
        ParameterBean parameterBean = null;// 用来存放etl解析器的参数

        @Override
        protected void setup(Context context) throws IOException,
                InterruptedException {
            // setup方法会在每个map执行前执行一次。初始化此map执行解析方法etl的参数
            parameterBean = new ParameterBean();

            // 游戏配置文件的路径 hdfs://master:49000/user/hadoop/etl/conf/xa.properties
            // 在调用main方法是传入这个参数
            Properties properties = new Properties();
            String gamePropertiesPath = context.getConfiguration().get(
                    "game.properties.path");
            Configuration conf = context.getConfiguration();
            Path path = new Path(gamePropertiesPath);
            FileSystem fileSystem = FileSystem.get(path.toUri(), conf);
            InputStream inputStream = fileSystem.open(path);
            properties.load(inputStream);

            // 读取log的文件名，得到文件名后就能得到解析这个log的正则表达式。
            // 读取块,取得当前正在处理的log文件的名称
            InputSplit inputSplit = context.getInputSplit();
            // 获得读取文件的路径
            // eg：filePath=hdfs://master:49000/export/gamelog/xa/2014-05-29/xa.new_rolesbrief.csv.1.03
            String logFilePathAndName = ((FileSplit) inputSplit).getPath()
                    .toString();

            parameterBean.setLogFilePathAndName(logFilePathAndName);

            String logFileName = ETLUnit
                    .getLogFileNameFromPath(logFilePathAndName);
            parameterBean.setLogFileName(logFileName);

            String day = ETLUnit.getLogFileDayFromPath(logFilePathAndName);
            parameterBean.setDay(day);

            String hour = ETLUnit.getLogFileHourFromPath(logFilePathAndName);
            parameterBean.setHour(hour);

            // 一个log文件只有一种格式，因此只要一种正则表达式就行了
            String logPattern = properties.getProperty(parameterBean
                    .getLogFileName() + ".pattern.type");
            parameterBean.setPattern(logPattern);

            // 取得解析当前log的ETL实现类地址
            String etlClassName = properties.getProperty("etlClassName");
            parameterBean.setEtlClassName(etlClassName);

        }

        @Override
        protected void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {
            parameterBean.setLine(value.toString().trim());

            StringBuffer lineByEtled = null;// 存放被etl方法解析之后的数据，是被解析文件中的一行数据。
            GameETL etl = null;// 解析器，注意这里是接口;

            // 开始解析每一行数据
            etl = (GameETL) ETLAopFactory.getAOPProxyedObject(parameterBean
                    .getEtlClassName());
            lineByEtled = etl.execute(parameterBean);

            // 将解析之后的一行数据作为Map输出的key，value为空就行。
            if (null != lineByEtled
                    && lineByEtled.toString().trim().length() > 0) {
                System.out.println("lineByEtled = " + lineByEtled);

                // 解析完成，计数器加1
                ETLCounter.counterETLLineNumber(ETLCounter.ETL_COUNTER_GROUP_SUCCESS_NUM, parameterBean.getLogFileName(),context);
                mapKey.set(lineByEtled.toString());
                context.write(mapKey, NullWritable.get());
            }

        }
    }

    public static class HourETLReduce extends
            Reducer<Text, NullWritable, Text, NullWritable> {

        private MultipleOutputs<Text, NullWritable> mos;
        private Text reduceKey = new Text();

        @Override
        protected void setup(Context context) throws IOException,
                InterruptedException {
            mos = new MultipleOutputs<Text, NullWritable>(context);
        }

        @Override
        protected void reduce(Text key, Iterable<NullWritable> values,
                              Context context) throws IOException, InterruptedException {
            // 分割
            // key=2014-05-30 00:00:00 11142 393844740 2127310970 offical 2 31 0
            // 1!s_ln=2251#srbip=172.22.71.254#srbgn=11142%2013-09-23/xa.new_rolesbrief.csv/xa.new_rolesbrief.csv
            String stmp[] = key.toString().split("%");

            String content = stmp[0];
            String fileOutPathAndName = stmp[1];

            if (null != content && content.trim().length() > 0
                    && null != fileOutPathAndName
                    && fileOutPathAndName.trim().length() > 0) {
                reduceKey.set(content);
                // 输出
                mos.write(reduceKey, NullWritable.get(), fileOutPathAndName);
            }
        }

        @Override
        protected void cleanup(Context context) throws IOException,
                InterruptedException {
            mos.close();
        }
    }

    public static void main(String[] args) throws Exception {
        // hadoop 集群的配置文件
        Configuration conf = new Configuration();
        // 传入的参数
        String[] otherArgs = new GenericOptionsParser(conf, args)
                .getRemainingArgs();
        // 判断参数的数量
        if (otherArgs.length < 3 || otherArgs.length > 4) {
            System.err.println("传入参数不足~~");
            System.exit(1);
        }

        // 设置游戏的名称，hdfs://master:49000/user/hadoop/etl/conf/xa.properties
        conf.set("game.properties.path", otherArgs[2].trim());

        // 任务名称

        Job job = new Job(conf, "xa 2014-04-29 hour");

        job.setJarByClass(KPITest.class);
        job.setMapperClass(HourETLMap.class);
        job.setReducerClass(HourETLReduce.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(NullWritable.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);

        // 设定reduce数量
        if (otherArgs.length == 4) {
            job.setNumReduceTasks(Integer.parseInt(otherArgs[3].trim()));
        }

        // hadoop 文件路径
        FileSystem hdfs = FileSystem.get(conf);
        // 多文件路径按逗号分隔
        String input[] = otherArgs[0].trim().split(",");

        // 如果切割开了，如果有多个输入文件
        if (input.length > 1) {
            // 循环通配符路径
            for (int i = 0; i < input.length; i++) {
                Path path = new Path(input[i].trim());
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

        FileOutputFormat.setOutputPath(job, new Path(otherArgs[1].trim()));

        boolean runResult = job.waitForCompletion(true);

        if (runResult) {// 如果执行完成
            // 计数器的统计
            ETLCounter.saveHourCountNumber(job, otherArgs[1].trim(), conf);
            // 删除无用文件

            System.exit(job.waitForCompletion(true) ? 0 : 1);
        }

    }
}
