package org.conan.myhadoop.mr.common;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.CounterGroup;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;


public class ETLCounter {
	
	//用来保存main函数中输入log的路径，在
	//public static final String ETL_COUNTER_LOGINPUTPATH="gamelog_path";
	public static final String ETL_COUNTER_GROUP_SUCCESS_NUM="etl_success_number";
	
	
	
	/**
	 * 统计解析已经解析完成的数据行数
	 * 应该等于
	 * zx.world2.formatlog.filter.type=lottery;rolelogout_gs;forbiduser_question;newbie_exit_game_reason;getattach;gbonusshop_trade;accountlogin;accountlogin_1;accountlogin_2;rolelogin;rolelogout;deleterole;accountlogout;accountlogout_1;accountlogout_2;task_complete;gamemailbox;regmailbox;gamemailbox_award;regmailbox_award;obtain_cash_gift;addcash;stockbalance;gmlog;shoptrade;task_abort;task_accept;territoryowner;forbiduser;clear360cash;clear360item
	 的和
     * @param context hadoop context
     */
	public static void counterETLLineNumber(String groupName,String logFilename,Mapper<Object, Text, Text, NullWritable>.Context context) {
		context.getCounter(groupName,logFilename).increment(1);
	}
	
	/**
	 * 减去包含关键字的数量
	 * @param context hadoop context
	 */
	@SuppressWarnings("rawtypes")
	public static void minusETLLineNumber(String groupName,String logFilename,Context context) {
		context.getCounter(groupName,logFilename).increment(-1);
	}
	
	
	/**
	 * 保存统计数量
	 * gameName 
	 * @param job
	 * @param conf
	 * @param path
	 * gamePropertiesPath=hdfs://master:49000/user/hadoop/etl/conf/xa.properties
	 * path=hdfs://master:49000/export/bisql/gamesql/xa/2014-05-29/hour1
	 */
	public static void saveHourCountNumber(Job job,String path,Configuration conf) {

		CounterGroup etlSuccessGroup = null;

		
		try {
			etlSuccessGroup = job.getCounters().getGroup(ETL_COUNTER_GROUP_SUCCESS_NUM);
		} catch (IOException e) {
		}
		
		//迭代
		Iterator <Counter> containIterator = etlSuccessGroup.iterator();
		
		//统计的集合
		List<String> countList = new ArrayList<String>();
		countList.add("           contain         Analyze");
		
		String gamePrefix = "";
		
		//包含的
		while(containIterator.hasNext()) {
			Counter containCounter = containIterator.next();
			//保存的信息
			StringBuilder stringBuilder = new StringBuilder();
			
			//显示的名称
			String displayName = containCounter.getDisplayName().toString().trim();
			
			
			//包含的数据
			stringBuilder.append(displayName);
			
			for(int i = 0 ; i < (70 - displayName.length()); i++) {
				stringBuilder.append(" ");
			}
			
			stringBuilder.append(containCounter.getValue());
			
			
			 //最后计数器的数据
			countList.add(stringBuilder.toString());
			System.out.println("GameTool count countlist row="+stringBuilder.toString());
		}
		
		//hadoop 文件系统
		FileSystem hdfs = null;
		FSDataOutputStream out = null;
		Path outPath = new Path(path+ "/counter/file");
		
		try {
			hdfs = FileSystem.get(outPath.toUri(),conf);
			out = hdfs.create(outPath );
			
			//循环输出
			for(String st  : countList) {
				out.writeBytes(st + "\n");
			}
			
			//关闭
			out.close();
			hdfs.close();
		} catch (IOException e) {
		}
	}

}
