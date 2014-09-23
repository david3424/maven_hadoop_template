package org.conan.myhadoop.mr.common;

/**
 * Created with IntelliJ IDEA.
 * User: liliangyang
 * Date: 14-6-24
 * Time: 上午8:49
 * GameETLv2
 * ETL解析器的参数
 */
public class ParameterBean {
    //等待被解析的一行数据
    private String line=null;

    //etl解析器的实现类,eg:com.wanmei.gamelog.etl.CommonETLImpl
    private String etlClassName=null;

    //正则表达式,这里一个log文件中的log只是用一种正则表达式。
    private String pattern=null;

    //每个map解析的log文件的路径和文件名,
    // eg:hdfs://master:49000/export/gamelog/xa/2014-05-29/xa.new_rolesbrief.csv.1.03
    //文件名中的1.03表示 服务器组为1,03表示2点~3点1小时的log；
    private String logFilePathAndName=null;

    //每个Map解析的log文件的文件名，从 logFilePathAndName 解析得到
    //eg:xa.new_rolesbrief.csv
    private String logFileName=null;

    //log文件的日期：eg：2014-06-01，将从logFilePathAndName中解析得到
    private String day;

    //这个字段在解析每小时的log是有意义；
    //解析之后的log存放的路径中会以小时为文件夹存放
    //eg：/export/bisql/gamesql/xa/2014-06-01/hour/01/2014-06-01/xa.world2.formatlog/gamelog_xa_stone_combine/gamelog_xa_stone_combine-r-00015
    //以天为单位存放的log路径为：
    // eg：export/bisql/gamesql/xa/2014-06-01/day/2014-06-01/xa.world2.formatlog/gamelog_xa_stone_combine/gamelog_xa_stone_combine-r-00015
    //以天为单位的log没有01那个文件夹。
    private String hour;

    public String getLine() {
        return line;
    }

    public void setLine(String line) {
        this.line = line;
        System.out.println("line = " + line);
    }

    public String getEtlClassName() {
        return etlClassName;
    }

    public void setEtlClassName(String etlClassName) {
    	System.out.println("etlClassName="+etlClassName);
    	this.etlClassName = etlClassName;
    }

    public String getPattern() {
        return pattern;
    }

    public void setPattern(String pattern) {
    	System.out.println("pattern="+pattern);
        this.pattern = pattern;
    }

    public String getLogFilePathAndName() {
        return logFilePathAndName;
    }

    public void setLogFilePathAndName(String logFilePathAndName) {
    	System.out.println("logFilePathAndName = "+logFilePathAndName);
    	this.logFilePathAndName = logFilePathAndName;
    }

    public String getLogFileName() {
        return logFileName;
    }

    public void setLogFileName(String logFileName) {
    	System.out.println("logFileName = "+logFileName);
    	this.logFileName = logFileName;
    }

    public String getDay() {
        return day;
    }

    public void setDay(String day) {
    	System.out.println("day = "+day);
    	this.day = day;
    }

    public String getHour() {
        return hour;
    }

    public void setHour(String hour) {
    	System.out.println("hour = "+hour);
    	this.hour = hour;
    }
}
