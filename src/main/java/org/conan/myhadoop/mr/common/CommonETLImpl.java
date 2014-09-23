package org.conan.myhadoop.mr.common;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created with IntelliJ IDEA.
 * User: liliangyang
 * Date: 14-6-24
 * Time: 上午8:42
 * GameETLv2
 * 解析非快照类型，快照类型line中没有日期，日期是通过目录截取后插入到line中的。
 */
public class CommonETLImpl implements GameETL {
    @Override
    public StringBuffer execute(ParameterBean parameterBean) {
        String line=parameterBean.getLine();
        String patternStr=parameterBean.getPattern();

        if (null!=line&&line.trim().length()>0&&null!=patternStr&&patternStr.trim().length()>0){
            StringBuffer sb=new StringBuffer();
            Pattern p=Pattern.compile(parameterBean.getPattern());
            Matcher m=p.matcher(parameterBean.getLine());

            if (m.find()){
                int matchCount=m.groupCount();
                String uniqueMark=null;
                for (int i = 1; i < matchCount; i++) {
                    if (i==1){
                        uniqueMark=m.group(i);
                    }else if(i>1&&i<matchCount-1){
                        sb.append(m.group(i)+"\t");
                    }else{
                        //解析的格式如下：！后面是确定这行log唯一的标记，求天的log是用来比较，进行排重，最终的log没有！后面的部分
                        //eg:2014-05-30 00:00:00	393844740	2127310970	offical		2	31	0	1!s_ln=2251#srbip=172.22.71.254#srbgn=11142
                        sb.append(m.group(i)+"!"+uniqueMark);
                    }
                }
                
                System.out.println("sb.toString() = " + sb.toString());

                //取得服务器组号，插入到line中日期的后面
                //eg:2014-05-30 00:00:00	393844740	2127310970	offical		2	31	0	1!s_ln=2251#srbip=172.22.71.254#srbgn=11142
                //插入后: 111142
                //eg:2014-05-30 00:00:00	11142	393844740	2127310970	offical		2	31	0	1!s_ln=2251#srbip=172.22.71.254#srbgn=11142
                String serverId= ETLUnit.getLogServerIDFromUniqueMark(uniqueMark);
                sb.insert(20,serverId+"\t");

                //将这条log line 存放的文件路径拼接好，接在line的后面，在reduce阶段，解析出来，作为log存放的路径
                //eg：2014-05-30 00:00:00	11142	393844740	2127310970	offical		2	31	0	1!s_ln=2251#srbip=172.22.71.254#srbgn=11142%2013-09-23/xa.new_rolesbrief.csv/xa.new_rolesbrief.csv
                //reduce之后，生成的文件是：2013-09-23/xa.new_rolesbrief.csv/xa.new_rolesbrief.csv_r_000001
                //截取 日期 2014-05-30
                String logDate = sb.substring(0, 10);
                sb.append("%").append(parameterBean.getHour()).append("/").append(logDate).append("/").append(parameterBean.getLogFileName()).append("/").append(parameterBean.getLogFileName());


               // System.out.println("sb.toString() = " + sb.toString());
            }

            return sb;
        }else{
            return null;
        }

    }
}
