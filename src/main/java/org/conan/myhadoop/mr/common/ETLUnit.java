package org.conan.myhadoop.mr.common;

/**
 * Created with IntelliJ IDEA.
 * User: liliangyang
 * Date: 14-6-24
 * Time: 上午10:30
 * GameETLv2
 */
public class ETLUnit {

    /**
     * 通过log文件的路径，得到log的文件名
     * eg：hdfs://master:49000/export/gamelog/xa/2014-05-29/xa.new_rolesbrief.csv.1.03
     * filename=xa.new_rolesbrief.csv
     *
     * @param logpath log文件的路径
     * @return filename
     */
    public static String getLogFileNameFromPath(String logpath) {
        String logFileName = "can't get logfilename";
        if (null != logpath && logpath.trim().length() > 0) {
            String tempStr[] = logpath.split("/");
            if (tempStr.length > 0) {
                logFileName = tempStr[tempStr.length - 1];
            }
            logFileName = logFileName.substring(0, logFileName.length() - 6);
        }
        return logFileName;
    }

    /**
     * 通过log文件的路径，得到log的日期
     * eg：hdfs://master:49000/export/gamelog/xa/2014-05-29/xa.new_rolesbrief.csv.1.03
     * day=2014-05-29
     *
     * @param logpath log路径
     * @return 日期 2014-05-29
     */
    public static String getLogFileDayFromPath(String logpath) {
        String day = "can't get day";
        if (null != logpath && logpath.trim().length() > 0) {
            String tempStr[] = logpath.split("/");
            if (tempStr.length > 0) {
                day = tempStr[tempStr.length - 2];
            }
        }
        return day;
    }

    /**
     * 通过log文件的路径，得到log的日期
     * eg：hdfs://master:49000/flume/nginx/14/09/23/00/nginx.1411405056070
     * day=14-09-23
     * @param logpath log路径
     * @return 日期 14-09-23
     */
    public static String getKPILogFileDayFromPath(String logpath) {
        StringBuilder day = new StringBuilder();
        if (null != logpath && logpath.trim().length() > 0) {
            String tempStr[] = logpath.split("/");
            if (tempStr.length > 0) {
                day = day.append(tempStr[tempStr.length - 5]).append("-").append(tempStr[tempStr.length - 4]).append("-").append(tempStr[tempStr.length - 3]);
            }
        }
        return day.toString();
    }

    /**
     * 通过log文件的路径，得到log的日期
     * eg：hdfs://master:49000/export/gamelog/xa/2014-05-29/xa.new_rolesbrief.csv.1.03
     * day=2014-05-29
     *
     * @param logpath log路径
     * @return 小时 03？
     */
    public static String getLogFileHourFromPath(String logpath) {
        String hour = "can't get hour";
        if (null != logpath && logpath.trim().length() > 0) {
            String tempStr[] = logpath.split("/");
            if (tempStr.length > 0) {
                String filename = tempStr[tempStr.length - 1];
                hour=filename.substring(filename.length()-2,filename.length());
            }
        }
        return hour;
    }

     /**
     * 通过line中的s_ln=2251#srbip=172.22.71.254#srbgn=11142取得服务器组号11142。
     * @param uniqueMark 被分解的唯一标识
     * @return 日期 2014-05-29
     */
    public static String getLogServerIDFromUniqueMark(String uniqueMark) {
        String serverId = "can't get serverId";
        if (null != uniqueMark && uniqueMark.trim().length() > 0) {
            String tempStr[] = uniqueMark.split("=");
            serverId = tempStr[tempStr.length - 1];
        }
        return serverId;
    }




    public static void main(String[] args) {
        String filename = getLogFileNameFromPath("hdfs://master:49000/export/gamelog/xa/2014-05-29/xa.new_rolesbrief.csv.1.03");
        System.out.println("filename = " + filename);
        String day = getLogFileDayFromPath("hdfs://master:49000/export/gamelog/xa/2014-05-29/xa.new_rolesbrief.csv.1.03");
        System.out.println("day = " + day);
        String hour = getLogFileHourFromPath("hdfs://master:49000/export/gamelog/xa/2014-05-29/xa.new_rolesbrief.csv.1.03");
        System.out.println("hour = " + hour);
    }

    /**
     * /flume/nginx/14/09/23/00/nginx.1411405056070
     * @param logpath
     * @return
     */
    public static String getKPILogFileHourFromPath(String logpath) {
        if (null != logpath && logpath.trim().length() > 0) {
            String tempStr[] = logpath.split("/");
            if (tempStr.length > 0) {
                return tempStr[tempStr.length - 2];
            }
        }
        return "no hour";
    }
}
