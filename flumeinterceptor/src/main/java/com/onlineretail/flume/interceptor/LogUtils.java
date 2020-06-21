package com.onlineretail.flume.interceptor;

import org.apache.commons.lang.math.NumberUtils;

public class LogUtils {
    public static boolean validateStart(String log){
        if(log==null)
            return false;
        if(!log.trim().startsWith("{")||!log.trim().endsWith("}"))
            return false;
        return true;
    }
    public static boolean validateEvent(String log){

        if(log==null) return false;

        String[] logContents = log.split("\\|");

        //校验服务器时间和日志格式
        if(logContents.length!=2) return false;

        //时间长度必须是13位 且数字
        if(logContents[0].trim().length()!=13|| !NumberUtils.isDigits(logContents[0]))
            return false;

        if(!logContents[1].trim().startsWith("{")||!logContents[1].trim().endsWith("}"))
            return false;

        return true;
    }
}
