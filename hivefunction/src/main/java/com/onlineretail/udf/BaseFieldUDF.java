package com.onlineretail.udf;

import jodd.util.StringUtil;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.json.JSONObject;

/*
对hdfs中ods层的事件日志做处理

 */
public class BaseFieldUDF extends UDF {
    /*
        line格式： 1582838283|{"cm"(公共字段-用户信息):{},"et"(事件json数组):[{
        "ett"(time):" 123414","en"(事件类型):"display","kv"(en具体的信息):{ }}]}
     */
    public String evaluate(String line,String key){

        String[] log = line.split("\\|");

        if(log.length!=2 || StringUtil.isBlank(log[1].trim())){
            return "";
        }

        String serverTime = log[0].trim();

        JSONObject jsonObject = new JSONObject(log[1].trim());

        String res = "";
        if("st".equals(key)){
            res = serverTime;
        }else if("et".equals(key)){
            if(jsonObject.has("et")){
                res = jsonObject.getString("et");
            }
        }else{
            JSONObject cm = jsonObject.getJSONObject("cm");
            if(cm.has(key)){
                res = cm.getString(key);
            }
        }
        return res;
    }
}
