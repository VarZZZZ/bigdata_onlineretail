package com.onlineretail.flume.interceptor;

import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.interceptor.Interceptor;

import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;

public class LogETLInterceptor implements Interceptor {
    @Override
    public void initialize() {

    }

    //单event
    @Override
    public Event intercept(Event event) {
        // 清洗数据
        // 如果大括号缺失一半，清除掉
        byte[] body = event.getBody();
        String s = new String(body, Charset.forName("UTF-8"));

        if(s.contains("start")){// 启动日志
            if(LogUtils.validateStart(s)){
                return event;
            }
        }else{
            if(LogUtils.validateEvent(s)){
                return event;
            }
        }
        return null;
    }

    // 多event
    @Override
    public List<Event> intercept(List<Event> events) {

        List<Event> interceptors = new ArrayList<>();
        for (Event event : events) {
            Event intercept1 = intercept(event);
            if(intercept1!=null){
                interceptors.add(intercept1);
            }
        }
        return interceptors;
    }

    @Override
    public void close() {

    }

    public static class Builder implements Interceptor.Builder{

        @Override
        public Interceptor build() {
            return new LogETLInterceptor();
        }

        @Override
        public void configure(Context context) {

        }
    }
}
