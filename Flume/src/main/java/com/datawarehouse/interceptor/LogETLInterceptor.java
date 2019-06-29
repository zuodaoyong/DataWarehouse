package com.datawarehouse.interceptor;

import com.datawarehouse.utils.LogUtils;
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

    @Override
    public Event intercept(Event event) {
        //获取数据
        byte[] body = event.getBody();
        String log=new String(body, Charset.forName("UTF-8"));
        //启动日志(格式：json)，事件日志(格式：服务器事件|json)
        if(log.contains("start")){//启动日志
            if(LogUtils.validateStart(log)){
                return event;
            }
        }else {
            if(LogUtils.validateEvent(log)){
                return event;
            }
        }
        return null;
    }

    @Override
    public List<Event> intercept(List<Event> list) {
        List<Event> resultList=new ArrayList<>();
        for(Event event:list){
            Event intercept = intercept(event);
            if(intercept!=null){
                resultList.add(intercept);
            }
        }
        return resultList;
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
