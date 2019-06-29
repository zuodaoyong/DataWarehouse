package com.datawarehouse.interceptor;

import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.interceptor.Interceptor;

import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class LogTypeInterceptor implements Interceptor {
    @Override
    public void initialize() {

    }

    @Override
    public Event intercept(Event event) {
        //获取数据
        byte[] body = event.getBody();
        String log=new String(body, Charset.forName("UTF-8"));
        //获取headers
        Map<String, String> headers = event.getHeaders();
        if(log.contains("start")){
            headers.put("topic","topic_start");
        }else {
            headers.put("topic","topic_event");
        }
        return null;
    }

    @Override
    public List<Event> intercept(List<Event> list) {
        List<Event> resultList=new ArrayList<>();
        for(Event event:list){
            resultList.add(event);
        }
        return resultList;
    }

    @Override
    public void close() {

    }

    public static class Builder implements Interceptor.Builder{

        @Override
        public Interceptor build() {
            return new LogTypeInterceptor();
        }

        @Override
        public void configure(Context context) {

        }
    }
}
