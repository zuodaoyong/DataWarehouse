package com.datawarehouse.commerce;

import com.alibaba.fastjson.JSONObject;
import com.datawarehouse.commerce.accumulators.SessionAccumulator;
import com.datawarehouse.commerce.bean.UserInfo;
import com.datawarehouse.commerce.bean.UserVisitAction;
import com.datawarehouse.config.InitSpark;
import com.datawarehouse.utils.TimeUtils;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.*;
import org.apache.spark.sql.*;
import scala.Tuple2;

import java.util.*;

public class SessionApplication {


    private static String TIME_PERIOD_1s_3s = "1s_3s";
    private static String TIME_PERIOD_4s_6s = "4s_6s";
    private static String TIME_PERIOD_7s_9s = "7s_9s";
    private static String TIME_PERIOD_10s_30s = "10s_30s";
    private static String TIME_PERIOD_30s_60s = "30s_60s";
    private static String TIME_PERIOD_1m_3m = "1m_3m";
    private static String TIME_PERIOD_3m_10m = "3m_10m";
    private static String IME_PERIOD_10m_30m = "10m_30m";
    private static String TIME_PERIOD_30m = "30m";

    public static void main(String[] args) {
        InitSpark initSpark = new InitSpark();
        SparkSession sparkSession = initSpark.getSparkSession(SessionApplication.class.getSimpleName(), 1);
        JavaRDD<UserVisitAction> userVisitActionRDD = getUserVisitActionRDD(sparkSession);
        JavaPairRDD<Long, String> userIdAggr = getUserIdAggr(userVisitActionRDD);
        JavaPairRDD<Long, UserInfo> userInfoRDD = getUserInfo(sparkSession);
        JavaRDD<Tuple2<String, String>> sessionIdFullRDD = userIdAggrJoinUserInfo(userIdAggr, userInfoRDD);
        // 注册自定义累加器
        SessionAccumulator sessionAccumulator=new SessionAccumulator();
        sparkSession.sparkContext().register(sessionAccumulator,"sessionAccumulator");
        JavaRDD<Tuple2<String, String>> sessionIdStaticFullRDD = sessionIdStaticFullRDD(sessionIdFullRDD, sessionAccumulator);
        sessionIdStaticFullRDD.foreach(new VoidFunction<Tuple2<String, String>>() {
            @Override
            public void call(Tuple2<String, String> stringStringTuple2) throws Exception {
                System.out.println(stringStringTuple2);
            }
        });
        Map<String, Integer> sessionAccumulatorValue = sessionAccumulator.value();

        Iterator<Map.Entry<String, Integer>> iterator = sessionAccumulatorValue.entrySet().iterator();
        while (iterator.hasNext()){
            Map.Entry<String, Integer> next = iterator.next();
            System.out.println(next.getKey()+":"+next.getValue());
        }
        sparkSession.close();
    }

    private static JavaRDD<Tuple2<String, String>> sessionIdStaticFullRDD(JavaRDD<Tuple2<String, String>> sessionIdFullRDD,SessionAccumulator sessionAccumulator){
        return sessionIdFullRDD.map(new Function<Tuple2<String, String>, Tuple2<String, String>>() {
            @Override
            public Tuple2<String, String> call(Tuple2<String, String> stringStringTuple2) throws Exception {
                String sessionId=stringStringTuple2._1;
                sessionAccumulator.add(sessionId);
                JSONObject jsonObject=JSONObject.parseObject(stringStringTuple2._2);
                Integer visitLen = jsonObject.getInteger("visitLen");
                if(visitLen>=1&&visitLen<=3){
                    sessionAccumulator.add(TIME_PERIOD_1s_3s);
                }else if(visitLen>=4&&visitLen<=6){
                    sessionAccumulator.add(TIME_PERIOD_4s_6s);
                }else if(visitLen>=7&&visitLen<=9){
                    sessionAccumulator.add(TIME_PERIOD_7s_9s);
                }else if(visitLen>=10&&visitLen<=30){
                    sessionAccumulator.add(TIME_PERIOD_10s_30s);
                }else if(visitLen>=30&&visitLen<=60){
                    sessionAccumulator.add(TIME_PERIOD_30s_60s);
                }else if(visitLen>=60&&visitLen<=180){
                    sessionAccumulator.add(TIME_PERIOD_1m_3m);
                }else if(visitLen>=180&&visitLen<=600){
                    sessionAccumulator.add(TIME_PERIOD_3m_10m);
                }else if(visitLen>=600&&visitLen<=1800){
                    sessionAccumulator.add(IME_PERIOD_10m_30m);
                }else if(visitLen>=1800){
                    sessionAccumulator.add(TIME_PERIOD_30m);
                }
                return stringStringTuple2;
            }
        });
    }

    private static JavaRDD<Tuple2<String, String>> userIdAggrJoinUserInfo(JavaPairRDD<Long, String> userIdAggr,JavaPairRDD<Long, UserInfo> userInfoRDD){
        JavaPairRDD<Long, Tuple2<String, UserInfo>> join = userIdAggr.join(userInfoRDD);
        JavaRDD<Tuple2<String, String>> map = join.map(new Function<Tuple2<Long, Tuple2<String, UserInfo>>, Tuple2<String, String>>() {
            @Override
            public Tuple2<String, String> call(Tuple2<Long, Tuple2<String, UserInfo>> longTuple2Tuple2) throws Exception {
                long userId = longTuple2Tuple2._1;
                Tuple2<String, UserInfo> tuple2 = longTuple2Tuple2._2;
                UserInfo userInfo = tuple2._2;
                JSONObject jsonObject = JSONObject.parseObject(tuple2._1);
                jsonObject.put("age", userInfo.getAge());
                jsonObject.put("city", userInfo.getCity());
                jsonObject.put("sex", userInfo.getSex());
                jsonObject.put("professional", userInfo.getProfessional());
                return new Tuple2<String, String>(jsonObject.getString("sessionId"), jsonObject.toJSONString());
            }
        });
        return map;
    }

    private static JavaPairRDD<Long, UserInfo> getUserInfo(SparkSession sparkSession){
        sparkSession.sql("use commerce");
        Dataset<Row> df = sparkSession.sql("select * from user_info");
        Encoder<UserInfo> bean = Encoders.bean(UserInfo.class);
        Dataset<UserInfo> ds = df.as(bean);
        JavaRDD<UserInfo> userInfoJavaRDD = ds.toJavaRDD();
        userInfoJavaRDD.cache();
        JavaPairRDD<Long, UserInfo> longUserInfoJavaPairRDD = userInfoJavaRDD.mapPartitionsToPair(new PairFlatMapFunction<Iterator<UserInfo>, Long, UserInfo>() {
            @Override
            public Iterator<Tuple2<Long, UserInfo>> call(Iterator<UserInfo> userInfoIterator) throws Exception {
                List<Tuple2<Long, UserInfo>> list = new ArrayList<>();
                while (userInfoIterator.hasNext()) {
                    UserInfo next = userInfoIterator.next();
                    list.add(new Tuple2<Long, UserInfo>(next.getUser_id(), next));
                }
                return list.iterator();
            }
        });
        return longUserInfoJavaPairRDD;
    }

    private static JavaPairRDD<Long, String> getUserIdAggr(JavaRDD<UserVisitAction> userVisitActionRDD){
        JavaPairRDD<String, Iterable<UserVisitAction>> userVisitActionBySessionGroup = userVisitActionRDD.mapPartitionsToPair(new PairFlatMapFunction<Iterator<UserVisitAction>, String, UserVisitAction>() {
            @Override
            public Iterator<Tuple2<String, UserVisitAction>> call(Iterator<UserVisitAction> userVisitActionIterator) throws Exception {
                List<Tuple2<String, UserVisitAction>> list = new ArrayList<>();
                while (userVisitActionIterator.hasNext()) {
                    UserVisitAction next = userVisitActionIterator.next();
                    list.add(new Tuple2<String, UserVisitAction>(next.getSession_id(), next));
                }
                return list.iterator();
            }
        }).groupByKey();
        userVisitActionBySessionGroup.cache();
        JavaPairRDD<Long, String> longStringJavaPairRDD = userVisitActionBySessionGroup.mapPartitionsToPair(new PairFlatMapFunction<Iterator<Tuple2<String, Iterable<UserVisitAction>>>, Long, String>() {
            @Override
            public Iterator<Tuple2<Long, String>> call(Iterator<Tuple2<String, Iterable<UserVisitAction>>> tuple2Iterator) throws Exception {
                List<Tuple2<Long, String>> list = new ArrayList<>();
                while (tuple2Iterator.hasNext()) {
                    Tuple2<String, Iterable<UserVisitAction>> next = tuple2Iterator.next();
                    String sessionId = next._1;
                    Long userId = null;
                    JSONObject jsonObject = new JSONObject();
                    Set<String> keyWords = new HashSet<>();
                    Set<Long> click_category = new HashSet<>();
                    String startTime = null;
                    String endTime = null;
                    int stepLen = 0;
                    Iterator<UserVisitAction> it = next._2.iterator();
                    while (it.hasNext()) {
                        stepLen++;
                        UserVisitAction action = it.next();
                        if (userId == null) {
                            userId = action.getUser_id();
                        }
                        if (action.getSearch_keyword() != null) {
                            keyWords.add(action.getSearch_keyword());
                        }
                        if (action.getClick_category_id() != null) {
                            click_category.add(action.getClick_category_id());
                        }
                        long actionTime = TimeUtils.getFormatTimeByStr(action.getAction_time(), TimeUtils.SECONDSTR).getTime();
                        if (startTime == null) {
                            startTime = action.getAction_time();
                        } else {
                            long start_time = TimeUtils.getFormatTimeByStr(startTime, TimeUtils.SECONDSTR).getTime();
                            if (actionTime < start_time) {
                                startTime = action.getAction_time();
                            }
                        }
                        if (endTime == null) {
                            endTime = action.getAction_time();
                        } else {
                            long end_time = TimeUtils.getFormatTimeByStr(endTime, TimeUtils.SECONDSTR).getTime();
                            if (actionTime > end_time) {
                                endTime = action.getAction_time();
                            }
                        }
                    }
                    jsonObject.put("sessionId", sessionId);
                    jsonObject.put("userId", userId);
                    jsonObject.put("keyWords", keyWords);
                    jsonObject.put("click_category", click_category);
                    jsonObject.put("startTime", startTime);
                    jsonObject.put("endTime", endTime);
                    long start_time = TimeUtils.getFormatTimeByStr(startTime, TimeUtils.SECONDSTR).getTime();
                    long end_time = TimeUtils.getFormatTimeByStr(endTime, TimeUtils.SECONDSTR).getTime();
                    jsonObject.put("visitLen", (end_time - start_time) / 1000);
                    jsonObject.put("stepLen", stepLen);
                    list.add(new Tuple2<>(userId, jsonObject.toJSONString()));
                }
                return list.iterator();
            }
        });
        return longStringJavaPairRDD;
    }

    private static JavaRDD<UserVisitAction> getUserVisitActionRDD(SparkSession sparkSession){
        sparkSession.sql("use commerce");
        Dataset<Row> df = sparkSession.sql("select * from user_visit_action");
        Encoder<UserVisitAction> visitActionEncoder = Encoders.bean(UserVisitAction.class);
        Dataset<UserVisitAction> ds = df.as(visitActionEncoder);
        return ds.toJavaRDD();
    }
}
