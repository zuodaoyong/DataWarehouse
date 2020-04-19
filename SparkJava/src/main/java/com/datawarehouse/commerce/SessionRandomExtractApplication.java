package com.datawarehouse.commerce;

import com.alibaba.fastjson.JSONObject;
import com.datawarehouse.commerce.accumulators.CountAccumulator;
import com.datawarehouse.commerce.bean.UserInfo;
import com.datawarehouse.commerce.bean.UserVisitAction;
import com.datawarehouse.config.InitSpark;
import com.datawarehouse.utils.TimeUtils;
import org.apache.commons.collections.IteratorUtils;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;

import java.util.*;

public class SessionRandomExtractApplication {


    public static void main(String[] args) {

        InitSpark initSpark = new InitSpark();
        SparkSession sparkSession = initSpark.getSparkSession(SessionApplication.class.getSimpleName(), 1);
        JavaRDD<UserVisitAction> userVisitActionRDD = SessionApplication.getUserVisitActionRDD(sparkSession);
        JavaPairRDD<Long, String> userIdAggr = SessionApplication.getUserIdAggr(userVisitActionRDD);
        JavaPairRDD<Long, UserInfo> userInfoRDD = SessionApplication.getUserInfo(sparkSession);
        JavaRDD<Tuple2<String, String>> sessionIdFullRDD = SessionApplication.userIdAggrJoinUserInfo(userIdAggr, userInfoRDD);
        JavaPairRDD<String, String> actionTimeFullRDD = actionTimeFullRDD(sessionIdFullRDD);
        Map<String, Long> actionTimeAggr = actionTimeFullRDD.countByKey();
        Map<String, Map<String, Integer>> actionTimeAggrByDay = actionTimeAggrByDay(actionTimeAggr);
        Map<String, Map<String, List<Integer>>> randomIndexList = generateRandomIndexList(actionTimeAggrByDay);

        JavaSparkContext sc = JavaSparkContext.fromSparkContext(sparkSession.sparkContext());
        Broadcast<Map<String, Map<String, List<Integer>>>> broadcast = sc.broadcast(randomIndexList);

        JavaPairRDD<String, Iterable<String>> actionTimeFullRDDAggr = actionTimeFullRDD.groupByKey();

        JavaPairRDD<String, List<String>> extractSession = extractSession(actionTimeFullRDDAggr, broadcast);

        CountAccumulator countAccumulator=new CountAccumulator();
        sparkSession.sparkContext().register(countAccumulator,"countAccumulator");
        //sparkSession.
        extractSession.foreach(new VoidFunction<Tuple2<String, List<String>>>() {
            @Override
            public void call(Tuple2<String, List<String>> stringListTuple2) throws Exception {
                countAccumulator.add(stringListTuple2._2.size());
                System.out.println(stringListTuple2);
            }
        });

        System.out.println("countAccumulator="+countAccumulator.value());
    }


    public static JavaPairRDD<String, List<String>> extractSession(JavaPairRDD<String, Iterable<String>> actionTimeFullRDDAggr,Broadcast<Map<String, Map<String, List<Integer>>>> broadcast){
        return actionTimeFullRDDAggr.mapPartitionsToPair(new PairFlatMapFunction<Iterator<Tuple2<String, Iterable<String>>>, String, List<String>>() {
            @Override
            public Iterator<Tuple2<String, List<String>>> call(Iterator<Tuple2<String, Iterable<String>>> tuple2Iterator) throws Exception {
                List<Tuple2<String, List<String>>> resList=new ArrayList<>();
                Map<String, Map<String, List<Integer>>> indexMap = broadcast.value();
                while (tuple2Iterator.hasNext()){
                    Tuple2<String, Iterable<String>> next = tuple2Iterator.next();
                    String hourStr=next._1;
                    Iterator<String> fullInfoIterator=next._2.iterator();
                    List<String> list = IteratorUtils.toList(fullInfoIterator);
                    String[] hourArr = hourStr.split("\\s");
                    String dayStr=hourArr[0];
                    List<String> resFullInfoList=new ArrayList<>();
                    if(indexMap.containsKey(dayStr)){
                        Map<String, List<Integer>> hourListMap = indexMap.get(dayStr);
                        if(hourListMap.containsKey(hourStr)){
                            List<Integer> indexList = hourListMap.get(hourStr);

                            for(int i=0;i<list.size();i++){
                                if(indexList.contains(i)){
                                    resFullInfoList.add(list.get(i));
                                }
                            }
                        }
                    }
                    resList.add(new Tuple2<>(hourStr,resFullInfoList));
                }
                return resList.iterator();
            }
        });
    }

    public static Map<String, Map<String, List<Integer>>> generateRandomIndexList(Map<String, Map<String, Integer>> actionTimeAggrByDay){
        Map<String, Map<String, List<Integer>>> indexMap=new HashMap<>();
        Long extractByDay=100L;
        Iterator<Map.Entry<String, Map<String, Integer>>> dayIterator = actionTimeAggrByDay.entrySet().iterator();
        while (dayIterator.hasNext()){
            Map.Entry<String, Map<String, Integer>> dayNext = dayIterator.next();
            Map<String,List<Integer>> hourIndexMap;
            if(indexMap.containsKey(dayNext.getKey())){
                hourIndexMap=indexMap.get(dayNext.getKey());
            }else{
                hourIndexMap=new HashMap<>();
                indexMap.put(dayNext.getKey(),hourIndexMap);
            }
            Long sessionCountByDay=dayNext.getValue().values().stream().reduce((x,y)->x+y).get().longValue();
            Map<String, Integer> hourMap = dayNext.getValue();
            Iterator<Map.Entry<String, Integer>> hourIterator = hourMap.entrySet().iterator();
            while (hourIterator.hasNext()){
                Map.Entry<String, Integer> hourNext = hourIterator.next();
                double v = hourNext.getValue() / sessionCountByDay.doubleValue();
                Long extractByHour=(long)(v*extractByDay);
                if(extractByHour>hourNext.getValue()){
                    extractByHour=hourNext.getValue().longValue();
                }
                Random random=new Random();
                if(hourIndexMap.containsKey(hourNext.getKey())){
                    List<Integer> hourIndexList = hourIndexMap.get(hourNext.getKey());
                    for(int i=0;i<extractByHour;i++){
                        int nextInt = random.nextInt(hourNext.getValue());
                         // 一旦随机生成的index已经存在，重新获取，直到获取到之前没有的index
                         while (hourIndexList.contains(nextInt)){
                             nextInt = random.nextInt(hourNext.getValue());
                         }
                        hourIndexList.add(nextInt);
                    }
                    hourIndexMap.put(hourNext.getKey(),hourIndexList);
                }else{
                    List<Integer> hourIndexList = new ArrayList<>();
                    for(int i=0;i<extractByHour;i++){
                        int nextInt = random.nextInt(hourNext.getValue());
                        while (hourIndexList.contains(nextInt)){
                            nextInt = random.nextInt(hourNext.getValue());
                        }
                        hourIndexList.add(nextInt);
                    }
                    hourIndexMap.put(hourNext.getKey(),hourIndexList);
                }
            }
        }
        return indexMap;
    }

    public static Map<String,Map<String,Integer>> actionTimeAggrByDay(Map<String, Long> actionTimeAggr){
        Map<String,Map<String,Integer>> map=new HashMap<>();
        Iterator<Map.Entry<String, Long>> iterator = actionTimeAggr.entrySet().iterator();
        while (iterator.hasNext()){
            Map.Entry<String, Long> next = iterator.next();
            String hourStr = next.getKey();
            String[] hourArr=hourStr.split("\\s");
            if(map.containsKey(hourArr[0])){
                Map<String, Integer> hourIntegerMap = map.get(hourArr[0]);
                if(!hourIntegerMap.containsKey(hourArr[1])){
                    hourIntegerMap.put(hourStr,next.getValue().intValue());
                }
                map.put(hourArr[0],hourIntegerMap);
            }else {
                Map<String, Integer> hourIntegerMap = new HashMap<>();
                hourIntegerMap.put(hourStr,next.getValue().intValue());
                map.put(hourArr[0],hourIntegerMap);
            }
        }
        return map;
    }

    public static JavaPairRDD<String, String> actionTimeFullRDD(JavaRDD<Tuple2<String, String>> sessionIdFullRDD){
        return sessionIdFullRDD.mapPartitionsToPair(new PairFlatMapFunction<Iterator<Tuple2<String, String>>, String, String>() {
            @Override
            public Iterator<Tuple2<String, String>> call(Iterator<Tuple2<String, String>> tuple2Iterator) throws Exception {
                List<Tuple2<String, String>> list=new ArrayList<>();
                while (tuple2Iterator.hasNext()){
                    Tuple2<String, String> next = tuple2Iterator.next();
                    JSONObject jsonObject=JSONObject.parseObject(next._2);
                    String startTime = jsonObject.getString("startTime");
                    String hourStr=TimeUtils.parseToFormatTime(TimeUtils.parseToDate(startTime,TimeUtils.SECONDSTR),TimeUtils.HOURSTR);
                    list.add(new Tuple2<String, String>(hourStr,next._2));
                }
                return list.iterator();
            }
        });

    }
}
