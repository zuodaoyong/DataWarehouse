package com.datawarehouse.commerce;

import com.alibaba.fastjson.JSONObject;
import com.datawarehouse.commerce.bean.UserVisitAction;
import com.datawarehouse.commerce.sorts.CategoryKey;
import com.datawarehouse.config.InitSpark;
import org.apache.commons.lang3.StringUtils;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.Optional;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * 点击品类top N
 */
public class CategoryTopNApplication {

    public static void main(String[] args) {
        InitSpark initSpark = new InitSpark();
        SparkSession sparkSession = initSpark.getSparkSession(CategoryTopNApplication.class.getSimpleName(), 1);
        JavaRDD<UserVisitAction> userVisitActionRDD = SessionApplication.getUserVisitActionRDD(sparkSession);
        JavaPairRDD<String, Iterable<UserVisitAction>> sessionIdUserVisitActionRdd = sessionIdUserVisitActionRdd(userVisitActionRDD);
        JavaPairRDD<Long, Long> categoryPairRDD = categoryPairRDD(sessionIdUserVisitActionRdd);
        categoryPairRDD=categoryPairRDD.distinct();
        sessionIdUserVisitActionRdd.cache();
        JavaPairRDD<Long, Integer> clickCategoryNum = clickCategoryNum(sessionIdUserVisitActionRdd);
        JavaPairRDD<Long, Integer> orderCategoryNum = orderCategoryNum(sessionIdUserVisitActionRdd);
        JavaPairRDD<Long, Integer> payCategoryNum = payCategoryNum(sessionIdUserVisitActionRdd);
        JavaPairRDD<Long, String> category_click = category_click(categoryPairRDD, clickCategoryNum);
        JavaPairRDD<Long, String> category_click_order = category_click_order(category_click, orderCategoryNum);
        JavaPairRDD<Long, String> category_click_order_pay = category_click_order_pay(category_click_order, payCategoryNum);

        JavaPairRDD<CategoryKey, String> category_info = category_info(category_click_order_pay);
        JavaPairRDD<CategoryKey, String> categoryKeySortRDD = category_info.sortByKey(false);
        categoryKeySortRDD.foreach(new VoidFunction<Tuple2<CategoryKey, String>>() {
            @Override
            public void call(Tuple2<CategoryKey, String> categoryKeyStringTuple2) throws Exception {
                System.out.println(categoryKeyStringTuple2);
            }
        });

        List<Tuple2<CategoryKey, String>> take = categoryKeySortRDD.take(10);

        System.out.println("-----------------------------");
        for(Tuple2<CategoryKey, String> tuple2:take){
            System.out.println(tuple2);
        }


    }

    private static JavaPairRDD<CategoryKey,String> category_info(JavaPairRDD<Long,String> category_click_order_pay){
        return category_click_order_pay.mapPartitionsToPair(new PairFlatMapFunction<Iterator<Tuple2<Long, String>>, CategoryKey, String>() {
            @Override
            public Iterator<Tuple2<CategoryKey, String>> call(Iterator<Tuple2<Long, String>> tuple2Iterator) throws Exception {
                List<Tuple2<CategoryKey, String>> list=new ArrayList<>();
                while (tuple2Iterator.hasNext()){
                    Tuple2<Long, String> next = tuple2Iterator.next();
                    JSONObject jsonObject=JSONObject.parseObject(next._2);
                    Integer click = jsonObject.getInteger("click");
                    Integer order = jsonObject.getInteger("order");
                    Integer pay = jsonObject.getInteger("pay");
                    CategoryKey categoryKey=new CategoryKey(next._1,click,order,pay);
                    list.add(new Tuple2<>(categoryKey,next._2));
                }
                return list.iterator();
            }
        });
    }

    private static JavaPairRDD<Long,String> category_click_order_pay(JavaPairRDD<Long, String> category_click_order,JavaPairRDD<Long, Integer> payCategoryNum){
        return category_click_order.leftOuterJoin(payCategoryNum).mapPartitionsToPair(new PairFlatMapFunction<Iterator<Tuple2<Long, Tuple2<String, Optional<Integer>>>>, Long, String>() {
            @Override
            public Iterator<Tuple2<Long, String>> call(Iterator<Tuple2<Long, Tuple2<String, Optional<Integer>>>> tuple2Iterator) throws Exception {
                List<Tuple2<Long, String>> list=new ArrayList<>();
                while (tuple2Iterator.hasNext()){
                    Tuple2<Long, Tuple2<String, Optional<Integer>>> next = tuple2Iterator.next();
                    Tuple2<String, Optional<Integer>> stringOptionalTuple2 = next._2;
                    Optional<Integer> integerOptional = stringOptionalTuple2._2;
                    int v=0;
                    if(integerOptional.isPresent()){
                        v=integerOptional.get();
                    }
                    JSONObject jsonObject = JSONObject.parseObject(stringOptionalTuple2._1);
                    jsonObject.put("pay",v);
                    list.add(new Tuple2<>(next._1,jsonObject.toJSONString()));
                }
                return list.iterator();
            }
        });
    }

    private static JavaPairRDD<Long,String> category_click_order(JavaPairRDD<Long, String> category_click,JavaPairRDD<Long, Integer> orderCategoryNum){
        return category_click.leftOuterJoin(orderCategoryNum).mapPartitionsToPair(new PairFlatMapFunction<Iterator<Tuple2<Long, Tuple2<String, Optional<Integer>>>>, Long, String>() {
            @Override
            public Iterator<Tuple2<Long, String>> call(Iterator<Tuple2<Long, Tuple2<String, Optional<Integer>>>> tuple2Iterator) throws Exception {
                List<Tuple2<Long, String>> list=new ArrayList<>();
                while (tuple2Iterator.hasNext()){
                    Tuple2<Long, Tuple2<String, Optional<Integer>>> next = tuple2Iterator.next();
                    Tuple2<String, Optional<Integer>> stringOptionalTuple2 = next._2;
                    Optional<Integer> integerOptional = stringOptionalTuple2._2;
                    int v=0;
                    if(integerOptional.isPresent()){
                        v=integerOptional.get();
                    }
                    JSONObject jsonObject = JSONObject.parseObject(stringOptionalTuple2._1);
                    jsonObject.put("order",v);
                    list.add(new Tuple2<>(next._1,jsonObject.toJSONString()));
                }
                return list.iterator();
            }
        });
    }

    private static JavaPairRDD<Long,String> category_click(JavaPairRDD<Long, Long> categoryPairRDD,JavaPairRDD<Long, Integer> clickCategoryNum){
        return categoryPairRDD.leftOuterJoin(clickCategoryNum).mapPartitionsToPair(new PairFlatMapFunction<Iterator<Tuple2<Long, Tuple2<Long, Optional<Integer>>>>, Long, String>() {
            @Override
            public Iterator<Tuple2<Long, String>> call(Iterator<Tuple2<Long, Tuple2<Long, Optional<Integer>>>> tuple2Iterator) throws Exception {
                List<Tuple2<Long, String>> list=new ArrayList<>();
                while (tuple2Iterator.hasNext()){
                    Tuple2<Long, Tuple2<Long, Optional<Integer>>> next = tuple2Iterator.next();
                    Tuple2<Long, Optional<Integer>> longOptionalTuple2 = next._2();
                    Optional<Integer> integerOptional = longOptionalTuple2._2;
                    Integer v=0;
                    if(integerOptional.isPresent()){
                        v=integerOptional.get();
                    }
                    JSONObject jsonObject=new JSONObject();
                    jsonObject.put("click",v);
                    list.add(new Tuple2<>(next._1,jsonObject.toJSONString()));
                }
                return list.iterator();
            }
        });
    }

    private static JavaPairRDD<Long,Integer> payCategoryNum(JavaPairRDD<String, Iterable<UserVisitAction>> sessionIdUserVisitActionRdd){
        JavaPairRDD<Long, Integer> longIntegerJavaPairRDD = sessionIdUserVisitActionRdd.mapPartitionsToPair(new PairFlatMapFunction<Iterator<Tuple2<String, Iterable<UserVisitAction>>>, Long, Integer>() {
            @Override
            public Iterator<Tuple2<Long, Integer>> call(Iterator<Tuple2<String, Iterable<UserVisitAction>>> tuple2Iterator) throws Exception {
                List<Tuple2<Long, Integer>> list = new ArrayList<>();
                while (tuple2Iterator.hasNext()) {
                    Tuple2<String, Iterable<UserVisitAction>> next = tuple2Iterator.next();
                    Iterator<UserVisitAction> iterator = next._2().iterator();
                    while (iterator.hasNext()) {
                        UserVisitAction next1 = iterator.next();
                        if (StringUtils.isNotEmpty(next1.getPay_category_ids())) {
                            String[] split = next1.getPay_category_ids().split(",");
                            for (String str : split) {
                                list.add(new Tuple2<>(Long.valueOf(str), 1));
                            }
                        }
                    }
                }
                return list.iterator();
            }
        });
        return longIntegerJavaPairRDD.reduceByKey(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer integer, Integer integer2) throws Exception {
                return integer+integer2;
            }
        });
    }

    private static JavaPairRDD<Long,Integer> orderCategoryNum(JavaPairRDD<String, Iterable<UserVisitAction>> sessionIdUserVisitActionRdd){
        JavaPairRDD<Long, Integer> longIntegerJavaPairRDD = sessionIdUserVisitActionRdd.mapPartitionsToPair(new PairFlatMapFunction<Iterator<Tuple2<String, Iterable<UserVisitAction>>>, Long, Integer>() {
            @Override
            public Iterator<Tuple2<Long, Integer>> call(Iterator<Tuple2<String, Iterable<UserVisitAction>>> tuple2Iterator) throws Exception {
                List<Tuple2<Long, Integer>> list = new ArrayList<>();
                while (tuple2Iterator.hasNext()) {
                    Tuple2<String, Iterable<UserVisitAction>> next = tuple2Iterator.next();
                    Iterator<UserVisitAction> iterator = next._2().iterator();
                    while (iterator.hasNext()) {
                        UserVisitAction next1 = iterator.next();
                        if (StringUtils.isNotEmpty(next1.getOrder_category_ids())) {
                            String[] split = next1.getOrder_category_ids().split(",");
                            for (String str : split) {
                                list.add(new Tuple2<>(Long.valueOf(str), 1));
                            }
                        }
                    }
                }
                return list.iterator();
            }
        });
        return longIntegerJavaPairRDD.reduceByKey(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer integer, Integer integer2) throws Exception {
                return integer+integer2;
            }
        });
    }

    private static JavaPairRDD<Long,Integer> clickCategoryNum(JavaPairRDD<String, Iterable<UserVisitAction>> sessionIdUserVisitActionRdd){
        JavaPairRDD<Long, Integer> longIntegerJavaPairRDD = sessionIdUserVisitActionRdd.mapPartitionsToPair(new PairFlatMapFunction<Iterator<Tuple2<String, Iterable<UserVisitAction>>>, Long, Integer>() {
            @Override
            public Iterator<Tuple2<Long, Integer>> call(Iterator<Tuple2<String, Iterable<UserVisitAction>>> tuple2Iterator) throws Exception {
                List<Tuple2<Long, Integer>> list = new ArrayList<>();
                while (tuple2Iterator.hasNext()) {
                    Tuple2<String, Iterable<UserVisitAction>> next = tuple2Iterator.next();
                    Iterator<UserVisitAction> iterator = next._2.iterator();
                    while (iterator.hasNext()) {
                        UserVisitAction next1 = iterator.next();
                        if (next1.getClick_category_id() != null && next1.getClick_category_id() != -1) {
                            list.add(new Tuple2<Long, Integer>(next1.getClick_category_id(), 1));
                        }
                    }
                }
                return list.iterator();
            }
        });
        return longIntegerJavaPairRDD.reduceByKey(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer integer, Integer integer2) throws Exception {
                return integer+integer2;
            }
        });
    }

    private static JavaPairRDD<Long,Long> categoryPairRDD(JavaPairRDD<String, Iterable<UserVisitAction>> sessionIdUserVisitActionRdd){

        return sessionIdUserVisitActionRdd.mapPartitionsToPair(new PairFlatMapFunction<Iterator<Tuple2<String, Iterable<UserVisitAction>>>, Long, Long>() {
            @Override
            public Iterator<Tuple2<Long, Long>> call(Iterator<Tuple2<String, Iterable<UserVisitAction>>> tuple2Iterator) throws Exception {
                List<Tuple2<Long, Long>> list = new ArrayList<>();
                while (tuple2Iterator.hasNext()) {
                    Tuple2<String, Iterable<UserVisitAction>> next = tuple2Iterator.next();
                    Iterable<UserVisitAction> userVisitActions = next._2;
                    Iterator<UserVisitAction> iterator = userVisitActions.iterator();
                    while (iterator.hasNext()) {
                        UserVisitAction userVisitAction = iterator.next();
                        if (userVisitAction.getClick_category_id() != null && userVisitAction.getClick_category_id() != -1) {
                            list.add(new Tuple2<>(userVisitAction.getClick_category_id(), userVisitAction.getClick_category_id()));
                        } else if (StringUtils.isNotEmpty(userVisitAction.getOrder_category_ids())) {
                            String[] split = userVisitAction.getOrder_category_ids().split(",");
                            for (String s : split) {
                                list.add(new Tuple2<>(Long.valueOf(s), Long.valueOf(s)));
                            }
                        } else if (StringUtils.isNotEmpty(userVisitAction.getPay_category_ids())) {
                            String[] split = userVisitAction.getPay_category_ids().split(",");
                            for (String s : split) {
                                list.add(new Tuple2<>(Long.valueOf(s), Long.valueOf(s)));
                            }
                        }
                    }
                }
                return list.iterator();
            }
        });
    }

    private static JavaPairRDD<String, Iterable<UserVisitAction>> sessionIdUserVisitActionRdd(JavaRDD<UserVisitAction> userVisitActionRDD){
        JavaPairRDD<String, UserVisitAction> sessionUserAction = userVisitActionRDD.mapPartitionsToPair(new PairFlatMapFunction<Iterator<UserVisitAction>, String, UserVisitAction>() {
            @Override
            public Iterator<Tuple2<String, UserVisitAction>> call(Iterator<UserVisitAction> userVisitActionIterator) throws Exception {
                List<Tuple2<String, UserVisitAction>> list = new ArrayList<>();
                while (userVisitActionIterator.hasNext()) {
                    UserVisitAction next = userVisitActionIterator.next();
                    list.add(new Tuple2<>(next.getSession_id(), next));
                }
                return list.iterator();
            }
        });
        return sessionUserAction.groupByKey();
    }
}
