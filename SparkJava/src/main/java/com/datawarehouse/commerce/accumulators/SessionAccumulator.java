package com.datawarehouse.commerce.accumulators;

import org.apache.spark.util.AccumulatorV2;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

public class SessionAccumulator extends AccumulatorV2<String,Map<String,Integer>> {

    private Map<String,Integer> map=new HashMap<>();

    @Override
    public boolean isZero() {
        return map.isEmpty();
    }

    @Override
    public AccumulatorV2<String, Map<String, Integer>> copy() {
        SessionAccumulator sessionAccumulator=new SessionAccumulator();
        synchronized (SessionAccumulator.class){
            sessionAccumulator.map.putAll(this.map);
        }
        return sessionAccumulator;
    }

    @Override
    public void reset() {
        this.map.clear();
    }

    @Override
    public void add(String v) {
        if(!this.map.containsKey(v)){
            this.map.put(v,1);
        }else {
            this.map.put(v,this.map.get(v)+1);
        }
    }

    @Override
    public void merge(AccumulatorV2<String, Map<String, Integer>> other) {
        if(other instanceof SessionAccumulator){
            Map<String,Integer> ortherMap=((SessionAccumulator) other).map;
            Iterator<Map.Entry<String, Integer>> iterator = ortherMap.entrySet().iterator();
            while (iterator.hasNext()){
                Map.Entry<String, Integer> next = iterator.next();
                if(this.map.containsKey(next.getKey())){
                    Integer old_v=this.map.get(next.getKey());
                    this.map.put(next.getKey(),old_v+next.getValue());
                }else {
                    this.map.put(next.getKey(),next.getValue());
                }
            }
        }
    }

    @Override
    public Map<String, Integer> value() {
        return this.map;
    }
}
