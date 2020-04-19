package com.datawarehouse.commerce.accumulators;

import org.apache.spark.util.AccumulatorV2;

public class CountAccumulator extends AccumulatorV2<Integer,Integer> {

    private Integer value=0;
    @Override
    public boolean isZero() {
        return true;
    }

    @Override
    public AccumulatorV2<Integer, Integer> copy() {
        CountAccumulator countAccumulator=new CountAccumulator();
        synchronized (CountAccumulator.class){
            countAccumulator.value=this.value;
        }
        return countAccumulator;
    }

    @Override
    public void reset() {

        this.value=0;
    }

    @Override
    public void add(Integer v) {
          this.value+=v;
    }

    @Override
    public void merge(AccumulatorV2<Integer, Integer> other) {
        if(other instanceof CountAccumulator){
            this.value+=((CountAccumulator) other).value;
        }
    }

    @Override
    public Integer value() {
        return this.value;
    }
}
