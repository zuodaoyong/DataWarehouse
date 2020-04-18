package com.datawarehouse.config;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;

public class InitSpark {

    private SparkConf sparkConf;
    private SparkConf getSparkConf(String appName,int parallelism){
        System.setProperty("HADOOP_USER_NAME", "root");
        if(sparkConf==null){
            sparkConf=new SparkConf().setAppName(appName).setMaster("local[*]");
            sparkConf.set("spark.default.parallelism",parallelism+"");
            sparkConf.set("spark.sql.catalogImplementation","hive");
            sparkConf.set("hive.metastore.uris","thrift://master:9083");
            sparkConf.set("spark.sql.warehouse.dir","hdfs://master:9000/user/hive/warehouse");
            sparkConf.set("hive.metastore.warehouse.dir","hdfs://master:9000/user/hive/warehouse");
        }
        return sparkConf;
    }

    public SparkSession getSparkSession(String appName,int parallelism){
        sparkConf=getSparkConf(appName,parallelism);
        return SparkSession.builder().config(sparkConf)
                .enableHiveSupport().getOrCreate();
    }

    public JavaSparkContext getJavaSparkContext(String appName,int parallelism){
        sparkConf=getSparkConf(appName,parallelism);
        return new JavaSparkContext(sparkConf);
    }
}
