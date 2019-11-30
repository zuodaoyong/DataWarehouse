package com.datawarehouse;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.fs.StringWriter;
import org.apache.flink.streaming.connectors.fs.bucketing.BucketingSink;
import org.apache.flink.streaming.connectors.fs.bucketing.DateTimeBucketer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer09;

import java.time.ZoneId;

public class KafkaToHdfs {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        System.setProperty("HADOOP_USER_NAME","root");
        ParameterTool params = ParameterTool.fromArgs(args);
        env.enableCheckpointing(30000L);

        FlinkKafkaConsumer09<String> topic_startConsumer = new FlinkKafkaConsumer09<>("topic_start", new SimpleStringSchema(), KafkaProperties.getProperties());
        FlinkKafkaConsumer09<String> topic_eventConsumer = new FlinkKafkaConsumer09<>("topic_event", new SimpleStringSchema(), KafkaProperties.getProperties());
        DataStreamSource<String> topic_startDataSource = env.addSource(topic_startConsumer);
        DataStreamSource<String> topic_eventDataSource = env.addSource(topic_eventConsumer);

        BucketingSink<String> topic_startSink = new BucketingSink<>("hdfs://master:9000/logs/topic_start");
        //创建一个按照时间创建目录的bucketer,默认是yyyy-MM-dd--HH，时区默认是美国时间。这里我都改了，一天创建一次目录，上海时间
        topic_startSink.setBucketer(new DateTimeBucketer<String>("yyyy-MM-dd", ZoneId.of("Asia/Shanghai")));
        //topic_startSink.setFSConfig()
        BucketingSink<String> topic_eventSink = new BucketingSink<>("hdfs://master:9000/logs/topic_event");
        //创建一个按照时间创建目录的bucketer,默认是yyyy-MM-dd--HH，时区默认是美国时间。这里我都改了，一天创建一次目录，上海时间
        topic_eventSink.setBucketer(new DateTimeBucketer<String>("yyyy-MM-dd", ZoneId.of("Asia/Shanghai")));
        /**
         * 设置大小和滚动时间，只要满足一个，就会滚动，和flume差不多
         */
        //设置每个文件的最大大小 ,默认是384M(1024 * 1024 * 384)
        topic_startSink.setBatchSize(1024 * 1024); //this is 1M
        //设置多少时间，就换一个文件写  但是ms  这里我设置的是 一个小时
        topic_startSink.setWriter(new StringWriter<>());
        topic_startSink.setBatchRolloverInterval(1000 * 60 * 10);
        topic_startDataSource.addSink(topic_startSink);

        topic_eventSink.setBatchSize(1024 * 1024);
        topic_eventSink.setWriter(new StringWriter<>());
        topic_eventSink.setBatchRolloverInterval(1000 * 60 * 10);
        topic_eventDataSource.addSink(topic_eventSink);

        env.execute(KafkaToHdfs.class.getSimpleName());
    }
}
