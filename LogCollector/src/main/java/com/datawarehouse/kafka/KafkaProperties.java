package com.datawarehouse.kafka;

public class KafkaProperties {

	public static final String TOPIC = "storm_test";
    public static final String KAFKA_SERVER_URL = "192.168.1.110:9092,192.168.1.111:9092,192.168.1.112:9092";
    public static final int KAFKA_PRODUCER_BUFFER_SIZE = 64 * 1024;
    public static final int CONNECTION_TIMEOUT = 100000;
    public static final String TOPIC2 = "topic2";
    public static final String TOPIC3 = "topic3";
    public static final String CLIENT_ID = "SimpleConsumerDemoClient";
    public static final String ZKQUORUM="192.168.1.110:2181,192.168.1.111:2181,192.168.1.112:2181";
    private KafkaProperties() {}
}
