package com.datawarehouse;

import java.util.Properties;

public class KafkaProperties {
    public static Properties getProperties(){
        Properties prop = new Properties();
        prop.setProperty("bootstrap.servers", "192.168.1.110:9092,192.168.1.111:9092,192.168.1.112:9092");
        prop.setProperty("group.id", "DemoProducer");
        return prop;
    }
}
