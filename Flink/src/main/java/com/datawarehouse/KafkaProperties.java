package com.datawarehouse;

import java.util.Properties;

public class KafkaProperties {
    public static Properties getProperties(){
        Properties prop = new Properties();
        prop.setProperty("bootstrap.servers", "192.168.234.130:9092,192.168.234.131:9092,192.168.234.132:9092");
        prop.setProperty("group.id", "DemoProducer");
        return prop;
    }
}
