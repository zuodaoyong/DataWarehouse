package com.datawarehouse.kafka;

import java.util.Properties;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;



public class KafkaProducerService {

	private final KafkaProducer<Integer, String> producer;
    private String topic;
    private final Boolean isAsync;

    public KafkaProducerService(String bootstrapServers,String topic, Boolean isAsync) {
        Properties props = new Properties();
        props.put("bootstrap.servers", bootstrapServers);
        props.put("client.id", "DemoProducer");
        props.put("key.serializer", "org.apache.kafka.common.serialization.IntegerSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        producer = new KafkaProducer<Integer, String>(props);
        this.topic = topic;
        this.isAsync = isAsync;
    }
    
    public KafkaProducerService(String bootstrapServers,Boolean isAsync) {
        Properties props = new Properties();
        props.put("bootstrap.servers", bootstrapServers);
        props.put("client.id", "DemoProducer");
        props.put("key.serializer", "org.apache.kafka.common.serialization.IntegerSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        producer = new KafkaProducer<Integer, String>(props);
        this.isAsync = isAsync;
    }

    public void sendMessage(String topic,String message) {
        long startTime = System.currentTimeMillis();
        if (isAsync) { // Send asynchronously
            producer.send(new ProducerRecord<Integer, String>(topic,
            		message), new DemoCallBack(startTime, message));
        } else { // Send synchronously
            try {
                producer.send(new ProducerRecord<Integer, String>(topic,
                		message)).get();
                System.out.println("Sent message: ("+ message + ")");
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }
    
    public void sendMessage(String message) {
        long startTime = System.currentTimeMillis();
        if (isAsync) { // Send asynchronously
            producer.send(new ProducerRecord<Integer, String>(topic,
            		message), new DemoCallBack(startTime, message));
        } else { // Send synchronously
            try {
                producer.send(new ProducerRecord<Integer, String>(topic,
                		message)).get();
                System.out.println("Sent message: ("+ message + ")");
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }
    
    public void close(){
    	this.producer.close();
    }
}

class DemoCallBack implements Callback {

    private final long startTime;
    private final String message;

    public DemoCallBack(long startTime, String message) {
        this.startTime = startTime;
        this.message = message;
    }

    /**
     * A callback method the user can implement to provide asynchronous handling of request completion. This method will
     * be called when the record sent to the server has been acknowledged. Exactly one of the arguments will be
     * non-null.
     *
     * @param metadata  The metadata for the record that was sent (i.e. the partition and offset). Null if an error
     *                  occurred.
     * @param exception The exception thrown during processing of this record. Null if no error occurred.
     */
    public void onCompletion(RecordMetadata metadata, Exception exception) {
        long elapsedTime = System.currentTimeMillis() - startTime;
        if (metadata != null) {
            System.out.println(
                "message(" + message + ") sent to partition(" + metadata.partition() +
                    "), " +
                    "offset(" + metadata.offset() + ") in " + elapsedTime + " ms");
        } else {
            exception.printStackTrace();
        }
    }
}
