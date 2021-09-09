package sx.nine.producer;

import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

/**
 * @author shaoxia.peng
 * @date 2021/9/6 20:19
 * @description 生产者API操作
 * @since 1.0
 * @version 1.0
 */
public class KafkaProducerSample {

  //topicname
  private final static String TOPIC_NAME="shaoxia.peng001";

  public static void main(String[] args) throws ExecutionException, InterruptedException {
    producerSend();
  }

  /**
   * 生产者异步发送
   */
  public static void producerSend() throws ExecutionException, InterruptedException {

    Properties properties = new Properties();
    properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"*****");
    properties.put(ProducerConfig.ACKS_CONFIG,"all");
    properties.put(ProducerConfig.RETRIES_CONFIG,"0");
    properties.put(ProducerConfig.BATCH_SIZE_CONFIG,"16384");
    properties.put(ProducerConfig.BUFFER_MEMORY_CONFIG,"33554432");
    properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.StringSerializer");
    properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.StringSerializer");
    //Producer的主对象
    KafkaProducer<String, String> producer = new KafkaProducer<>(properties);
    //Producer record消息对象
    for (int i = 1; i < 15; i++) {
      ProducerRecord producerRecord = new ProducerRecord<>(TOPIC_NAME, "key"+i, "v"+i);
      Future<RecordMetadata> send = producer.send(producerRecord);
      RecordMetadata recordMetadata = send.get();
      System.out.println(recordMetadata.partition());
      System.out.println(recordMetadata.topic());
      System.out.println(recordMetadata.offset());
    }
    producer.close();

  }

  public static void producerSendQWithCallBack() throws ExecutionException, InterruptedException {

    Properties properties = new Properties();
    properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"*****");
    properties.put(ProducerConfig.ACKS_CONFIG,"all");
    properties.put(ProducerConfig.RETRIES_CONFIG,"0");
    properties.put(ProducerConfig.BATCH_SIZE_CONFIG,"16384");
    properties.put(ProducerConfig.BUFFER_MEMORY_CONFIG,"33554432");
    properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.StringSerializer");
    properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.StringSerializer");
    //Producer的主对象
    KafkaProducer<String, String> producer = new KafkaProducer<>(properties);
    //Producer record消息对象
    for (int i = 11; i < 15; i++) {
      ProducerRecord producerRecord = new ProducerRecord<>(TOPIC_NAME, "key"+i, "v"+i);
      producer.send(producerRecord, new Callback() {
        @Override
        public void onCompletion(RecordMetadata recordMetadata, Exception e) {
          System.out.println(recordMetadata.partition());
          System.out.println(recordMetadata.topic());
          System.out.println(recordMetadata.offset());
        }
      });

    }
    producer.close();

  }

  public static void producerSendQWithCallBackWithPartition() throws ExecutionException, InterruptedException {

    Properties properties = new Properties();
    properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"*****");
    properties.put(ProducerConfig.ACKS_CONFIG,"all");
    properties.put(ProducerConfig.RETRIES_CONFIG,"0");
    properties.put(ProducerConfig.BATCH_SIZE_CONFIG,"16384");
    properties.put(ProducerConfig.BUFFER_MEMORY_CONFIG,"33554432");
    properties.put(ProducerConfig.PARTITIONER_CLASS_CONFIG,"sx.nine.partition.SamplePartition");
    properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.StringSerializer");
    properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.StringSerializer");
    //Producer的主对象
    KafkaProducer<String, String> producer = new KafkaProducer<>(properties);
    //Producer record消息对象
    for (int i = 11; i < 15; i++) {
      ProducerRecord producerRecord = new ProducerRecord<>(TOPIC_NAME, "key"+i, "v"+i);
      producer.send(producerRecord, new Callback() {
        @Override
        public void onCompletion(RecordMetadata recordMetadata, Exception e) {
          System.out.println(recordMetadata.partition());
          System.out.println(recordMetadata.topic());
          System.out.println(recordMetadata.offset());
        }
      });

    }
    producer.close();

  }



}
