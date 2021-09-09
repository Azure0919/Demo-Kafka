package sx.nine.consumer;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Properties;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

/**
 * @author shaoxia.peng
 * @date 2021/9/9 11:29
 * @description
 * @since 1.0
 * @version 1.0
 */
public class KafkaConsumerSample {
  //topicname
  private final static String TOPIC_NAME="shaoxia.peng001";

  public static void main(String[] args) {
    helloConsumer();
  }


  /***
   * 自动提交offset
   * @author psx
   * @date 2021/9/9 14:11
   * @param
   * @return void
   * @throws
  */
  private static void helloConsumer(){
    Properties properties = new Properties();
    properties.setProperty("bootstrap.servers","*****");
    properties.setProperty("group.id","shaoxia.peng001");
    properties.setProperty("enable.auto.commit","true");
    properties.setProperty("auto.commit.interval.ms","1000");
    properties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
    properties.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
    KafkaConsumer<String,String> kafkaConsumer = new KafkaConsumer<>(properties);
    kafkaConsumer.subscribe(Arrays.asList(TOPIC_NAME));
    while (true){
      ConsumerRecords<String, String> records = kafkaConsumer.poll(10000);
      for (ConsumerRecord<String, String> record : records) {
        System.out.printf("patition = %d , offset = %d, key = %s, value = %s%n",
            record.partition(),record.offset(), record.key(), record.value());
      }
    }

  }

  /***
   * 手动提交offser
   * @author psx
   * @date 2021/9/9 14:11
   * @param
   * @return void
   * @throws
  */
  private static void coomitOffset(){
    Properties properties = new Properties();
    //指定kafka服务
    properties.setProperty("bootstrap.servers","*****");
    //指定消费组
    properties.setProperty("group.id","shaoxia.peng");
    //自动提交关闭
    properties.setProperty("enable.auto.commit","false");
    properties.setProperty("auto.commit.interval.ms","1000");
    //反序列化
    properties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
    properties.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
    KafkaConsumer<String,String> kafkaConsumer = new KafkaConsumer<>(properties);
    //订阅消费topic组
    kafkaConsumer.subscribe(Arrays.asList(TOPIC_NAME));
    while (true){
      ConsumerRecords<String, String> records = kafkaConsumer.poll(10000);
      for (ConsumerRecord<String, String> record : records) {
        System.out.printf("patition = %d , offset = %d, key = %s, value = %s%n",
            record.partition(),record.offset(), record.key(), record.value());
        // TODO: 2021/9/9 保存db 
        // TODO: 2021/9/9 失败则回滚不要提交offset
      }
      //手动提交offset
      kafkaConsumer.commitSync();
    }

  }

  /***
   * Partition的消费控制
   * @author psx
   * @date 2021/9/9 14:34
   * @param
   * @return void
   * @throws
  */
  private static void coomitOffsetWithPartition(){
    Properties properties = new Properties();
    //指定kafka服务
    properties.setProperty("bootstrap.servers","*****");
    //指定消费组
    properties.setProperty("group.id","shaoxia.peng");
    //自动提交关闭
    properties.setProperty("enable.auto.commit","false");
    properties.setProperty("auto.commit.interval.ms","1000");
    //反序列化
    properties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
    properties.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
    KafkaConsumer<String,String> kafkaConsumer = new KafkaConsumer<>(properties);
    //订阅消费topic组
    kafkaConsumer.subscribe(Arrays.asList(TOPIC_NAME));
    while (true){
      ConsumerRecords<String, String> records = kafkaConsumer.poll(10000);
      //partition单独处理
      for (TopicPartition partition : records.partitions()) {
        List<ConsumerRecord<String, String>> recordsP = records.records(partition);
        //消费每个partition的record
        for (ConsumerRecord<String, String> record : recordsP) {
          System.out.printf("patition = %d , offset = %d, key = %s, value = %s%n",
              record.partition(),record.offset(), record.key(), record.value());
        }
        //针对partition手动提交offset
        long offsetLast = recordsP.get(recordsP.size() - 1).offset();
        HashMap<TopicPartition, OffsetAndMetadata> offset = new HashMap<>();
        offset.put(partition,new OffsetAndMetadata(offsetLast+1));
        kafkaConsumer.commitSync(offset);
        System.out.println("========partition:"+partition);
      }


    }

  }

  /***
   * 根据Partition的拉取与消费控制
   * @author psx
   * @date 2021/9/9 16:17
   * @param
   * @return void
   * @throws
  */
  private static void coomitOffsetWithPartition2(){
    Properties properties = new Properties();
    //指定kafka服务
    properties.setProperty("bootstrap.servers","*****");
    //指定消费组
    properties.setProperty("group.id","shaoxia.peng");
    //自动提交关闭
    properties.setProperty("enable.auto.commit","false");
    properties.setProperty("auto.commit.interval.ms","1000");
    //反序列化
    properties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
    properties.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
    KafkaConsumer<String,String> kafkaConsumer = new KafkaConsumer<>(properties);

    TopicPartition topicPartition0 = new TopicPartition(TOPIC_NAME,0);
    TopicPartition topicPartition1 = new TopicPartition(TOPIC_NAME,1);
    //订阅消费topic组
    //kafkaConsumer.subscribe(Arrays.asList(TOPIC_NAME));
    //订阅消费topic组的某个topicPartition
    kafkaConsumer.assign(Arrays.asList(topicPartition0));

    kafkaConsumer.subscribe(Arrays.asList(TOPIC_NAME));
    while (true){
      ConsumerRecords<String, String> records = kafkaConsumer.poll(10000);
      //partition单独处理
      for (TopicPartition partition : records.partitions()) {
        List<ConsumerRecord<String, String>> recordsP = records.records(partition);
        //消费每个partition的record
        for (ConsumerRecord<String, String> record : recordsP) {
          System.out.printf("patition = %d , offset = %d, key = %s, value = %s%n",
              record.partition(),record.offset(), record.key(), record.value());
        }
        //针对partition手动提交offset
        long offsetLast = recordsP.get(recordsP.size() - 1).offset();
        HashMap<TopicPartition, OffsetAndMetadata> offset = new HashMap<>();
        offset.put(partition,new OffsetAndMetadata(offsetLast+1));
        kafkaConsumer.commitSync(offset);
        System.out.println("========partition:"+partition);
      }


    }

  }

}
