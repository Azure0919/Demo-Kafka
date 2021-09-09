package sx.nine.partition;

import java.util.Map;
import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;

/**
 * @author shaoxia.peng
 * @date 2021/9/8 21:13
 * @description
 * @since 1.0
 * @version 1.0
 */
public class SamplePartition implements Partitioner {


  /**
   * 数据分区原则
   * @param topic
   * @param key
   * @param bytes
   * @param value
   * @param valuebyte
   * @param cluster
   * @return
   */
  @Override
  public int partition(String topic, Object key, byte[] bytes, Object value, byte[] valuebyte,
      Cluster cluster) {
    String keystr = key+"";
    String keyInt = keystr.substring(3);
    System.out.println("keystr:"+keystr+",keyInt:"+keyInt);
    int i = Integer.parseInt(keyInt);
    return i%2;
  }

  @Override
  public void close() {

  }

  @Override
  public void configure(Map<String, ?> map) {

  }
}
