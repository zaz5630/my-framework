package cn.azzhu.flink.test;

/**
 * @author taochanglian
 * @since 2020-07-31 16:09
 */


import cn.azzhu.flink.test.entity.StatusKeyValueMap;
import cn.azzhu.flink.test.entity.TestRecord;
import com.alibaba.fastjson.JSON;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.Random;

/**
 * 本地测试，推送简单的json发送到本机的kafka集群，方便测试
 */
public class TestKafkaProducer {

    public static void main(String[] args) throws Exception{
        Properties properties = new Properties();
        properties.put("bootstrap.servers", ",node01:9092,node02:9092,node03:9092");
        properties.put("acks", "1");
        properties.put("retries", 0);
        //缓冲区的大小  //默认32M
        properties.put("buffer.memory", 33554432);
        //批处理数据的大小，每次写入多少数据到topic   //默认16KB
        properties.put("batch.size", 16384);
        //可以延长多久发送数据   //默认为0 表示不等待 ，立即发送
        properties.put("linger.ms", 1);
        //指定key和value的序列化器
        properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        Producer<String, String> producer = new KafkaProducer<String, String>(properties);
        final List<Integer> numList = Arrays.asList(100, 200, 300, 400);
        final List<String> vinList = Arrays.asList("20200712082200001", "20200712082200002", "20200712082200003", "20200712082200004");

        final List<String> f1List = Arrays.asList("stop", "start", "drivering", "push");
        final List<Integer> f2List = Arrays.asList(10, 20, 30, 40);
        final List<Double> f3List = Arrays.asList(Double.parseDouble("111.111"), Double.parseDouble("222.222"), Double.parseDouble("333.333"), Double.parseDouble("444.444"));

//        for (int i = 0; i < 100; i++) {
//            producer.send(new ProducerRecord<String, String>("test", Integer.toString(i), "hello-kafka-" + i));
//        }
//        producer.close();
        while (true) {
            Random random = new Random();
            StatusKeyValueMap statusKeyValueMap = new StatusKeyValueMap(f1List.get(random.nextInt(4)),f2List.get(random.nextInt(4)),f3List.get(random.nextInt(4)));
            TestRecord testRecord = new TestRecord(numList.get(random.nextInt(4)),System.currentTimeMillis(),vinList.get(random.nextInt(4)),statusKeyValueMap);
            final String value = JSON.toJSONString(testRecord);
            producer.send(new ProducerRecord<String, String>("flink_test", testRecord.getVin(), value));
            System.out.println(value);
            Thread.sleep(1000);
        }
    }
}
