package com.example.springbootKafkademo;

import java.util.concurrent.CountDownLatch;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

/**
 * @Author:lhz
 * @Description:
 * @Date:14:33 2018-6-4
 */
 //@Component
public class KafkaLister {

  private CountDownLatch latch = new CountDownLatch(1);

  public CountDownLatch getLatch() {
    return latch;
  }

  @KafkaListener(topicPattern = "e.*")
  public  void recetive(ConsumerRecord<String, String> record)
  {
    System.out.printf("threadname=%s,offset = %d, key = %s, value = %s,topic=%s,parttion=%s", Thread.currentThread().getName(),record.offset(), record.key(),
        record.value(),record.topic(),record.partition());
    System.out.println();
    latch.countDown();
  }



}
