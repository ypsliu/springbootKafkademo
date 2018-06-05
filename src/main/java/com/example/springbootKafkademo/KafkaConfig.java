package com.example.springbootKafkademo;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.config.KafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer;
import org.springframework.kafka.listener.ConsumerAwareRebalanceListener;
import org.springframework.kafka.listener.KafkaMessageListenerContainer;
import org.springframework.kafka.listener.MessageListener;
import org.springframework.kafka.listener.config.ContainerProperties;
import org.springframework.kafka.support.Acknowledgment;

/**
 * @Author:lhz
 * @Description:
 * @Date:16:21 2018-6-4
 */
@Configuration
@EnableKafka
public class KafkaConfig {

  @Bean
  KafkaListenerContainerFactory<ConcurrentMessageListenerContainer<String, String>>
  kafkaListenerContainerFactory() {
    ConcurrentKafkaListenerContainerFactory<String, String> factory =
        new ConcurrentKafkaListenerContainerFactory<>();
    factory.setConsumerFactory(consumerFactory());
    factory.setConcurrency(3);  //创建3个消费者
    factory.getContainerProperties().setPollTimeout(3000);
    factory.getContainerProperties().setConsumerRebalanceListener(new ConsumerAwareRebalanceListener() {

      @Override
      public void onPartitionsAssigned(Consumer<?, ?> consumer,
          Collection<TopicPartition> partitions) {

      }
      @Override
      public void onPartitionsRevokedBeforeCommit(Consumer<?, ?> consumer, Collection<TopicPartition> partitions) {
        // acknowledge any pending Acknowledgments (if using manual acks)
        System.out.print("PartitionsRevoked ...");
        consumer.commitAsync();
      }

      @Override
      public void onPartitionsAssigned(Collection<TopicPartition> partitions) {

        System.out.print("PartitionsAssigned ...");
      }
    });
    return factory;
  }

  @Bean
  public ConsumerFactory<String, String> consumerFactory() {
    return new DefaultKafkaConsumerFactory<>(consumerConfigs());
  }

  @Bean
  public Map<String, Object> consumerConfigs() {
    Map<String, Object> propsMap = new HashMap<>();
    propsMap.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
    //propsMap.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, enableAutoCommit);
    propsMap.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, 100);
    propsMap.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, 6000);
    propsMap.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
    propsMap.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
    propsMap.put(ConsumerConfig.GROUP_ID_CONFIG, "group_t");
    propsMap.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    return propsMap;
  }


  @Bean
  KafkaMessageListenerContainer creatMessageList()
  {
    ContainerProperties containerProps = new ContainerProperties("p1", "p2");
    containerProps.setMessageListener(new MessageListener<String, String>() {
      @Override
      public void onMessage(ConsumerRecord<String, String> record) {
        System.out.printf("threadname=%s,offset = %d, key = %s, value = %s,topic=%s,parttion=%s", Thread.currentThread().getName(),record.offset(), record.key(),
            record.value(),record.topic(),record.partition());
        System.out.println();
      }
      @Override
      public void onMessage(ConsumerRecord<String, String> data, Consumer<?, ?> consumer) {

      }
      @Override
      public void onMessage(ConsumerRecord<String, String> data, Acknowledgment acknowledgment,
          Consumer<?, ?> consumer) {

      }
    });
    KafkaMessageListenerContainer<String, String> container =
        new KafkaMessageListenerContainer<>(consumerFactory(), containerProps);   //创建一个消费者
    return container;
  }
}
