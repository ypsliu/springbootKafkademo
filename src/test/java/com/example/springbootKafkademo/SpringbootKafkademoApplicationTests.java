package com.example.springbootKafkademo;

import java.util.concurrent.TimeUnit;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

@RunWith(SpringRunner.class)
@SpringBootTest
public class SpringbootKafkademoApplicationTests {

	//@Autowired
	private KafkaLister  kafkaLister;
	@Test
	public void contextLoads() {

		try {
			kafkaLister.getLatch().await(10000, TimeUnit.SECONDS);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}

}
