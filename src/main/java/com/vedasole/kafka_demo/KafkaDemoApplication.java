package com.vedasole.kafka_demo;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.annotation.Scheduled;

@SpringBootApplication
@EnableKafka
@EnableScheduling
public class KafkaDemoApplication implements CommandLineRunner {

	@Autowired
	private KafkaTemplate<String, String> kafkaTemplate;

	public static void main(String[] args) {
		SpringApplication.run(KafkaDemoApplication.class, args);
	}

	@KafkaListener(topics = "test-topic", groupId = "group_id")
	public void listen(String message) {
		System.out.println("Received Message: " + message);
		// Process the message as needed
		// For example, you can save it to a database or perform some other action
		System.out.println("Message processed successfully.");
		// You can also add error handling and logging as needed
		System.out.println("Message processing completed.");
		System.out.println("--------------------------------------------------");
	}

	@Scheduled(fixedRate = 5000)
	public void processMessage() {
		// Implement your message processing logic here
		System.out.println("Processing message");
		kafkaTemplate.send("test-topic", "Scheduled message from Kafka!");
		System.out.println("Message processing completed.");
		System.out.println("--------------------------------------------------");
	}

	@Override
	public void run(String... args) {
		kafkaTemplate.send("test-topic", "Hello, Kafka from CommandLineRunner!");
	}
}