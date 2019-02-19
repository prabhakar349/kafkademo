package com.pd.kakademo.kafkademo;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ApplicationListener;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.event.KafkaEvent;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.messaging.Message;
import org.springframework.messaging.support.GenericMessage;

@SpringBootApplication
public class KafkademoApplication {

	public static void main(String[] args) {
		SpringApplication.run(KafkademoApplication.class, args);
	}

	@Bean
	public ApplicationRunner runner(KafkaListenerEndpointRegistry registry,
		KafkaTemplate<String, Object> kafkaTemplate) {
		return args -> {
			Map<String, Object> headers = new HashMap<>();
			headers.put("kafka_topic", "foo.create");
			Foo foo = new Foo();
			foo.setAge(35);
			foo.setName("Pd");
			Message<Foo> message = new GenericMessage<>(foo, headers);
			kafkaTemplate.send( message );
		};
	}

	@KafkaListener( id="test", topics = "foo.create")
	public void listen(List<Foo> in, Acknowledgment acknowledgment) {
		System.out.println("KafkaMessage"+ in.get(0).toString());
		acknowledgment.acknowledge();
	}

	@Bean
	public NewTopic topic() {
		return new NewTopic("foo.create", 2, (short) 1);
	}



}

