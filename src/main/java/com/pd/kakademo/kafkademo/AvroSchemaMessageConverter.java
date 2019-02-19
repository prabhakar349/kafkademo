package com.pd.kakademo.kafkademo;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.type.TypeFactory;
import com.fasterxml.jackson.dataformat.avro.AvroMapper;
import java.io.IOException;
import java.lang.reflect.Type;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.utils.Bytes;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.kafka.support.DefaultKafkaHeaderMapper;
import org.springframework.kafka.support.JacksonPresent;
import org.springframework.kafka.support.KafkaHeaderMapper;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.kafka.support.SimpleKafkaHeaderMapper;
import org.springframework.kafka.support.converter.ConversionException;
import org.springframework.kafka.support.converter.MessagingMessageConverter;
import org.springframework.kafka.support.converter.RecordMessageConverter;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageHeaders;
import org.springframework.util.Assert;

public class AvroSchemaMessageConverter extends MessagingMessageConverter {

  private AvroMapper avroMapper;
  private SchemaRegistry schemaRegistry;
  private KafkaHeaderMapper headerMapper;


  public AvroSchemaMessageConverter(AvroMapper avroMapper, SchemaRegistry schemaRegistry) {
    this.avroMapper = avroMapper;
    this.schemaRegistry = schemaRegistry;
    if (JacksonPresent.isJackson2Present()) {
      this.headerMapper = new DefaultKafkaHeaderMapper();
    } else {
      this.headerMapper = new SimpleKafkaHeaderMapper();
    }
  }

  @Override
  protected Object extractAndConvertValue(ConsumerRecord<?, ?> record, Type type) {
    System.out.printf(record.value().getClass().getName());
    ByteBuffer buffer = ByteBuffer.wrap((byte[])record.value());
    JavaType javaType = TypeFactory.defaultInstance().constructType(type);
    try {
      return avroMapper.readerFor(javaType).with(schemaRegistry.getAvroSchema(record.topic()))
        .readValue(buffer.array(), buffer.arrayOffset(), buffer.limit());
    } catch (IOException e) {
      throw new ConversionException("Failed to convert AvroMessage", e);
    }
  }

  @Override
  public ProducerRecord<?, ?> fromMessage(Message<?> message, String defaultTopic) {
    MessageHeaders headers = message.getHeaders();
    Object topicHeader = headers.get(KafkaHeaders.TOPIC);
    String topic = null;
    if (topicHeader instanceof byte[]) {
      topic = new String(((byte[]) topicHeader), StandardCharsets.UTF_8);
    } else if (topicHeader instanceof String) {
      topic = (String) topicHeader;
    } else if (topicHeader == null) {
      Assert.state(defaultTopic != null, "With no topic header, a defaultTopic is required");
    } else {
      throw new IllegalStateException(KafkaHeaders.TOPIC + " must be a String or byte[], not "
        + topicHeader.getClass());
    }
    String actualTopic = topic == null ? defaultTopic : topic;
    Integer partition = headers.get(KafkaHeaders.PARTITION_ID, Integer.class);
    Object key = headers.get(KafkaHeaders.MESSAGE_KEY);
    Object payload = convertPayload(message, topic);
    Long timestamp = headers.get(KafkaHeaders.TIMESTAMP, Long.class);
    Headers recordHeaders = initialRecordHeaders(message);
    if (this.headerMapper != null) {
      this.headerMapper.fromHeaders(headers, recordHeaders);
    }
    return new ProducerRecord(topic == null ? defaultTopic : topic, partition, timestamp, key,
      payload,
      recordHeaders);
  }

  protected Object convertPayload(Message<?> message, String topic) {
    try {
      return avroMapper.writer(schemaRegistry.getAvroSchema(topic))
        .writeValueAsBytes(message.getPayload());
    } catch (JsonProcessingException e) {
      throw new ConversionException("Failed to convert object to AvroMessage", e);
    }
  }
}
