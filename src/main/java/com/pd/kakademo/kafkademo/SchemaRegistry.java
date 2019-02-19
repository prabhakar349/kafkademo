package com.pd.kakademo.kafkademo;

import static java.util.stream.Collectors.toMap;

import com.fasterxml.jackson.dataformat.avro.AvroSchema;
import java.io.InputStream;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import org.apache.avro.Schema;
import org.apache.commons.compress.compressors.FileNameUtil;
import org.apache.commons.io.FilenameUtils;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.io.Resource;
import org.springframework.stereotype.Component;
import org.springframework.util.Assert;

public class SchemaRegistry implements InitializingBean {

  @Value("${:classpath:avro/*.avsc}")
  private Resource[] schemas;

  private Map<String, AvroSchema> schemaRegistry;


  public AvroSchema getAvroSchema(String topic) {
    return Optional.ofNullable(schemaRegistry.get(topic))
      .orElseThrow(() -> new RuntimeException("SchemaNotAvailableForTopic"));
  }

  @Override
  public void afterPropertiesSet() throws Exception {
    schemaRegistry = Arrays.stream(schemas)
      .collect(toMap(schema -> FilenameUtils.getBaseName(schema.getFilename()),
        schema -> new AvroSchema(schemaFromResource(schema))));
    Assert.notEmpty(schemaRegistry, " schemaRegistry is empty");
  }

  private Schema schemaFromResource(Resource schemaResource) {
    try (InputStream stream = schemaResource.getInputStream()) {
      return new Schema.Parser().parse(schemaResource.getInputStream());
    } catch (Exception ex) {
      throw new RuntimeException("errorLoadingSchema");
    }
  }
}
