package com.github.jcustenborder.kafka.connect.archive;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.transforms.Transformation;

import java.util.HashMap;
import java.util.Map;

public class Archive<R extends ConnectRecord<R>> implements Transformation<R> {
	@Override
	public R apply(R r) {
		if (r.valueSchema() == null) {
			return applySchemaless(r);
		} else {
			return applyWithSchema(r);
		}
	}

	private R applyWithSchema(R r) {
		final Schema schema = SchemaBuilder.struct().name("com.github.jcustenborder.kafka.connect.archive.Storage")
				.field("key", r.keySchema()).field("partition", Schema.INT64_SCHEMA).field("value", r.valueSchema())
				.field("topic", Schema.STRING_SCHEMA).field("headers", Schema.STRING_SCHEMA)
				.field("timestamp", Schema.INT64_SCHEMA);
		Struct value = new Struct(schema).put("key", r.key()).put("partition", r.kafkaPartition())
				.put("value", r.value()).put("topic", r.topic()).put("timestamp", r.timestamp());
		return r.newRecord(r.topic(), r.kafkaPartition(), r.keySchema(), r.key(), schema, value, r.timestamp());
	}

	@SuppressWarnings("unchecked")
	private R applySchemaless(R r) {

		final Map<String, Object> archiveValue = new HashMap<>();

		final Map<String, Object> value = (Map<String, Object>) r.value();

		archiveValue.put("key", r.key());
		archiveValue.put("value", value);
		archiveValue.put("topic", r.topic());
		archiveValue.put("partition", r.kafkaPartition());
		archiveValue.put("timestamp", r.timestamp());

		return r.newRecord(r.topic(), r.kafkaPartition(), null, r.key(), null, archiveValue, r.timestamp());
	}

	@Override
	public ConfigDef config() {
		return new ConfigDef();
	}

	@Override
	public void close() {

	}

	@Override
	public void configure(Map<String, ?> map) {

	}
}