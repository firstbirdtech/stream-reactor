package com.github.jcustenborder.kafka.connect.archive;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.transforms.Transformation;

import java.util.Map;

public class UnArchive<R extends ConnectRecord<R>> implements Transformation<R> {
	@Override
	public R apply(R r) {
		return applySchemaless(r);
	}

	@SuppressWarnings("unchecked")
	private R applySchemaless(R r) {
		final Map<String, Object> value = (Map<String, Object>) r.value();
		return r.newRecord(r.topic(),
				value.get("partition") != null ? Integer.parseInt(value.get("partition").toString()) : null, null,
				value.get("key"), null, value.get("value"), Long.parseLong(value.get("timestamp").toString()));
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