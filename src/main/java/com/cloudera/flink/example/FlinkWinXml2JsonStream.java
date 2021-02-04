package com.cloudera.flink.example;

import java.nio.charset.StandardCharsets;
import java.util.Properties;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.json.XML;

import com.cloudera.flink.filter.DedupeFilterValueState;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonObject;

/**
 * 
 * This flink streaming job convert windows xml event logs to json format and remove duplicates.
 * 
 * @author Mandeep Bawa
 *
 */
public class FlinkWinXml2JsonStream {

	private static final Gson gson = new GsonBuilder().create();

	public static void main(String[] args) throws Exception {

		String brokers = args[0];
		String inputTopic = args[1];
		String outTopic = args[2];
		String groupId = args[3];

		// For local execution enable this property
		//System.setProperty("java.security.auth.login.config", "./src/main/resources/jaas.conf");

		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.enableCheckpointing(2000);

		Properties consumerProperties = new Properties();
		consumerProperties.setProperty("bootstrap.servers", brokers);
		consumerProperties.setProperty("group.id", groupId);
		consumerProperties.setProperty("security.protocol", "SASL_SSL");
		consumerProperties.setProperty("sasl.mechanism", "PLAIN");
		consumerProperties.setProperty("sasl.kerberos.service.name", "kafka");

		// For local execution enable this property
		//consumerProperties.setProperty("ssl.truststore.location", "./src/main/resources/truststore.jks");

		// Disable these properties while running on local
		consumerProperties.setProperty("ssl.truststore.location","/usr/lib/jvm/java-1.8.0/jre/lib/security/cacerts");
		consumerProperties.put("sasl.jaas.config", "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"my_user\" password=\"******\";");

		FlinkKafkaConsumer<String> myConsumer = new FlinkKafkaConsumer<>(inputTopic, new SimpleStringSchema(), consumerProperties);
		myConsumer.setStartFromLatest();

		DataStream<String> stream = env.addSource(myConsumer).name("WindowsEventsTopic");

		DataStream<String> messageStream = stream
				.filter(log -> log.contains("<Event")).name("XmlFilter")
				.map(xmlEvent -> XML.toJSONObject(xmlEvent).toString()).name("Xml2JsonMap")
				.map(jsonString -> gson.fromJson(jsonString, JsonObject.class)).name("JsonString2ObjectMap")
				.keyBy(jsonObject -> jsonObject.getAsJsonObject("Event").getAsJsonObject("System").get("Computer")
						.getAsString()
						+ jsonObject.getAsJsonObject("Event").getAsJsonObject("System").get("EventRecordID").getAsString())
				.filter(new DedupeFilterValueState<>()).name("DeDupFilter")
				.map(jsonObject -> jsonObject.toString()).name("JsonObject2StringMap");
			
		Properties producerProperties = new Properties();
		producerProperties.setProperty("bootstrap.servers", brokers);
		producerProperties.setProperty("security.protocol", "SASL_SSL");
		producerProperties.setProperty("sasl.mechanism", "PLAIN");
		producerProperties.setProperty("sasl.kerberos.service.name", "kafka");

		// For local execution enable this property
		//producerProperties.setProperty("ssl.truststore.location", "./src/main/resources/truststore.jks");
		producerProperties.setProperty("ssl.truststore.location","/usr/lib/jvm/java-1.8.0/jre/lib/security/cacerts");
		producerProperties.put("sasl.jaas.config", "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"my_user\" password=\"********\";");

		   
		FlinkKafkaProducer<String> producer = new FlinkKafkaProducer<>(outTopic, 
				new KafkaSerializationSchema<String>() {
					private static final long serialVersionUID = 1L;
					
					@Override
				    public ProducerRecord<byte[], byte[]> serialize(String element, Long timestamp) {
				        return new ProducerRecord<byte[], byte[]>(outTopic, element.getBytes(StandardCharsets.UTF_8));
				    }
				},
				producerProperties, FlinkKafkaProducer.Semantic.EXACTLY_ONCE);
		
		messageStream.addSink(producer).name("DedupJsonEvents");

		env.execute();

	}

}
