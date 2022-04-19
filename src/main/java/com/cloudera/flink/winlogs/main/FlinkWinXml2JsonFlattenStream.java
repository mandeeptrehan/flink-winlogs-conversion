package com.cloudera.flink.winlogs.main;

import java.nio.charset.StandardCharsets;
import java.util.Properties;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.json.XML;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;

/**
 * This flink job converts windows xml event logs to flatten json and remove
 * duplicates
 * 
 * @author Mandeep Bawa
 *
 */

public class FlinkWinXml2JsonFlattenStream {

	private static final Gson gson = new GsonBuilder().create();

	public static void main(String[] args) throws Exception {

		String brokers = args[0];
		String inputTopic = args[1];
		String outTopic = args[2];
		String groupId = args[3];

		// For local execution enable this property
		//System.setProperty("java.security.auth.login.config","./jaas.conf");

		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.enableCheckpointing(30000);

		Properties producerProperties = new Properties();
		producerProperties.setProperty("bootstrap.servers", brokers);
		producerProperties.setProperty("client.id", "flink-xml-to-json");
		producerProperties.setProperty("security.protocol", "SASL_SSL");
		producerProperties.setProperty("sasl.mechanism", "PLAIN");
		producerProperties.setProperty("sasl.kerberos.service.name", "kafka");

		// Disable these properties while running on local
		producerProperties.setProperty("ssl.truststore.location", "/usr/lib/jvm/java-1.8.0/jre/lib/security/cacerts");
		producerProperties.put("sasl.jaas.config",
				"org.apache.kafka.common.security.plain.PlainLoginModule required username=\"name\" password=\"password\";");

		// For local execution enable this property
		// producerProperties.setProperty("ssl.truststore.location", "./src/main/resources/truststore.jks");

		FlinkKafkaProducer<String> producer = new FlinkKafkaProducer<>(outTopic,
				new KafkaSerializationSchema<String>() {
					private static final long serialVersionUID = 1L;

					@Override
					public ProducerRecord<byte[], byte[]> serialize(String element, Long timestamp) {
						return new ProducerRecord<byte[], byte[]>(outTopic, element.getBytes(StandardCharsets.UTF_8));
					}
				}, producerProperties, FlinkKafkaProducer.Semantic.AT_LEAST_ONCE);

		Properties consumerProperties = new Properties();
		consumerProperties.setProperty("bootstrap.servers", brokers);
		consumerProperties.setProperty("group.id", groupId);
		consumerProperties.setProperty("security.protocol", "SASL_SSL");
		consumerProperties.setProperty("sasl.mechanism", "PLAIN");
		consumerProperties.setProperty("sasl.kerberos.service.name", "kafka");

		// For local execution enable this property
		// consumerProperties.setProperty("ssl.truststore.location","./src/main/resources/truststore.jks");

		// Disable these properties while running on local
		consumerProperties.setProperty("ssl.truststore.location", "/usr/lib/jvm/java-1.8.0/jre/lib/security/cacerts");
		consumerProperties.put("sasl.jaas.config",
				"org.apache.kafka.common.security.plain.PlainLoginModule required username=\"name\" password=\"Password\";");

		FlinkKafkaConsumer<String> myConsumer = new FlinkKafkaConsumer<>(inputTopic, new SimpleStringSchema(),
				consumerProperties);
		myConsumer.setStartFromLatest();

		DataStream<String> stream = env.addSource(myConsumer);

		DataStream<String> messageStream = stream
				.filter(new FilterFunction<String>() {

						/**
						 * 
						 */
						private static final long serialVersionUID = 1L;
			
						@Override
						public boolean filter(String log) throws Exception {
							try {
								String tempLog = log.replaceAll("[^\\p{ASCII}]", "");
								XML.toJSONObject(tempLog).toString();
								if (!log.trim().equals("") && log.contains("<Event")) {
									return true;
								}
							} catch (Exception e) {
								System.out.println("Json Event:" + log.toString());
								e.printStackTrace();
	
							}
							return false;
						}
					}).name("Filter")
				.map(xmlEvent -> {	
						xmlEvent = xmlEvent.replaceAll("[^\\p{ASCII}]", "");
						String jsonString = XML.toJSONObject(xmlEvent).toString();
						if (jsonString == null || jsonString.equals("") || jsonString.equals("{}")) {
							System.out.println(xmlEvent);
						}
						return jsonString;
					}).name("Xml2Json")
				.map(jsonString -> gson.fromJson(jsonString, JsonObject.class)).name("Json2Object")
						//.keyBy(jsonObject -> jsonObject.getAsJsonObject("Event").getAsJsonObject("System").get("Computer")
						//		.getAsString()
						//		+ jsonObject.getAsJsonObject("Event").getAsJsonObject("System").get("EventRecordID")
						//				.getAsString())
				//.filter(new DedupeFilterValueState<>()).name("Dedup")
				.map(new MapFunction<JsonObject, String>() {
		
							/**
							 * 
							 */
							private static final long serialVersionUID = 1L;
		
							@Override
							public String map(JsonObject jsonObject) {
								JsonObject flattenJson = new JsonObject();
								try {
									JsonElement system = jsonObject.getAsJsonObject("Event").get("System");
									if (system != null && !system.isJsonPrimitive()) {
										JsonObject systemData = jsonObject.getAsJsonObject("Event").getAsJsonObject("System");
										systemData.entrySet().forEach(action -> {
											if (action.getValue().isJsonObject()) {
												((JsonObject) action.getValue()).entrySet()
														.forEach(value -> flattenJson.add(value.getKey(), value.getValue()));
											} else {
												flattenJson.add(action.getKey(), action.getValue());
											}
										});
									}
		
									JsonElement event = jsonObject.getAsJsonObject("Event").get("EventData");
		
									if (event != null && !event.isJsonPrimitive()) {
										JsonObject eventData = jsonObject.getAsJsonObject("Event").getAsJsonObject("EventData");
		
										eventData.entrySet().forEach(action -> {
											if (action.getValue().isJsonArray()) {
												action.getValue().getAsJsonArray().forEach(arrayElmnt -> {
													if (arrayElmnt.isJsonObject()) {
														flattenJson.add(((JsonObject) arrayElmnt).get("Name").getAsString(),
																((JsonObject) arrayElmnt).get("content"));
													}
												});
											}
										});
									}
		
								} catch (Exception e) {
									System.out.println("Json Event:" + jsonObject.toString());
									e.printStackTrace();
		
								}
								return flattenJson.toString();
							}
						}).name("Json2FlattenedJson");

		messageStream.addSink(producer);

		env.execute();

	}

}
