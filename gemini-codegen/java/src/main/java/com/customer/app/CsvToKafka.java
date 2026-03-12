package com.customer.app;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.io.FileReader;
import java.io.IOException;
import java.io.Reader;
import java.util.Properties;

public class CsvToKafka {

    public static void main(String[] args) {
        if (args.length != 3) {
            System.err.println("Usage: CsvToKafka <csv-file-path> <kafka-bootstrap-servers> <kafka-topic>");
            System.exit(1);
        }

        String csvFilePath = args[0];
        String bootstrapServers = args[1];
        String topic = args[2];

        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        try (KafkaProducer<String, String> producer = new KafkaProducer<>(props);
             Reader reader = new FileReader(csvFilePath);
             CSVParser csvParser = new CSVParser(reader, CSVFormat.DEFAULT.withFirstRecordAsHeader())) {

            for (CSVRecord csvRecord : csvParser) {
                // Convert CSV record to a simple JSON string
                String jsonRecord = recordToJson(csvRecord);
                ProducerRecord<String, String> record = new ProducerRecord<>(topic, jsonRecord);
                
                producer.send(record, (metadata, exception) -> {
                    if (exception != null) {
                        exception.printStackTrace();
                    }
                });
            }
            System.out.println("Finished loading CSV to Kafka.");

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private static String recordToJson(CSVRecord record) {
        StringBuilder json = new StringBuilder("{");
        record.toMap().forEach((key, value) -> {
            json.append("\"").append(key).append("\":\"").append(value).append("\",");
        });
        if (json.length() > 1) {
            json.setLength(json.length() - 1); // Remove trailing comma
        }
        json.append("}");
        return json.toString();
    }
}
