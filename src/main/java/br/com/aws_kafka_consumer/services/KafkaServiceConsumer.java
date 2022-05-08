package br.com.aws_kafka_consumer.services;

import java.time.Duration;
import java.util.Collections;
import java.util.concurrent.ExecutionException;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

public class KafkaServiceConsumer {
    public static void readMessage(String groupId) throws InterruptedException, ExecutionException {
        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(
                PropertiesConfig.properties(groupId));
        consumer.subscribe(Collections.singletonList(System.getenv("KAFKA_TOPIC")));

        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(500));

            for (ConsumerRecord<String, String> registro : records) {
                System.out.println("------------------------------------------");
                System.out.println("Tópico: " + registro.topic());
                System.out.println("Produto | Quantidade");
                System.out.println("Nome do Produto: " + registro.key());
                System.out.println("Qauntidade: " + registro.value());
                System.out.println("------------------------------------------");
            }
        }
    }
}
