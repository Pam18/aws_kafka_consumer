package br.com.aws_kafka_consumer;

import java.util.concurrent.ExecutionException;

import br.com.aws_kafka_consumer.services.KafkaServiceConsumer;

public class App 
{
    public static void main( String[] args ) throws InterruptedException, ExecutionException
    {
        System.out.println("Recendo mensagem...");
        String grupoId = System.getenv("KAFKA_GROUP_ID_READER");
        KafkaServiceConsumer.readMessage(grupoId);
    }
}
