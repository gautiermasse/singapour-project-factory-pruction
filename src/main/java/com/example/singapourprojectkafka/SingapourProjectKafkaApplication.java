package com.example.singapourprojectkafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import java.util.Properties;
import java.util.Random;

@SpringBootApplication
public class SingapourProjectKafkaApplication {

    public static void main(String[] args) {

        SpringApplication.run(SingapourProjectKafkaApplication.class, args);

        while(true) {
            Properties props = new Properties();
            props.put("bootstrap.servers", "localhost:39092"); // Spécifier l'hôte et le port du cluster Kafka
            props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer"); // Spécifier le sérialiseur pour la clé
            props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer"); // Spécifier le sérialiseur pour la valeur
            Producer<String, String> producer = new KafkaProducer<>(props);

            // Generate key product because we have 3 product with 3 ids in your base.
            int borneMinKeyProduct = 1; // borne minimale
            int borneMaxKeyProduct = 3; // borne maximale
            Random rand = new Random(); // création d'un objet de type Random
            int keyProduct = rand.nextInt(borneMaxKeyProduct - borneMinKeyProduct + 1) + borneMinKeyProduct; // génération du nombre aléatoire

            int borneMinNbProduct = 1; // borne minimale
            int borneMaxNbProduct = 3000; // borne maximale
            int NbProducts = rand.nextInt(borneMaxNbProduct - borneMinNbProduct + 1) + borneMinNbProduct;


            String key = Integer.toString(keyProduct);
            String value = "{\"data\": {\"idProduct\":" + keyProduct + ",\"prodcution\":" + NbProducts + ",\"status\": 'SUCCESS'}}";
            String topic = "production";

            ProducerRecord<String, String> record = new ProducerRecord<>(topic, key, value);
            producer.send(record);
            try {
                Thread.sleep(3000); // Mettre le thread en pause pendant 5 secondes
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

    }

}
