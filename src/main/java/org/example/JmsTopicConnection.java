package org.example;

import java.net.URI;
import java.net.URISyntaxException;

import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.Session;
import javax.jms.Topic;
import javax.jms.TopicConnection;
import javax.jms.TopicConnectionFactory;
import javax.jms.TopicPublisher;
import javax.jms.TopicSession;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.broker.BrokerFactory;
import org.apache.activemq.broker.BrokerService;

/*
Pour cette classe nous allons utiliser 4 apis spécifique
TopicConnectionFactory:
TopicConnectionFactory définit la façon de créer des connexions à un système de messagerie pour les publications/abonnements de sujets

TopicConnection :
Un topic de connexion est utilisé pour publier/s'abonner à des messages dans un modèle de publication/abonnement

TopicSession:
TopicSession représente une session de messagerie pour les publications/abonnements de sujets.

TopicPublisher:
TopicPublisher représente l'éditeur de messages pour les publications de sujets
* */
public class JmsTopicConnection {
    public static void main(String[] args) throws URISyntaxException, Exception {
        BrokerService broker = BrokerFactory.createBroker(new URI(
                "broker:(tcp://localhost:61616)")); //création d'un nouveau broker
        broker.setPersistent(true); // on rend le broker persistant
        broker.start(); //demarrage du service broker
        TopicConnection topicConnection = null; // instaciation du topic connexion

        try {
            // --- Producteur ---
            //API 1 : TopicConnectionFactory:
            //on crée une connexion au système de messagerie
            TopicConnectionFactory connectionFactory = new ActiveMQConnectionFactory(
                    "tcp://localhost:61616");
            //on connecte le topic de connexion/ le sujet à la connexion systeme crée
            topicConnection = connectionFactory.createTopicConnection();
            //on défini l'identifiant du client pour une connexion JMS
            topicConnection.setClientID("JMSTOPIC");

            // API 2 : TopicSession
            // on crée une session de messagerie pour publier et s'abonner à des messages sur des sujets
            TopicSession topicConsumerSession = topicConnection.createTopicSession(
                    false, Session.AUTO_ACKNOWLEDGE);
            // on crée un topic/sujet
            Topic topic = topicConsumerSession.createTopic("customerTopic");

            // --- Consommateur 1 qui s'inscrit au customerTopic ---
            MessageConsumer consumer1 = topicConsumerSession.createSubscriber(topic);
            consumer1.setMessageListener(new ConsumerMessageListener(
                    "Consumer1"));

            // --- Consommateur 2 qui s'inscrit au customerTopic ---
            MessageConsumer consumer2 = topicConsumerSession.createSubscriber(topic);
            consumer2.setMessageListener(new ConsumerMessageListener(
                    "Consumer2"));

            topicConnection.start();

            // Publish
            TopicSession topicPublisherSession = topicConnection.createTopicSession(
                    false, Session.AUTO_ACKNOWLEDGE);
            String payload = "Important Task";
            Message msg = topicPublisherSession.createTextMessage(payload);
            TopicPublisher publisher = topicPublisherSession.createPublisher(topic);
            System.out.println("Sending text '" + payload + "'");
            publisher.publish(msg);


            Thread.sleep(3000);
            topicPublisherSession.close();
            topicConsumerSession.close();
        } finally {
            if (topicConnection != null) {
                topicConnection.close();
            }
            broker.stop();
        }
    }
}
