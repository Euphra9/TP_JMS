package org.example;

import java.net.URI;
import java.net.URISyntaxException;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.TextMessage;
import javax.jms.Topic;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.broker.BrokerFactory;
import org.apache.activemq.broker.BrokerService;

public class JmsDurableSubscriber{
    public static void main(String[] args) throws URISyntaxException, Exception {
        BrokerService broker = BrokerFactory.createBroker(new URI(
                "broker:(tcp://localhost:61616)"));
        broker.start();
        Connection connection = null;
        try {
            // Producteur
            ConnectionFactory connectionFactory = new ActiveMQConnectionFactory(
                    "tcp://localhost:61616");
            connection = connectionFactory.createConnection();
            connection.setClientID("Test de durabilité");
            Session session = connection.createSession(false,
                    Session.AUTO_ACKNOWLEDGE);
            Topic topic = session.createTopic("customerTopic");

            // Publication
            String payload = "Il faut rendre le TP de JMS au plus tard le vendredi 10 février";
            TextMessage msg = session.createTextMessage(payload);
            MessageProducer publisher = session.createProducer(topic);
            System.out.println("Message envoyé : '" + payload + "'");
            //publication du message
            publisher.send(msg, javax.jms.DeliveryMode.PERSISTENT, javax.jms.Message.DEFAULT_PRIORITY, Message.DEFAULT_TIME_TO_LIVE);

            // Consommateur 1 s'abonne au sujet
            MessageConsumer consumer1 = session.createDurableSubscriber(topic, "consumer1", "", false);

            // Consommateur 2 s'abonne au sujet
            MessageConsumer consumer2 = session.createDurableSubscriber(topic, "consumer2", "", false);

            connection.start();

            msg = (TextMessage) consumer1.receive();
            System.out.println("Euphraïm reçoit le message ' " + msg.getText()+"'");


            msg = (TextMessage) consumer2.receive();
            System.out.println("Malak reçoit le message '" + msg.getText()+"'");

            session.close();
        } finally {
            if (connection != null) {
                connection.close();
            }
            broker.stop();
        }
    }
}