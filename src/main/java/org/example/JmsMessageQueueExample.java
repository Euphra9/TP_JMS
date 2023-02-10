package org.example;

import java.net.URI;
import java.net.URISyntaxException;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.Session;
import javax.jms.TextMessage;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.broker.BrokerFactory;
import org.apache.activemq.broker.BrokerService;

public class JmsMessageQueueExample {
    public static void main(String[] args) throws URISyntaxException, Exception {
        BrokerService broker = BrokerFactory.createBroker(new URI(
                "broker:(tcp://localhost:61616)"));
        broker.start();
        Connection connection = null;
        try {
            // Producer
            ConnectionFactory connectionFactory = new ActiveMQConnectionFactory(
                    "tcp://localhost:61616");
            connection = connectionFactory.createConnection();

            //In order to create a queue object, we need to first create a session
            // and then call createQueue() on the session object.
            //The queue stores all messages until they’re delivered or until they expire.
            Session session = connection.createSession(false,
                    Session.AUTO_ACKNOWLEDGE);
            Queue queue = session.createQueue("customerQueue");

            //Create a message to be able to send it to the queue
            String payload = "Bonjour tout le monde";
            Message msg = session.createTextMessage(payload);

            //Sending message to Queue (from producer to a queue)
            MessageProducer producer = session.createProducer(queue);
            System.out.println("Voici le message envoyé : " +"'" + payload + "'");
            producer.send(msg);

            // Just like the producer, consumer also needs a session using which it will connect to the queue.
            MessageConsumer consumer = session.createConsumer(queue);
            connection.start();
            TextMessage textMsg = (TextMessage) consumer.receive();
            System.out.println(textMsg);
            System.out.println("Voici le message reçu : " + textMsg.getText());
            session.close();
        } finally {
            if (connection != null) {
                connection.close();
            }
            broker.stop();
        }
    }

}