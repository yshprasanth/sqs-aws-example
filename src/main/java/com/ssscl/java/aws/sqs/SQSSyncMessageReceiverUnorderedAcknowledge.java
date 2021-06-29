package com.ssscl.java.aws.sqs;

import com.amazon.sqs.javamessaging.AmazonSQSMessagingClientWrapper;
import com.amazon.sqs.javamessaging.ProviderConfiguration;
import com.amazon.sqs.javamessaging.SQSConnection;
import com.amazon.sqs.javamessaging.SQSConnectionFactory;
import com.amazon.sqs.javamessaging.SQSSession;
import com.amazonaws.AmazonClientException;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.auth.PropertiesFileCredentialsProvider;
import com.amazonaws.services.sqs.AmazonSQSClientBuilder;
import org.springframework.beans.factory.annotation.Value;

import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.TextMessage;
import java.util.concurrent.TimeUnit;

/**
 * An example class to demonstrate the behavior of CLIENT_ACKNOWLEDGE mode for received messages. This example
 * complements the example given in {@link SQSSyncMessageReceiverUnorderedAcknowledge} for UNORDERED_ACKNOWLEDGE mode.
 *
 * First, a session, a message producer, and a message consumer are created. Then, two messages are sent. Next, two messages
 * are received but only the second one is acknowledged. After waiting for the visibility time out period, an attempt to
 * receive another message is made. It's shown that no message is returned for this attempt since in CLIENT_ACKNOWLEDGE mode,
 * as expected, all the messages prior to the acknowledged messages are also acknowledged.
 *
 * This ISN'T the behavior for UNORDERED_ACKNOWLEDGE mode. Please see {@link SQSSyncMessageReceiverUnorderedAcknowledge}
 * for an example.
 */
public class SQSSyncMessageReceiverUnorderedAcknowledge {

    @Value("sqs.aws.region")
    private String region;

    @Value("sqs.queue.name")
    private String queueName;

    @Value("sqs.receiver.waitTimeSeconds")
    private int waitTimeSeconds;

    private AWSCredentialsProvider credentialsProvider = new DefaultAWSCredentialsProviderChain();

    public SQSSyncMessageReceiverUnorderedAcknowledge() {
    }

    public void setCredentialsProvider(String credentialFile) {
        try {
            this.credentialsProvider = new PropertiesFileCredentialsProvider(credentialFile);
        } catch (AmazonClientException e) {
            throw new IllegalArgumentException("Error reading credentials from " + credentialFile, e );
        }
    }

    public void connect() {
        // Create the connection factory based on the config
        SQSConnectionFactory connectionFactory = new SQSConnectionFactory(
                new ProviderConfiguration(),
                AmazonSQSClientBuilder.standard()
                        .withRegion(region)
                        .withCredentials(this.credentialsProvider)
        );

        try {
            // Create the connection
            SQSConnection connection = connectionFactory.createConnection();

            // Create the queue if needed
            ensureQueueExists(connection, queueName);

            // Create the session  with unordered acknowledge mode
            Session session = connection.createSession(false, SQSSession.UNORDERED_ACKNOWLEDGE);

            // Create the producer and consumer
            MessageProducer producer = session.createProducer(session.createQueue(queueName));
            MessageConsumer consumer = session.createConsumer(session.createQueue(queueName));

            // Open the connection
            connection.start();

            // Send two text messages
            sendMessage(producer, session, "Message 1");
            sendMessage(producer, session, "Message 2");

            // Receive a message and don't acknowledge it
            receiveMessage(consumer, false);

            // Receive another message and acknowledge it
            receiveMessage(consumer, true);

            // Wait for the visibility time out, so that unacknowledged messages reappear in the queue
            System.out.println("Waiting for visibility timeout...");
            try {
                Thread.sleep(TimeUnit.SECONDS.toMillis(waitTimeSeconds));
            } catch (InterruptedException e) {
                e.printStackTrace();
            }

            // Attempt to receive another message and acknowledge it. This results in receiving the first message since
            // we have acknowledged only the second message. In the UNORDERED_ACKNOWLEDGE mode, all the messages must
            // be explicitly acknowledged.
            receiveMessage(consumer, true);

            // Close the connection. This closes the session automatically
            connection.close();
            System.out.println("Connection closed.");
        } catch (JMSException e) {
            e.printStackTrace();
        }

    }

    public void ensureQueueExists(SQSConnection connection, String queueName) throws JMSException {
        AmazonSQSMessagingClientWrapper client = connection.getWrappedAmazonSQSClient();

        /**
         * In most cases, you can do this with just a createQueue call, but GetQueueUrl
         * (called by queueExists) is a faster operation for the common case where the queue
         * already exists. Also many users and roles have permission to call GetQueueUrl
         * but don't have permission to call CreateQueue.
         */
        if( !client.queueExists(queueName) ) {
            client.createQueue( queueName );
        }
    }

    /**
     * Sends a message through the producer.
     *
     * @param producer Message producer
     * @param session Session
     * @param messageText Text for the message to be sent
     * @throws JMSException
     */
    private void sendMessage(MessageProducer producer, Session session, String messageText) throws JMSException {
        // Create a text message and send it
        producer.send(session.createTextMessage(messageText));
    }

    /**
     * Receives a message through the consumer synchronously with the default timeout (TIME_OUT_SECONDS).
     * If a message is received, the message is printed. If no message is received, "Queue is empty!" is
     * printed.
     *
     * @param consumer Message consumer
     * @param acknowledge If true and a message is received, the received message is acknowledged.
     * @throws JMSException
     */
    private void receiveMessage(MessageConsumer consumer, boolean acknowledge) throws JMSException {
        // Receive a message
        Message message = consumer.receive(TimeUnit.SECONDS.toMillis(waitTimeSeconds));

        if (message == null) {
            System.out.println("Queue is empty!");
        } else {
            // Since this queue has only text messages, cast the message object and print the text
            System.out.println("Received: " + ((TextMessage) message).getText());

            // Acknowledge the message if asked
            if (acknowledge) message.acknowledge();
        }
    }
}
