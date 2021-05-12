package com.ssscl.java.aws.sqs;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import software.amazon.awssdk.services.sqs.model.Message;
import software.amazon.awssdk.services.sqs.model.ReceiveMessageRequest;
import software.amazon.awssdk.services.sqs.model.SqsException;

import java.util.List;

@Service
public class SQSReceiveMessages {

    @Autowired
    private SQSClientConnection sqsClientConnection;

    @Value("sqs.receiver.waitTimeSeconds")
    private int waitTimeSeconds;

    @Value("sqs.receiver.maxNumberOfMessages")
    private int maxNumberOfMessages;

    public List<Message> receiveMessages() {
        System.out.println("\nReceive messages..");
        try {
            ReceiveMessageRequest receiveMessageRequest = ReceiveMessageRequest.builder()
                    .queueUrl(sqsClientConnection.getQueueUrl())
                    .waitTimeSeconds(waitTimeSeconds) // long polling
                    .maxNumberOfMessages(maxNumberOfMessages)
                    .build();
            List<Message> messages = sqsClientConnection.getSqsClient()
                    .receiveMessage(receiveMessageRequest)
                    .messages();
            return messages;
        } catch (SqsException e) {
            System.err.println(e.awsErrorDetails().errorMessage());
        }
        return null;
    }
}
