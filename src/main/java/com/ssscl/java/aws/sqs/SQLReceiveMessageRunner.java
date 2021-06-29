package com.ssscl.java.aws.sqs;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.CommandLineRunner;
import software.amazon.awssdk.services.sqs.model.Message;

import java.util.List;

public class SQLReceiveMessageRunner implements CommandLineRunner {

    @Autowired
    private SQSReceiveMessages sqsReceiveMessages;

    @Value("consumer.waitTimeBetweenPollingInMillis")
    private int waitTimeBetweenPollingInMillis;

    @Override
    public void run(String... args) throws Exception {
        while(true) {
            List<Message> messagesList = sqsReceiveMessages.receiveMessages();
            messagesList.forEach(System.out::println);

            for(Message message: messagesList) {
                String empId = message.getValueForField("empId", String.class).orElse("");


            }
            Thread.sleep(waitTimeBetweenPollingInMillis);
        }
    }
}
