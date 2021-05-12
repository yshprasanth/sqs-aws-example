package com.ssscl.java.aws.sqs;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.GetQueueUrlRequest;
import software.amazon.awssdk.services.sqs.model.GetQueueUrlResponse;

@Service
public class SQSClientConnection {

    @Value("sqs.aws.region")
    private String region;

    @Value("sqs.queue.name")
    private String queueName;

    private SqsClient sqsClient;
    private String queueUrl;

    public SQSClientConnection() {
        try {
            this.sqsClient = SqsClient.builder()
                    .region(Region.of(region))
                    .build();

            if(sqsClient!=null) {
                GetQueueUrlResponse getQueueUrlResponse = sqsClient.getQueueUrl(
                        GetQueueUrlRequest.builder().queueName(queueName).build());
                queueUrl = getQueueUrlResponse.queueUrl();
            }
        } catch(Exception ex) {
            System.out.println("Exception while creating SqsClient: " + ex);
        }
    }

    public SqsClient getSqsClient() {
        return sqsClient;
    }

    public String getQueueUrl() {
        return queueUrl;
    }
}
