package com.example.requestsynckafkapoc;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.MediaType;
import org.springframework.kafka.requestreply.ReplyingKafkaTemplate;
import org.springframework.kafka.requestreply.RequestReplyFuture;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.kafka.support.SendResult;
import org.springframework.util.concurrent.ListenableFutureCallback;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import java.time.Duration;
import java.util.concurrent.ExecutionException;

@RestController
@Slf4j
public class KafkaController {

    @Autowired
    ReplyingKafkaTemplate<String, String, String> kafkaTemplate;

    @Value("${requestTopic}")
    String requestTopic;

    @Value("${requestReplyTopic}")
    String requestReplyTopic;


    @ResponseBody
    @PostMapping(value = "/sum", produces = MediaType.APPLICATION_JSON_VALUE, consumes = MediaType.APPLICATION_JSON_VALUE)
    public String sum(@RequestBody Model request) throws InterruptedException, ExecutionException {
        // create producer record
        ProducerRecord<String, String> record = new ProducerRecord<>(requestTopic, "HI");
        // set reply topic in header
        record.headers().add(new RecordHeader(KafkaHeaders.REPLY_TOPIC, requestReplyTopic.getBytes()));
        // post in kafka topic
        Duration duration = Duration.ofSeconds(6000L);

        if (!kafkaTemplate.isRunning()) {
            kafkaTemplate.setAutoStartup(true);
            kafkaTemplate.start();
        }
        final RequestReplyFuture<String, String, String> sendAndReceive = kafkaTemplate.sendAndReceive(record, duration);

        sendAndReceive.getSendFuture().addCallback(new ListenableFutureCallback<SendResult<String, String>>() {
            @Override
            public void onFailure(Throwable throwable) {
                log.error(throwable.getMessage());
                System.out.println(throwable.getMessage());
            }

            @Override
            public void onSuccess(SendResult<String, String> stringStringSendResult) {
                System.out.println();
                System.out.println();
                System.out.println();
                System.out.println();
                System.out.println();
                log.error("Successfully send");
                System.out.println();
                System.out.println();
                System.out.println();
                try {
                    System.out.println(sendAndReceive.get().value());
                } catch (InterruptedException e) {
                    e.printStackTrace();
                } catch (ExecutionException e) {
                    e.printStackTrace();
                }
            }
        });
//         confirm if producer produced successfully
//        SendResult<String, String> sendResult = sendAndReceive.getSendFuture().get();

        //print all headers
//        sendResult.getProducerRecord().headers().forEach(header -> System.out.println(header.key() + ":" + Arrays.toString(header.value())));

        // get consumer record
        // ConsumerRecord<String, String> consumerRecord = sendAndReceive.get();
        // return consumer value
        //return consumerRecord.value();
        return null;
    }
}
