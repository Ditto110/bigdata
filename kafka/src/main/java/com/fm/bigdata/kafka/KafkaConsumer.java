package com.fm.bigdata.kafka;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

/**
 * @author ASUS
 * Created at 16:43.2019/4/21
 */
@Component
public class KafkaConsumer {
    private static final Logger LOGGER = LogManager.getLogger(KafkaConsumer.class);
    @KafkaListener(topics = "ditto")
    public void getMsg(ConsumerRecord<?, ?> record) {
        LOGGER.info(record.topic() + " key:" + record.key() + " value:" + record.value());
    }
}
