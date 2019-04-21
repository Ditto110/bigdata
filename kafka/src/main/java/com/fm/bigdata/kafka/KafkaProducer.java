package com.fm.bigdata.kafka;

import com.alibaba.fastjson.JSONObject;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

/**
 * @author ASUS
 * Created at 16:13.2019/4/21
 * 初始化信息
 */
@Component
public class KafkaProducer implements CommandLineRunner {
    public static final String TOPIC = "ditto";

    @Autowired
    private KafkaTemplate<String,Object> kafkaTemplate;

    @Override
    public void run(String... strings) throws Exception {

        while (true) {
            Thread.sleep(1000);
            JSONObject jsonObject = new JSONObject();
            jsonObject.put("pv", 100);
            jsonObject.put("uv", 100);
            kafkaTemplate.send(TOPIC, jsonObject.toJSONString());
        }

    }
}
