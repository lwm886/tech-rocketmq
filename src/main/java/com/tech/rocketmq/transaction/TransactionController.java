package com.tech.rocketmq.transaction;

import com.tech.rocketmq.jms.JmsConfig;
import lombok.extern.slf4j.Slf4j;
import org.apache.rocketmq.client.producer.TransactionSendResult;
import org.apache.rocketmq.common.message.Message;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.HashMap;

/**
 * @author lw
 * @since 2021/11/23
 */
@Slf4j
@RestController
public class TransactionController {
    
    @Autowired
    private TransactionProducer transactionMQProducer;

    /**
     * 分布式事务
     * @param tag
     * @param otherParam
     * @return
     * @throws Exception
     */
    @GetMapping("tran")
    public Object callback(String tag,String otherParam) throws Exception{
        Message message = new Message(JmsConfig.TOPIC, tag, tag + "_key", tag.getBytes());
        TransactionSendResult sendResult = transactionMQProducer.getProducer().sendMessageInTransaction(message, otherParam);
        log.info("发送结果={}",sendResult);
        return new HashMap<>();
    }
}
