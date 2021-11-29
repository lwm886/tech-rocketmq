package com.tech.rocketmq.jms;

import lombok.extern.slf4j.Slf4j;
import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeOrderlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeOrderlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerOrderly;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.consumer.ConsumeFromWhere;
import org.apache.rocketmq.common.message.MessageExt;
import org.springframework.stereotype.Component;

import java.util.List;

/**
 * @author lw
 * @since 2021/11/19
 */
@Slf4j
@Component
public class OrderlyConsumer {
    private DefaultMQPushConsumer consumer;
    private String consumerGroup="pay_orderly_consumer_group";

    public OrderlyConsumer() throws MQClientException {
        consumer = new DefaultMQPushConsumer(consumerGroup);
        consumer.setNamesrvAddr(JmsConfig.NAME_SERVER);
        consumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_LAST_OFFSET);
        
        consumer.subscribe(JmsConfig.ORDERLY_TOPIC,"*");
        //顺序消费
        consumer.registerMessageListener(new MessageListenerOrderly() {
            @Override
            public ConsumeOrderlyStatus consumeMessage(List<MessageExt> msgs, ConsumeOrderlyContext context) {
                MessageExt messageExt = msgs.get(0);
                try{
                    byte[] body = messageExt.getBody();
                    log.info("Receive New Messages:{}",new java.lang.String(body));
                    //做业务逻辑操作
                    return ConsumeOrderlyStatus.SUCCESS;
                }catch (Exception e){
                    e.printStackTrace();
                    //暂停一会再获取
                    return ConsumeOrderlyStatus.SUSPEND_CURRENT_QUEUE_A_MOMENT;
                }
            }
        });
        consumer.start();
        System.out.println("consumer start...");
    }
}
