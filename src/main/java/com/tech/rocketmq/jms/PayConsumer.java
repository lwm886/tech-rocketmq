package com.tech.rocketmq.jms;

import lombok.extern.slf4j.Slf4j;
import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.MessageSelector;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.consumer.ConsumeFromWhere;
import org.apache.rocketmq.common.message.MessageExt;
import org.springframework.stereotype.Component;

import java.util.List;

/**
 * @author lw
 * @since 2021/11/15
 */
@Slf4j
@Component
public class PayConsumer {
    private DefaultMQPushConsumer consumer;
    private String consumerGroup = "pay_consumer_group";

    public PayConsumer() throws MQClientException {
        consumer = new DefaultMQPushConsumer(consumerGroup);
        consumer.setNamesrvAddr(JmsConfig.NAME_SERVER);
        consumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_LAST_OFFSET);
        //默认是集群模式，如果改为广播模式不支持消费端重试
//        consumer.setMessageModel(MessageModel.BROADCASTING);
        // * 订阅Topic下所有的TAG
//        consumer.subscribe(JmsConfig.TOPIC, "*");
        //订阅该TOPIC下，TAG为create或pay的消息
//        consumer.subscribe(JmsConfig.TOPIC, "create||pay");
        //SQL过滤  属性在发送消息时使用 message.putUserProperty添加
        consumer.subscribe(JmsConfig.TOPIC, MessageSelector.bySql("amount > 5"));
        
        consumer.registerMessageListener(new MessageListenerConcurrently() {
            @Override
            public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> list, ConsumeConcurrentlyContext consumeConcurrentlyContext) {
                MessageExt message = list.get(0);
                int reconsumeTimes = message.getReconsumeTimes();
                log.info("重试次数：{}", reconsumeTimes);
                try {
                    log.info("Receive New Message: {}", new String(message.getBody()));
                    String topic = message.getTopic();
                    String tags = message.getTags();
                    String keys = message.getKeys();
//                    if(keys.equals("666")){
//                        throw new Exception("模拟异常");
//                    }
                    log.info("topic={} tags={} keys={}", topic, tags, keys);
                    return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
                } catch (Exception e) {
                    log.error("消费异常",e);
                    if(reconsumeTimes>=2){
                        log.info("重试次数大于等于2，记录数据库，发短信通知开发人员或者运营人员");
                        //告诉broker，本次消费成功
                        return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
                    }
                    return ConsumeConcurrentlyStatus.RECONSUME_LATER;
                }
            }
        });
        consumer.start();
        System.out.println("consumer start ...");
    }
}
