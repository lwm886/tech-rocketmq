package com.tech.rocketmq.jms;

import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.springframework.stereotype.Component;

/**
 * @author lw
 * @since 2021/11/15
 */
@Component
public class PayProducer {
    private String producerGroup="pay_group";
    private DefaultMQProducer producer;
    public PayProducer(){
        producer=new DefaultMQProducer(producerGroup);
        //指定NameServer地址，多个地址以;隔开
        //如producer.setNamesrvAddr("127.0.0.1:9876;127.0.0.1:9877")
        producer.setNamesrvAddr(JmsConfig.NAME_SERVER);
        //生产者投递消息重试次数（内部重试）,默认是2次，异步和SendOneWay下配置无效
        producer.setRetryTimesWhenSendFailed(3);
        start();
    }

    public DefaultMQProducer getProducer() {
        return producer;
    }

    /**
     * 对象使用之前必须调用一次，只能初始化一次
     */
    private void start() {
        try {
            this.producer.start();
        } catch (MQClientException e) {
            e.printStackTrace();
        }
    }

    /**
     * 一般在应用上下文，使用上下文监听器，进行关闭
     */
    private void shutDown(){
        this.producer.shutdown();
    }
}
