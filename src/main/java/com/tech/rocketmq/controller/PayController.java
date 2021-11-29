package com.tech.rocketmq.controller;

import com.tech.rocketmq.domain.ProductOrder;
import com.tech.rocketmq.jms.JmsConfig;
import com.tech.rocketmq.jms.PayProducer;
import lombok.extern.slf4j.Slf4j;
import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.MessageQueueSelector;
import org.apache.rocketmq.client.producer.SendCallback;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.remoting.exception.RemotingException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.HashMap;
import java.util.List;

/**
 * @author lw
 * @since 2021/11/15
 */
@Slf4j
@RestController
public class PayController {

    @Autowired
    private PayProducer payProducer;

    // 同步发送
    @GetMapping("send")
    Object callback(String text) throws InterruptedException, RemotingException, MQClientException, MQBrokerException {
//        Message message = new Message(JmsConfig.TOPIC, "taga", ("hello word = " + text).getBytes());
        //发送消息时，指定消息的key，比如订单编号
        Message message = new Message(JmsConfig.TOPIC, "taga", "666", ("hello word = " + text).getBytes());
        SendResult sendResult = payProducer.getProducer().send(message);
        System.out.println(sendResult);
        return new HashMap<>();
    }
    
    //异步发送
    @GetMapping("sendAsync")
    Object callbackAsync(String text) throws InterruptedException, RemotingException, MQClientException, MQBrokerException {
        Message message = new Message(JmsConfig.TOPIC, "taga", "a", ("hello word = " + text).getBytes());
        payProducer.getProducer().send(message, new SendCallback() {
            @Override
            public void onSuccess(SendResult sendResult) {
                log.info("发送结果：{}",sendResult.toString());
            }

            @Override
            public void onException(Throwable e) {
                e.printStackTrace();
                System.out.println("补偿机制 根据业务需要决定是否进行重试");
            }
        });
        return new HashMap<>();
    }
    
    //OneWay发送
    @GetMapping("sendOneWay")
    Object callbackOneWay(String text) throws InterruptedException, RemotingException, MQClientException, MQBrokerException {
        Message message = new Message(JmsConfig.TOPIC, "taga", "a", ("hello word = " + text).getBytes());
        payProducer.getProducer().sendOneway(message);
        return new HashMap<>();
    }

    //延迟消息
    @GetMapping("sendDelay")
    Object callbackDelay(String text) throws InterruptedException, RemotingException, MQClientException, MQBrokerException {
        Message message = new Message(JmsConfig.TOPIC, "taga", "a", ("hello word = " + text).getBytes());
        //"1s 5s 10s 30s 1m 2m 3m 4m 5m 6m 7m 8m 9m 10m 20m 30m 1h 2h"
        //1s对应level为1,5s对应level为2,10s对应level为3，时间与level依次对应
        //延迟消息是到期的消息先被消费，而不一定是发送最早的消息先被消费
        if(text.equals("a")){
            message.setDelayTimeLevel(3); //延迟时间为10s
        }else{
            message.setDelayTimeLevel(1);//延迟时间为1s
        }
        payProducer.getProducer().send(message, new SendCallback() {
            @Override
            public void onSuccess(SendResult sendResult) {
                log.info("发送结果：{}",sendResult.toString());
            }

            @Override
            public void onException(Throwable e) {
                e.printStackTrace();
                System.out.println("补偿机制 根据业务需要决定是否进行重试");
            }
        });
        return new HashMap<>();
    }


    // 消息队列选择器指定Topic下的某个队列进行发送
    @GetMapping("sendSelect")
    Object callbackSelect(String text) throws InterruptedException, RemotingException, MQClientException, MQBrokerException {
        Message message = new Message(JmsConfig.TOPIC, "taga",  ("hello word = " + text).getBytes());
        SendResult sendResult = payProducer.getProducer().send(message, new MessageQueueSelector() {
            @Override
            public MessageQueue select(List<MessageQueue> mqs, Message msg, Object arg) {
                //arg是在send方法中传入的arg参数 0，最大为Topic下队列的数量-1
                int queueId=Integer.valueOf(arg.toString());
                return mqs.get(queueId);
            }
        },0);
        System.out.println(sendResult);
        return new HashMap<>();
    }

    //消息队列选择器指定Topic下的某个队列进行发送
    @GetMapping("sendAsyncSelect")
    Object callbackAsyncSelect(String text) throws InterruptedException, RemotingException, MQClientException, MQBrokerException {
        Message message = new Message(JmsConfig.TOPIC, "taga", "a", ("hello word = " + text).getBytes());
        payProducer.getProducer().send(message, new MessageQueueSelector() {
            @Override
            public MessageQueue select(List<MessageQueue> mqs, Message msg, Object arg) {
                //arg是在send方法中传入的arg参数 0,最大为Topic下队列的数量-1
                int queueId=Integer.valueOf(arg.toString());
                return mqs.get(queueId);
            }
        },1, new SendCallback() {
            @Override
            public void onSuccess(SendResult sendResult) {
                log.info("发送结果：{}",sendResult.toString());
            }

            @Override
            public void onException(Throwable e) {
                e.printStackTrace();
                System.out.println("补偿机制 根据业务需要决定是否进行重试");
            }
        });
        return new HashMap<>();
    }
    
    @GetMapping("sendOrderly")
    public Object callback() throws InterruptedException, RemotingException, MQClientException, MQBrokerException {
        List<ProductOrder> orderList = ProductOrder.getOrderList();
        for (int i = 0; i < orderList.size(); i++) {
            ProductOrder productOrder = orderList.get(i);
            Message message = new Message(JmsConfig.ORDERLY_TOPIC, "", productOrder.getOrderId()+"", productOrder.toString().getBytes());
            SendResult sendResult = payProducer.getProducer().send(message, new MessageQueueSelector() {
                @Override
                public MessageQueue select(List<MessageQueue> mqs, Message msg, Object arg) {
                    Long orderId = (Long) arg;
                    long index = orderId % mqs.size();
                    return mqs.get((int) index);
                }
            }, productOrder.getOrderId());
            log.info("发送结果：{}|order:{}",sendResult,productOrder);
        }
        return new HashMap<>();
    }

    // TAG过滤
    @GetMapping("sendTag")
    Object callbackTag(String text,String a) throws InterruptedException, RemotingException, MQClientException, MQBrokerException {
//        Message message = new Message(JmsConfig.TOPIC, "taga", ("hello word = " + text).getBytes());
        //发送消息时，指定消息的key，比如订单编号
//        Message message = new Message(JmsConfig.TOPIC, tag, "666", ("hello word = " + text).getBytes());
        Message message = new Message(JmsConfig.TOPIC, "", "666", ("hello word = " + text).getBytes());
        message.putUserProperty("amount",a);
        SendResult sendResult = payProducer.getProducer().send(message);
        System.out.println(sendResult);
        return new HashMap<>();
    }
}
