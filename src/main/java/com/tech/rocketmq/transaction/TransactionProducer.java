package com.tech.rocketmq.transaction;

import com.tech.rocketmq.jms.JmsConfig;
import lombok.extern.slf4j.Slf4j;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.LocalTransactionState;
import org.apache.rocketmq.client.producer.TransactionListener;
import org.apache.rocketmq.client.producer.TransactionMQProducer;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageExt;
import org.springframework.stereotype.Component;

import java.util.concurrent.*;

/**
 * @author lw
 * @since 2021/11/23
 */
@Slf4j
@Component
public class TransactionProducer {
    private String producerGroup = "trac_producer_group";
    //事务监听器
    private TransactionListener transactionListener = new TransactionListenerImpl();
    private TransactionMQProducer producer = null;
    private ExecutorService executorService = new ThreadPoolExecutor(2, 5, 100,
            TimeUnit.SECONDS, new ArrayBlockingQueue<Runnable>(2000),
            new ThreadFactory() {
                @Override
                public Thread newThread(Runnable r) {
                    Thread thread = new Thread(r);
                    thread.setName("client-transaction-msg-check-thread");
                    return thread;
                }
            });

    public TransactionProducer() {
        producer=new TransactionMQProducer(producerGroup);
        producer.setNamesrvAddr(JmsConfig.NAME_SERVER);
        producer.setTransactionListener(transactionListener);
        producer.setExecutorService(executorService);
        start();
    }
    
    public TransactionMQProducer getProducer(){
        return this.producer;
    }

    /**
     * 对象在调用之前必须要调用一次，只能初始化一次
     */
    public void start() {
        try {
            this.producer.start();
        } catch (MQClientException e) {
            e.printStackTrace();
        }
    }

    /**
     * 一般在应用上下文，使用上下文监听器，进行关闭
     */
    public void shutdown(){
        this.producer.shutdown();
    }
    
}

@Slf4j
class TransactionListenerImpl implements TransactionListener {
    
    //在发送消息时发送线程会调用该方法，执行本地事务
    @Override
    public LocalTransactionState executeLocalTransaction(Message msg, Object arg) {
        System.out.println("===================executeLocalTransaction=====================");
        String body = new String(msg.getBody());
        String key = msg.getKeys();
        String transactionId = msg.getTransactionId();
        log.info("transactionId={},key={},body={}",transactionId,key,body);
        //执行本地事务 begin  TODO 
        //执行本地事务 end  TODO 
        int status = Integer.parseInt(arg.toString());
        if(status == 1){
            //已确认状态 对待确认的消息进行确认，消费者可以消费该消息了
            return LocalTransactionState.COMMIT_MESSAGE;
        }
        if(status==2){
            //回滚状态 回滚消息，该状态下的消息会在broker端删除
            return LocalTransactionState.ROLLBACK_MESSAGE;
        }
        if(status==3){
            //未知状态，broker端会调用checkLocalTransaction回查本地事务
            return LocalTransactionState.UNKNOW;
        }
        
        //超时未返回，和未知状态消息一样，也会在等待一定时间后调用checkLocalTransaction回查本地事务
        try {
            TimeUnit.SECONDS.sleep(100);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        return null;
    }

    /**
     * 回查本地事务 根据需要返回事务状态
     * @param msg
     * @return
     */
    @Override
    public LocalTransactionState checkLocalTransaction(MessageExt msg) {
        System.out.println("===============checkLocalTransaction=================");
        String body = new String(msg.getBody());
        String key = msg.getKeys();
        String transactionId = msg.getTransactionId();
        log.info("transactionId={},key={},body={}",transactionId,key,body);
        //要么commit 要么rollback
        //可以根据key去检查本地事务消息是否完成
        return LocalTransactionState.COMMIT_MESSAGE;
    }
}
