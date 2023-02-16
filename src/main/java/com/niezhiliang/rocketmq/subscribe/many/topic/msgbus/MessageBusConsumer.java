package com.niezhiliang.rocketmq.subscribe.many.topic.msgbus;

import com.niezhiliang.rocketmq.subscribe.many.topic.listener.AbstractListener;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.message.MessageExt;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.util.List;

/**
 * @author nzl
 * @version v
 * @date 2023/2/15
 */
@Component
@Slf4j
public class MessageBusConsumer {

    @Value("${consumer.topic}")
    private List<String> consumerTopic;

    @Value("${rocketmq.name-server}")
    private String nameServer;

    @Value("${rocketmq.consumer.group}")
    private String consumerGroup;
    @Autowired
    private List<AbstractListener> listeners;


    /**
     * 消息订阅回调
     */
    private final MessageListenerConcurrently msgListener = (msgs, context) -> {
        for (MessageExt messageExt : msgs) {
            log.debug("topic:{},value:{}", messageExt.getTopic(), new String(messageExt.getBody()));
            try {
                for (AbstractListener listener : listeners) {
                    String topic = messageExt.getTopic();
                    String topicOfListener = listener.getTopic();
                    if(StringUtils.isEmpty(topicOfListener)){
                        continue;
                    }
                    if(topicOfListener.contains(topic)) {
                        listener.onMessage(messageExt);
                    }
                }
            } catch (Exception e) {
                log.error("consume message failed. messageExt:{}", messageExt, e);
                return ConsumeConcurrentlyStatus.RECONSUME_LATER;
            }
        }
        return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
    };


    @PostConstruct
    public void start() throws MQClientException {

        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer(consumerGroup);
        consumer.setNamesrvAddr(nameServer);
        consumer.registerMessageListener(msgListener);
        for (String topic : consumerTopic) {
            if (StringUtils.isNotBlank(topic)) {
                consumer.subscribe(topic.trim(), "*");
            }
        }
        consumer.start();
    }
}
