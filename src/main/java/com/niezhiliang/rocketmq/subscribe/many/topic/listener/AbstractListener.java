package com.niezhiliang.rocketmq.subscribe.many.topic.listener;

import lombok.Data;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.spring.core.RocketMQListener;

/**
 * @author nzl
 * @version v
 * @date 2023/2/15
 */
@Data
public abstract class AbstractListener implements RocketMQListener<MessageExt> {

    /**
     * 设置消费的topic，多个以逗号分割
     */
    private String topic;
}
