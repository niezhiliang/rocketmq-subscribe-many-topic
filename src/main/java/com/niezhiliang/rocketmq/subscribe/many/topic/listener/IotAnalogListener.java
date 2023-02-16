package com.niezhiliang.rocketmq.subscribe.many.topic.listener;

import lombok.extern.slf4j.Slf4j;
import org.apache.rocketmq.common.message.MessageExt;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.stereotype.Component;

/**
 * @author nzl
 * @version v
 * @date 2023/2/15
 */
@Component
@Slf4j
public class IotAnalogListener extends AbstractListener implements InitializingBean {

    @Override
    public void afterPropertiesSet() throws Exception {
        super.setTopic("iot-analog");
    }

    @Override
    public void onMessage(MessageExt message) {
        String topic = message.getTopic();
        byte[] body = message.getBody();
        String tags = message.getTags();
        String respString = new String(body);
        log.info("topic:{},value:{},tags:{}", topic, respString,tags);
    }
}
