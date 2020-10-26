package com.option.test.pubsub;

import com.option.test.service.DataService;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.cloud.gcp.pubsub.core.PubSubTemplate;
import org.springframework.cloud.gcp.pubsub.integration.AckMode;
import org.springframework.cloud.gcp.pubsub.integration.inbound.PubSubInboundChannelAdapter;
import org.springframework.cloud.gcp.pubsub.support.BasicAcknowledgeablePubsubMessage;
import org.springframework.cloud.gcp.pubsub.support.GcpPubSubHeaders;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.PropertySource;
import org.springframework.integration.annotation.ServiceActivator;
import org.springframework.integration.channel.DirectChannel;
import org.springframework.messaging.MessageChannel;
import org.springframework.messaging.MessageHandler;

/**
 * Created by Felipe González Alfaro on 22-10-20.
 */
@Configuration
@PropertySource("classpath:application.properties")
public class PubSubConnect {

    private static final String STORAGE_SCHEMA = "gs://";

    private static final Log LOGGER = LogFactory.getLog(PubSubConnect.class);

    @Value("${app.subscriptionname}")
    private String subscriptionName;


    @Bean
    public PubSubInboundChannelAdapter messageChannelAdapter(@Qualifier("inputChannel") MessageChannel inputChannel,
                                                             PubSubTemplate pubSubTemplate) {

        LOGGER.info("Subscription Name: " + this.subscriptionName);
        PubSubInboundChannelAdapter adapter = new PubSubInboundChannelAdapter(pubSubTemplate, this.subscriptionName);
        adapter.setOutputChannel(inputChannel);
        adapter.setAckMode(AckMode.MANUAL);

        return adapter;
    }

    @Bean
    public MessageChannel inputChannel() {
        return new DirectChannel();
    }


    @Bean
    @ServiceActivator(inputChannel = "inputChannel")
    public MessageHandler getPath(DataService dataService) {
        return message -> {
            String payload = new String((byte[]) message.getPayload());
            LOGGER.info("Message Payload: " + payload);

            BasicAcknowledgeablePubsubMessage originalMessage = message.getHeaders()
                    .get(GcpPubSubHeaders.ORIGINAL_MESSAGE, BasicAcknowledgeablePubsubMessage.class);
            originalMessage.ack();
            dataService.processData(STORAGE_SCHEMA + payload);
        };
    }

}
