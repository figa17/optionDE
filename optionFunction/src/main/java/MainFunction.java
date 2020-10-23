/**
 * Created by Felipe González Alfaro on 22-10-20.
 */

import com.google.cloud.functions.BackgroundFunction;
import com.google.cloud.functions.Context;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import pojo.GcsEvent;
import publisher.PublisherPubSub;

import java.io.IOException;

public class MainFunction implements BackgroundFunction<GcsEvent> {

    private static final Logger logger = LoggerFactory.getLogger(MainFunction.class);

    @Override
    public void accept(GcsEvent event, Context context) throws IOException, InterruptedException {

        PublisherPubSub pubSub = new PublisherPubSub();

        logger.info("Event: " + context.eventId());
        logger.info("Event Type: " + context.eventType());
        logger.info("Bucket: " + event.getBucket());
        logger.info("File: " + event.getName());
        logger.info("Metageneration: " + event.getMetageneration());
        logger.info("Created: " + event.getTimeCreated());
        logger.info("Updated: " + event.getUpdated());

        String path = event.getBucket() + "/" + event.getName();
        pubSub.publishWithErrorHandlerExample(path);

    }
}