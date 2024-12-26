package udemy.rabbitmq.consumer;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.rabbitmq.client.Channel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
// import org.springframework.amqp.AmqpRejectAndDontRequeueException;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.amqp.support.AmqpHeaders;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.stereotype.Service;
import udemy.rabbitmq.entity.Picture;

import java.io.IOException;


@Service
public class PictureImageTooLargeConsumer {

    private static final Logger LOG = LoggerFactory.getLogger(PictureImageTooLargeConsumer.class);

    @Autowired
    private ObjectMapper objectMapper;

    @RabbitListener(queues = {"q.mypicture.image"})
    public void consumeImage(String message, Channel channel, @Header(AmqpHeaders.DELIVERY_TAG) long tag) throws IOException {
        var picture = objectMapper.readValue(message, Picture.class);
        LOG.info("Image received: {}, ready to thunbnail", picture);
        if(picture.getSize() > 9000) {
            //throw new IllegalArgumentException("Picture size too large: " + picture);

            //throw new AmqpRejectAndDontRequeueException("Picture size too large: " + picture);
            channel.basicReject(tag, false);
        } else {
            LOG.info("Creating thumbnail & publishing: {}", picture);
        }
        channel.basicAck(tag, false);
    }
}
