package udemy.rabbitmq.consumer;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import udemy.rabbitmq.entity.Picture;

@Service
public class PictureImageConsumer {

    private static final Logger LOG = LoggerFactory.getLogger(PictureImageConsumer.class);

    @Autowired
    private ObjectMapper objectMapper;

    @RabbitListener(queues = {"q.picture.image"})
    public void consumeImage(String message) throws JsonProcessingException {
        var picture = objectMapper.readValue(message, Picture.class);
        LOG.info("Image received: {}, ready to thunbnail", picture);
    }

    @RabbitListener(queues = {"q.picture.vector"})
    public void consumeVector(String message) throws JsonProcessingException {
        var picture = objectMapper.readValue(message, Picture.class);
        LOG.info("Vector image received: {}, converting", picture);
    }

    @RabbitListener(queues = {"q.picture.image", "q.picture.vector", "q.picture.filter", "q.picture.log"})
    public void consumeEverything(Message message) throws JsonProcessingException {
        var jsonString = new String(message.getBody());

        LOG.info("Consuming: {} with rougint key : {}", jsonString, message.getMessageProperties().getReceivedRoutingKey());
    }
}
