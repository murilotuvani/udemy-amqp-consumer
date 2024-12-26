package udemy.rabbitmq.consumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.stereotype.Service;

import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

@Service
public class HelloWorldConsumer {

    private static final Logger LOG = LoggerFactory.getLogger(HelloWorldConsumer.class);

    // O atributo concurrency define o número de threads que serão criadas para consumir as mensagens, no mínimo 3 e no máximo 10.
    @RabbitListener(queues = {"course.hello", "course.fixedrate"}, concurrency = "3-10")
    public void consumeMessage(String message) {
        String threadName = Thread.currentThread().getName();
        LOG.info("Thread {}, Message received: {}", threadName, message);
        try {
            TimeUnit.MILLISECONDS.sleep(ThreadLocalRandom.current().nextLong(2000));
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }
}
