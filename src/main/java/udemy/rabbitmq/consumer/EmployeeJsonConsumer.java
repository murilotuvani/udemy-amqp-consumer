package udemy.rabbitmq.consumer;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import udemy.rabbitmq.entity.Employee;

@Service
public class EmployeeJsonConsumer {

    private static final Logger LOG = LoggerFactory.getLogger(EmployeeJsonConsumer.class);

    @Autowired
    private ObjectMapper objectMapper;

    @RabbitListener(queues = {"course.employee"})
    public void consumeMessage(String message) throws JsonProcessingException {
        var employee = objectMapper.readValue(message, Employee.class);
        LOG.info("Employee received: {}", employee);
    }
}
