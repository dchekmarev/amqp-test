package amqptest;

import com.rabbitmq.client.Channel;
import java.io.IOException;
import java.util.List;
import org.quartz.JobDetail;
import org.quartz.Trigger;
import org.springframework.amqp.core.AcknowledgeMode;
import org.springframework.amqp.core.AmqpAdmin;
import org.springframework.amqp.core.AmqpTemplate;
import org.springframework.amqp.core.Binding;
import org.springframework.amqp.core.FanoutExchange;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.Queue;
import org.springframework.amqp.rabbit.connection.CachingConnectionFactory;
import org.springframework.amqp.rabbit.core.ChannelAwareMessageListener;
import org.springframework.amqp.rabbit.core.RabbitAdmin;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.amqp.rabbit.listener.SimpleMessageListenerContainer;
import org.springframework.amqp.rabbit.listener.adapter.MessageListenerAdapter;
import org.springframework.amqp.support.converter.JsonMessageConverter;
import org.springframework.amqp.support.converter.MessageConverter;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.quartz.MethodInvokingJobDetailFactoryBean;
import org.springframework.scheduling.quartz.SchedulerFactoryBean;
import org.springframework.scheduling.quartz.SimpleTriggerBean;

@Configuration
public class Application {

/**
 * dataflow:
 * messageProducer(cron) -> AMQP -> buffer(listener) -> messageConsumer(cron, to emulate delay for waiting groups of messages)
 */

  /**
   * How many messages we can try to process without sending ack/nack
   */
  private int prefetchCount = 2;

  @Bean
  public CachingConnectionFactory amqpConnectionFactory() {
    return new CachingConnectionFactory("localhost");
  }

  @Bean
  public AmqpTemplate amqpTemplate() {
    RabbitTemplate template = new RabbitTemplate(amqpConnectionFactory());
    template.setMessageConverter(messageConverter());
    return template;
  }

  @Bean
  public FanoutExchange testEx() {
    return new FanoutExchange("testEx", true, false);
  }

  @Bean
  public Queue testQueue() {
    return new Queue("testQueue", true);
  }

  @Bean
  public AmqpAdmin amqpAdmin() {
    RabbitAdmin amqpAdmin = new RabbitAdmin(amqpConnectionFactory());
    amqpAdmin.declareExchange(testEx());
    amqpAdmin.declareQueue(testQueue());
    amqpAdmin.declareBinding(new Binding(testQueue(), testEx()));
    return amqpAdmin;
  }

  @Bean
  public MessageConverter messageConverter() {
    return new JsonMessageConverter();
  }

  @Bean
  public SchedulerFactoryBean schedulerFactoryBean() {
    SchedulerFactoryBean schedulerFactoryBean = new SchedulerFactoryBean();
    schedulerFactoryBean.setTriggers(new Trigger[]{simpleTriggerBean(), consumerTriggerBean()});
    return schedulerFactoryBean;
  }

  @Bean
  public SimpleTriggerBean simpleTriggerBean() {
    SimpleTriggerBean trigger = new SimpleTriggerBean();
    trigger.setJobDetail((JobDetail) producerJobDetail().getObject());
    trigger.setRepeatInterval(1000);
    return trigger;
  }

  @Bean
  public MethodInvokingJobDetailFactoryBean producerJobDetail() {
    MethodInvokingJobDetailFactoryBean jobDetails = new MethodInvokingJobDetailFactoryBean();
    jobDetails.setTargetObject(messageProducer());
    jobDetails.setTargetMethod("run");
    return jobDetails;
  }

  @Bean
  public SimpleTriggerBean consumerTriggerBean() {
    SimpleTriggerBean trigger = new SimpleTriggerBean();
    trigger.setJobDetail((JobDetail) consumerJobDetail().getObject());
    trigger.setRepeatInterval(2000);
    return trigger;
  }

  @Bean
  public MethodInvokingJobDetailFactoryBean consumerJobDetail() {
    MethodInvokingJobDetailFactoryBean jobDetails = new MethodInvokingJobDetailFactoryBean();
    jobDetails.setTargetObject(messageConsumer());
    jobDetails.setTargetMethod("run");
    return jobDetails;
  }
  @Bean
  public Runnable messageProducer() {
    return new Runnable() {
      private long idx;
      private final AmqpTemplate amqpTemplate = amqpTemplate();
      private final FanoutExchange testEx = testEx();

      public void run() {
        System.out.println("sending " + (++idx));
        try {
          amqpTemplate.convertAndSend(testEx.getName(), "", new ComplexMessage(idx, "[" + idx + "] hello, world!", "some body text"));
        } catch (Throwable t) {
          t.printStackTrace();
        }
      }
    };
  }

  @Bean
  public SimpleMessageListenerContainer listenerContainer() {
    SimpleMessageListenerContainer container = new SimpleMessageListenerContainer();
    container.setConnectionFactory(amqpConnectionFactory());
    container.setQueueNames(testQueue().getName());
    MessageListener listener = new MessageListener(messageConverter(), buffer());
    MessageListenerAdapter listenerAdapter = new MessageListenerAdapter(listener, messageConverter());
    container.setMessageListener(listenerAdapter);
    container.setAcknowledgeMode(AcknowledgeMode.MANUAL);
    container.setExposeListenerChannel(true);
    container.setPrefetchCount(prefetchCount);
    container.setConcurrentConsumers(2);
    return container;
  }

  @Bean
  public Runnable messageConsumer() {
    return new Runnable() {
      private final InternalBuffer buffer = buffer();
      public void run() {
        try {
          List<Object> messages = buffer.take(100, 500);
          for (Object message : messages) {
            System.out.println("consumed: " + message);
            // todo real processor
          }
          buffer.markProcessed(messages);
          // todo requeue failed
        } catch (InterruptedException e) {
          e.printStackTrace();
        } catch (IOException e) {
          e.printStackTrace();
        }
      }
    };
  }

  @Bean
  public InternalBuffer buffer() {
    return new InternalBuffer();
  }

  class MessageListener implements ChannelAwareMessageListener {
    private final MessageConverter converter;
    private final InternalBuffer buffer;

    public MessageListener(MessageConverter converter, InternalBuffer buffer) {
      this.converter = converter;
      this.buffer = buffer;
    }

    public void onMessage(Message rawMessage, Channel channel) throws Exception {
      ComplexMessage message = (ComplexMessage) converter.fromMessage(rawMessage);
      long deliveryTag = rawMessage.getMessageProperties().getDeliveryTag();
      buffer.add(message, deliveryTag, channel);
    }
  }
}
