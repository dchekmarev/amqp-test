package amqptest;

import com.rabbitmq.client.Channel;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

public class InternalBuffer {
  class InternalMessage {
    final Object message;
    final long deliveryTag;
    final Channel channel;
    InternalMessage(Object message, long deliveryTag, Channel channel) {
      this.message = message;
      this.deliveryTag = deliveryTag;
      this.channel = channel;
    }
  }

  private final ConcurrentMap<Object, InternalMessage> messageProperties = new ConcurrentHashMap<Object, InternalMessage>();
  private final BlockingQueue<Object> messages = new LinkedBlockingQueue<Object>();

  public void add(Object message, long deliveryTag, Channel channel) {
    messageProperties.put(message, new InternalMessage(message, deliveryTag, channel));
    messages.add(message);
  }

  public List<Object> take(int maxIds, long timeout) throws InterruptedException {
    return take(maxIds, timeout, TimeUnit.MILLISECONDS);
  }

  public List<Object> take(int maxIds, long timeout, TimeUnit unit) throws InterruptedException {
    Date deadLine = new Date(System.currentTimeMillis() + unit.toMillis(timeout));
    if (messages.size() == 0) {
      return Collections.EMPTY_LIST;
    }
    List<Object> ret = new ArrayList<Object>(Math.min(maxIds, messages.size()));
    while (maxIds-- > 0) {
      Object o = messages.poll();
      if (o != null) {
        ret.add(o);
      }
    }
    return ret;
  }

  public void markProcessed(List<Object> messages) throws IOException {
    for (Object message : messages) {
      InternalMessage internalMessage = messageProperties.remove(message);
      if (internalMessage != null) {
        internalMessage.channel.basicAck(internalMessage.deliveryTag, false);
      }
    }
  }

  public void markFailed(List<Object> messages) throws IOException {
    for (Object message : messages) {
      InternalMessage internalMessage = messageProperties.remove(message);
      if (internalMessage != null) {
        internalMessage.channel.basicNack(internalMessage.deliveryTag, false, true);
      }
    }
  }

}
