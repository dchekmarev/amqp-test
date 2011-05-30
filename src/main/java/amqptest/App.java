package amqptest;

import java.util.Timer;
import java.util.TimerTask;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.context.support.AbstractApplicationContext;

/**
 * spring context bootstrap
 */
public class App {
  public static void main(String[] args) {
    final ApplicationContext ctx = new AnnotationConfigApplicationContext(Application.class);
    final Timer timer = new Timer();
    timer.schedule(new TimerTask() {
                     @Override
                     public void run() {
                       ((AbstractApplicationContext) ctx).close();
                       timer.cancel();
                     }
                   }, 10000);
  }
}
