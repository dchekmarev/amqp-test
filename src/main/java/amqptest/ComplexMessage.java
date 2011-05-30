package amqptest;

import org.springframework.util.StringUtils;

public class ComplexMessage {
  private long id;
  private String subject;
  private String body;
  public ComplexMessage() {}
  public ComplexMessage(long id, String subject, String body) {
    this.id = id;
    this.subject = subject;
    this.body = body;
  }
  public long getId() {
    return id;
  }
  public void setId(long id) {
    this.id = id;
  }
  public String getSubject() {
    return subject;
  }
  public void setSubject(String subject) {
    this.subject = subject;
  }
  public String getBody() {
    return body;
  }
  public void setBody(String body) {
    this.body = body;
  }
  public String toString() {
    return "ComplexMessage(" + id + ", " + StringUtils.quote(subject) + ", " + StringUtils.quote(body) + ")";
  }
}
