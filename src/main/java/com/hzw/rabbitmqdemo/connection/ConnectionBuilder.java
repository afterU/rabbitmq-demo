package com.hzw.rabbitmqdemo.connection;

import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import java.io.IOException;
import java.util.concurrent.TimeoutException;

/**
 * @author hzw
 * @date 2019/6/30  4:45 PM
 * @Description:
 */
public class ConnectionBuilder {

  public static ConnectionBuilder newBuilder(){

    return new ConnectionBuilder();
  }

  public Connection build(){
    ConnectionFactory connectionFactory = new ConnectionFactory();

    connectionFactory.setHost("127.0.0.1");
    connectionFactory.setPort(5672);
    connectionFactory.setUsername("guest");
    connectionFactory.setPassword("guest");
    connectionFactory.setVirtualHost("/");

    try {
      return connectionFactory.newConnection();
    } catch (IOException e) {
      e.printStackTrace();
    } catch (TimeoutException e) {
      e.printStackTrace();
    }

    return null;
  }

}
