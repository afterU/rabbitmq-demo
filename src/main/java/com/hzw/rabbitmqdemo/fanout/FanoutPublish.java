package com.hzw.rabbitmqdemo.fanout;

import com.hzw.rabbitmqdemo.connection.ConnectionBuilder;
import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.AMQP.Queue.DeleteOk;
import com.rabbitmq.client.BuiltinExchangeType;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import java.io.IOException;
import java.util.concurrent.TimeoutException;

/**
 * @author hzw
 * @date 2019/7/1  2:17 PM
 * @Description:
 */
public class FanoutPublish {

  public static void main(String[] args) throws IOException, TimeoutException {
    Connection connection = ConnectionBuilder.newBuilder().build();

    Channel channel = connection.createChannel();

    String exchangeName = "fanout-test-ex";
    String queueName = "fanout-test-queue";

    channel.exchangeDeclare(exchangeName, BuiltinExchangeType.FANOUT,true);

    channel.queueDeclare(queueName,true,false,false,null);

    channel.queueBind(queueName,exchangeName,"");

    for (int i = 0; i < 10; i++) {
      channel.basicPublish(exchangeName,"",new AMQP.BasicProperties.Builder().deliveryMode(2).messageId(i+"").build(),("msg["+i+"],Hello world! ").getBytes());
    }

    channel.close();
    connection.close();


  }

}
