package com.hzw.rabbitmqdemo.direct;

import com.hzw.rabbitmqdemo.connection.ConnectionBuilder;
import com.rabbitmq.client.AMQP.BasicProperties;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.DefaultConsumer;
import com.rabbitmq.client.Envelope;
import java.io.IOException;
import java.util.EventListener;

/**
 * @author hzw
 * @date 2019/7/1  4:17 PM
 * @Description:
 */
public class DirectConsume {

  public static void main(String[] args) throws IOException {
    Connection connection = ConnectionBuilder.newBuilder().build();

    Channel channel = connection.createChannel();

    String queueName = "direct-test-queue-odd";

    channel.basicQos(2,false);

    channel.txSelect();//声明开启事务

//    hand_ackAndTx(channel,queueName);

    auto_ackAndTx(channel,queueName);


  }

  /**
   * 自动ack，发生回滚，回滚无效
   * @param channel
   * @param queueName
   * @throws IOException
   */
  public static void auto_ackAndTx(Channel channel,String queueName) throws IOException {
    DefaultConsumer defaultConsumer = new DefaultConsumer(channel) {

      @Override
      public void handleDelivery(String consumerTag, Envelope envelope, BasicProperties properties,
          byte[] body) throws IOException {
        try {
          System.out.println(new String(body));
          int tag = 1 / 0;
          channel.txCommit();
        } catch (Exception e) {
          channel.txRollback();
        }
      }
    };
    channel.basicConsume(queueName,true,defaultConsumer);
  }

  /**
   * 如果手动ack，确认ack了，在回滚的话，回滚有效，消息会返回到队列
   * @param channel
   * @param queueName
   * @throws IOException
   */
  public static void hand_ackAndTx(Channel channel,String queueName) throws IOException {
    DefaultConsumer defaultConsumer = new DefaultConsumer(channel) {

      @Override
      public void handleDelivery(String consumerTag, Envelope envelope, BasicProperties properties,
          byte[] body) throws IOException {
        try {
          System.out.println(new String(body));
          channel.basicAck(envelope.getDeliveryTag(),false);
          int tag = 1 / 0;
          channel.txCommit();
        } catch (Exception e) {
          channel.txRollback();
        }
      }
    };
    channel.basicConsume(queueName,false,defaultConsumer);
  }

}
