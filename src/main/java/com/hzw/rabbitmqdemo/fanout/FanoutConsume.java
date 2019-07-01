package com.hzw.rabbitmqdemo.fanout;

import com.hzw.rabbitmqdemo.connection.ConnectionBuilder;
import com.rabbitmq.client.AMQP.BasicProperties;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.DefaultConsumer;
import com.rabbitmq.client.Envelope;
import com.rabbitmq.client.GetResponse;
import com.rabbitmq.client.impl.ConsumerWorkService;
import java.io.IOException;
import java.util.EventListener;

/**
 * @author hzw
 * @date 2019/7/1  2:51 PM
 * @Description:
 */
public class FanoutConsume {

  public static void main(String[] args) throws IOException {
    Connection connection = ConnectionBuilder.newBuilder().build();

    Channel channel = connection.createChannel();
    String queueName = "fanout-test-queue";

    //订阅模式
//    pubModel(channel,queueName);
    //获取消息
//    getMessage(channel,queueName);
    //预取消息
    queueConsume(channel,queueName);


  }

  /**
   * 订阅模式，推送
   * @param channel
   * @param queueName
   * @throws IOException
   */
  public static void pubModel(Channel channel,String queueName) throws IOException {

    //手动ack
    boolean auto_ack = false;

    DefaultConsumer defaultConsumer = new DefaultConsumer(channel) {

      @Override
      public void handleDelivery(String consumerTag, Envelope envelope, BasicProperties properties,
          byte[] body) throws IOException {

        if ("5".equals(properties.getMessageId())){//如果messageID为"5"的话，将这本消息仍会到queue，所以在这或造成重复消费该消息，造成queue堵塞
          channel.basicNack(envelope.getDeliveryTag(),false,true);
        }

        System.out.println(properties.getMessageId());

        System.out.println(new String(body));

        channel.basicAck(envelope.getDeliveryTag(),false);

      }
    };

    channel.basicConsume(queueName,auto_ack,defaultConsumer);
  }

  /**
   * 获取一个消息，拉取
   * @param channel
   * @param queueName
   * @throws IOException
   */
  public static void getMessage(Channel channel,String queueName) throws IOException {
    boolean auto_ack = false;
    GetResponse getResponse = channel.basicGet(queueName, false);

    if (getResponse == null){
      System.out.println("没有message");
    }else{
      BasicProperties props = getResponse.getProps();
      if ("5".equals(props.getMessageId())){
        channel.basicNack(getResponse.getEnvelope().getDeliveryTag(),false,true);
      }else{
        System.out.println(new String(getResponse.getBody()));
        channel.basicAck(getResponse.getEnvelope().getDeliveryTag(),false);
      }
    }

  }

  /**
   * 设置预取消息
   * @param channel
   * @param queueName
   * @throws IOException
   */
  public static void queueConsume(Channel channel,String queueName) throws IOException {
    boolean auto_ack = false;

    //预取消息，第一个参数预取几条，第二个参数预取消息设置的级别，true是channel级别，false消费者级别
    channel.basicQos(2,false);

    DefaultConsumer defaultConsumer = new DefaultConsumer(channel) {

      @Override
      public void handleDelivery(String consumerTag, Envelope envelope, BasicProperties properties,
          byte[] body) throws IOException {

        if ("5".equals(properties.getMessageId())) {
          channel.basicNack(envelope.getDeliveryTag(), false, true);
        } else {
          System.out.println(new String(body));
          channel.basicAck(envelope.getDeliveryTag(), false);
        }
      }
    };
    channel.basicConsume(queueName,auto_ack,defaultConsumer);
  }

}
