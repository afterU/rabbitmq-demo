package com.hzw.rabbitmqdemo.topic;

import com.hzw.rabbitmqdemo.connection.ConnectionBuilder;
import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.BuiltinExchangeType;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.ConfirmListener;
import com.rabbitmq.client.Connection;
import java.io.IOException;
import java.util.concurrent.TimeoutException;

/**
 * @author hzw
 * @date 2019/7/1  4:39 PM
 * @Description:
 */
public class TopicPublish {

  /**
   * 发送方确认模式
   * channel.confirmSelect();//开启发送方确认模式
   *channel.waitForConfirms();//普通发送方确认，阻塞
   *channel.waitForConfirmsOrDie();//批量确认模式
   *channel.addConfirmListener();//异步监听发送方确认模式
   */
  public static void main(String[] args) throws IOException, TimeoutException {

    Connection connection = ConnectionBuilder.newBuilder().build();

    Channel channel = connection.createChannel();

    channel.confirmSelect();//开启发送方确认模式


    channel.close();
    connection.close();

  }

  /**
   * channel.waitForConfirms()  普通发生者确认 模式
   * @param channel
   * @throws IOException
   * @throws InterruptedException
   */
  public static void normalConfirm(Channel channel) throws IOException, InterruptedException {

    String exchangeName = "topic-test-ex";
    String queueName1 = "topic-test-queue1";
    String queueName2 = "topic-test-queue2";
    String queueName3 = "topic-test-queue3";

    String rountingKeys1 = "hello.*";
    String rountingKeys2 = "*.*.kaka";
    String rountingKeys3 = "likaduo.#";

    channel.exchangeDeclare(exchangeName, BuiltinExchangeType.TOPIC,true);
    channel.queueDeclare(queueName1,true,false,false,null);
    channel.queueDeclare(queueName2,true,false,false,null);
    channel.queueDeclare(queueName3,true,false,false,null);

    channel.queueBind(queueName1,exchangeName,rountingKeys1);
    channel.queueBind(queueName2,exchangeName,rountingKeys2);
    channel.queueBind(queueName3,exchangeName,rountingKeys3);

    for (int j = 0; j < 5; j++) {

      byte[] bytes = ("MSG [" + j + "],Hello World!").getBytes();
      int i = j%5;

      if (i==0){
        channel.basicPublish(exchangeName,"hello.likaduo",new AMQP.BasicProperties().builder().messageId(j+"").deliveryMode(2).build(),bytes);
      }else if (i ==1){
        channel.basicPublish(exchangeName,"hello.likaduo.kaka",new AMQP.BasicProperties().builder().messageId(j+"").deliveryMode(2).build(),bytes);
      }else if (i==2){
        channel.basicPublish(exchangeName,"likaduo.kaka.eat",new AMQP.BasicProperties().builder().messageId(j+"").deliveryMode(2).build(),bytes);
      }else if (i==3){
        channel.basicPublish(exchangeName,"kk.hello.likaduo",new AMQP.BasicProperties().builder().messageId(j+"").deliveryMode(2).build(),bytes);
      }else if (i==4){
        channel.basicPublish(exchangeName,"kkk.hello.likaduo.kkk",new AMQP.BasicProperties().builder().messageId(j+"").deliveryMode(2).build(),bytes);
      }

      try {
        if (channel.waitForConfirms()){
          System.out.println("消息发送成功");
        };
      }catch (Exception e){
        System.out.println("消息发送失败");
      }

    }
  }

  public static void batchConfirm(Channel channel) throws IOException, InterruptedException {

    String exchangeName = "topic-test-ex";
    String queueName1 = "topic-test-queue1";
    String queueName2 = "topic-test-queue2";
    String queueName3 = "topic-test-queue3";

    String rountingKeys1 = "hello.*";
    String rountingKeys2 = "*.*.kaka";
    String rountingKeys3 = "likaduo.#";

    channel.exchangeDeclare(exchangeName, BuiltinExchangeType.TOPIC,true);
    channel.queueDeclare(queueName1,true,false,false,null);
    channel.queueDeclare(queueName2,true,false,false,null);
    channel.queueDeclare(queueName3,true,false,false,null);

    channel.queueBind(queueName1,exchangeName,rountingKeys1);
    channel.queueBind(queueName2,exchangeName,rountingKeys2);
    channel.queueBind(queueName3,exchangeName,rountingKeys3);

    for (int j = 0; j < 5; j++) {

      byte[] bytes = ("MSG [" + j + "],Hello World!").getBytes();
      int i = j%5;

      if (i==0){
        channel.basicPublish(exchangeName,"hello.likaduo",new AMQP.BasicProperties().builder().messageId(j+"").deliveryMode(2).build(),bytes);
      }else if (i ==1){
        channel.basicPublish(exchangeName,"hello.likaduo.kaka",new AMQP.BasicProperties().builder().messageId(j+"").deliveryMode(2).build(),bytes);
      }else if (i==2){
        channel.basicPublish(exchangeName,"likaduo.kaka.eat",new AMQP.BasicProperties().builder().messageId(j+"").deliveryMode(2).build(),bytes);
      }else if (i==3){
        channel.basicPublish(exchangeName,"kk.hello.likaduo",new AMQP.BasicProperties().builder().messageId(j+"").deliveryMode(2).build(),bytes);
      }else if (i==4){
        channel.basicPublish(exchangeName,"kkk.hello.likaduo.kkk",new AMQP.BasicProperties().builder().messageId(j+"").deliveryMode(2).build(),bytes);
      }
    }

    try {
      channel.waitForConfirmsOrDie();
      System.out.println("消息全部发送成功");
    }catch (Exception e){
      System.out.println("消息发送失败");
    }
  }
  public static void asyncConfirm(Channel channel) throws IOException {


    String exchangeName = "topic-test-ex";
    String queueName1 = "topic-test-queue1";
    String queueName2 = "topic-test-queue2";
    String queueName3 = "topic-test-queue3";

    String rountingKeys1 = "hello.*";
    String rountingKeys2 = "*.*.kaka";
    String rountingKeys3 = "likaduo.#";

    channel.exchangeDeclare(exchangeName, BuiltinExchangeType.TOPIC,true);
    channel.queueDeclare(queueName1,true,false,false,null);
    channel.queueDeclare(queueName2,true,false,false,null);
    channel.queueDeclare(queueName3,true,false,false,null);

    channel.queueBind(queueName1,exchangeName,rountingKeys1);
    channel.queueBind(queueName2,exchangeName,rountingKeys2);
    channel.queueBind(queueName3,exchangeName,rountingKeys3);

    for (int j = 0; j < 5; j++) {

      byte[] bytes = ("MSG [" + j + "],Hello World!").getBytes();
      int i = j%5;

      if (i==0){
        channel.basicPublish(exchangeName,"hello.likaduo",new AMQP.BasicProperties().builder().messageId(j+"").deliveryMode(2).build(),bytes);
      }else if (i ==1){
        channel.basicPublish(exchangeName,"hello.likaduo.kaka",new AMQP.BasicProperties().builder().messageId(j+"").deliveryMode(2).build(),bytes);
      }else if (i==2){
        channel.basicPublish(exchangeName,"likaduo.kaka.eat",new AMQP.BasicProperties().builder().messageId(j+"").deliveryMode(2).build(),bytes);
      }else if (i==3){
        channel.basicPublish(exchangeName,"kk.hello.likaduo",new AMQP.BasicProperties().builder().messageId(j+"").deliveryMode(2).build(),bytes);
      }else if (i==4){
        channel.basicPublish(exchangeName,"kkk.hello.likaduo.kkk",new AMQP.BasicProperties().builder().messageId(j+"").deliveryMode(2).build(),bytes);
      }
    }

    //代码是异步执行的，消息确认有可能是批量确认的，是否批量确认在于返回的multiple的参数，
    // 此参数为bool值，如果true表示批量执行了deliveryTag这个值以前的所有消息，如果为false的话表示单条确认
    channel.addConfirmListener(new ConfirmListener() {
      @Override
      public void handleAck(long deliveryTag, boolean multiple) throws IOException {
        if (multiple){
          System.out.println(deliveryTag+",之前的所有消息确认提交");
        }else{
          System.out.println(deliveryTag+",一个消息确认提交");
        }
      }

      @Override
      public void handleNack(long deliveryTag, boolean multiple) throws IOException {
        if (multiple){
          System.out.println(deliveryTag+",之后的所有消息提交失败");
        }else{
          System.out.println(deliveryTag+",一个消息提交失败");
        }
      }
    });
  }

}
