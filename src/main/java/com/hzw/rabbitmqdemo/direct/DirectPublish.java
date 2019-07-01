package com.hzw.rabbitmqdemo.direct;

import com.hzw.rabbitmqdemo.connection.ConnectionBuilder;
import com.hzw.rabbitmqdemo.fanout.FanoutConsume;
import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.AMQP.BasicProperties;
import com.rabbitmq.client.BuiltinExchangeType;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import java.io.IOException;
import java.util.concurrent.TimeoutException;

/**
 * @author hzw
 * @date 2019/7/1  3:47 PM
 * @Description:
 */
public class DirectPublish {

  /**
   * 消息生产者启动，事务
   * channel.txSelect();//声明启动事务模式
   * channel.txCommit();//提交事务
   * channel.txRollback();//回滚事务
   */
  public static void main(String[] args) throws IOException, TimeoutException {

    long beginTime = System.currentTimeMillis();
    Connection connection = ConnectionBuilder.newBuilder().build();

    Channel channel = connection.createChannel();



    String exchangeName = "direct-test-ex";
    String queueNameOdd = "direct-test-queue-odd";
    String queueNameEven = "direct-test-queue-even";


    //减少测试的干扰
    channel.exchangeDelete(exchangeName);
    channel.queueDelete(queueNameOdd);
    channel.queueDelete(queueNameEven);


    channel.exchangeDeclare(exchangeName, BuiltinExchangeType.DIRECT,true);
    channel.queueDeclare(queueNameOdd,true,false,false,null);//奇数队列
    channel.queueDeclare(queueNameEven,true,false,false,null);//偶数队列

    channel.queueBind(queueNameOdd,exchangeName,"odd");
    channel.queueBind(queueNameEven,exchangeName,"even");

    //发送一万条消息耗时：75128ms
//    havaTX(channel,exchangeName);
    //发送一万条消息耗时：731ms
    noHavaTX(channel,exchangeName);

    channel.close();
    connection.close();

    long endTime = System.currentTimeMillis();

    System.out.println("发送一万条消息耗时："+(endTime - beginTime)+"ms");


  }

  /**
   * 开启事务
   * @param channel
   * @param exchangeName
   * @throws IOException
   */
  public static void havaTX(Channel channel,String exchangeName) throws IOException {

    channel.txSelect();//声明启动事务模式

    for (int i = 0; i < 10000; i++) {
      int tag = i%2;
      byte[] bytes = ("MSG [" + i + "],Hello World!").getBytes();

      if (0==tag){
        try {
          channel.basicPublish(exchangeName,"even",new AMQP.BasicProperties.Builder().deliveryMode(2).messageId(i+"").build(),bytes);
          channel.txCommit();//提交事务
        }catch (Exception e){
          channel.txRollback();//回滚事务
        }
      }else{
        try {
          channel.basicPublish(exchangeName,"odd",new AMQP.BasicProperties.Builder().deliveryMode(2).messageId(i+"").build(),bytes);
          channel.txCommit();//提交事务
        }catch (Exception e){
          channel.txRollback();//回滚事务
        }
      }

    }
  }

  /**
   * 不开启事务
   * @param channel
   * @param exchangeName
   * @throws IOException
   */
  public static void noHavaTX(Channel channel,String exchangeName) throws IOException {
    for (int i = 0; i < 10000; i++) {
      int tag = i%2;

      byte[] bytes = ("MSG [" + i + "],Hello World!").getBytes();

      if (0==tag){
        channel.basicPublish(exchangeName,"even",new AMQP.BasicProperties.Builder().deliveryMode(2).messageId(i+"").build(),bytes);
      }else{
        channel.basicPublish(exchangeName,"odd",new AMQP.BasicProperties.Builder().deliveryMode(2).messageId(i+"").build(),bytes);
      }

    }
  }
}
