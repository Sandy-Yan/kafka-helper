package com.github.andy.kafka.consumer;

/**
 * 消费数据接口
 *
 * @author andy
 * @date 2016年12月18日 上午1:09
 */
public interface IMessageConsumer {

    /**
     * 接收数据
     *
     * @param msg 数据
     */
    void receiveMessage(String msg);
}
