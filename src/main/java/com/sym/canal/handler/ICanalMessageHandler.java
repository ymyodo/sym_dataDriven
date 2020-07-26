package com.sym.canal.handler;

import com.sym.canal.message.CanalMessage;

import java.util.List;

/**
 * @author shenyanming
 * @date 2020/7/26 13:06.
 */
@FunctionalInterface
public interface ICanalMessageHandler {

    /**
     * 由 canal client 解析监听解析出来数据库变动数据
     * @param messageList 消息体
     */
    void resolve(List<CanalMessage> messageList);
}
