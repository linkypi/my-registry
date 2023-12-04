package org.hiraeth.govern.server.node.core;

import cn.hutool.core.date.LocalDateTimeUtil;
import lombok.extern.slf4j.Slf4j;
import org.hiraeth.govern.common.domain.MessageType;
import org.hiraeth.govern.server.entity.ServerMessage;
import org.hiraeth.govern.server.entity.ServerRequestType;
import org.hiraeth.govern.server.entity.request.RequestMessage;
import org.hiraeth.govern.server.entity.response.ResponseMessage;

import java.time.LocalDateTime;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * @author: lynch
 * @description: 服务端消息读取队列
 * @date: 2023/12/2 23:39
 */
@Slf4j
public class ServerMessageQueue {

    private static final long WAIT_FRO_TIMEOUT = 5 * 1000L;

    private static class Singleton {
        static ServerMessageQueue instance = new ServerMessageQueue();
    }

    public static ServerMessageQueue getInstance() {
        return ServerMessageQueue.Singleton.instance;
    }

    // messageType -> requestType -> messages
    private static final Map<Integer, Map<Integer, LinkedBlockingQueue<ServerMessage>>> QUEUES = new ConcurrentHashMap<>();

    public void initQueue() {
        for (MessageType messageType : MessageType.values()) {
            Map<Integer, LinkedBlockingQueue<ServerMessage>> map = new ConcurrentHashMap<>();
            for (ServerRequestType requestType : ServerRequestType.values()) {
                map.put(requestType.getValue(), new LinkedBlockingQueue<>());
            }
            QUEUES.put(messageType.getValue(), map);
        }
    }

    public void addMessage(ServerMessage message) {
        int messageType = message.getMessageType().getValue();
        int requestType = message.getRequestType();

        Map<Integer, LinkedBlockingQueue<ServerMessage>> map = QUEUES.get(messageType);
        if(messageType == MessageType.REQUEST.getValue()){
            RequestMessage requestMessage = RequestMessage.parseFrom(message);
            map.get(requestType).add(requestMessage);
        }else{
            ResponseMessage responseMessage = ResponseMessage.parseFrom(message);
            map.get(requestType).add(responseMessage);
        }
    }

    public RequestMessage takeRequestMessage(ServerRequestType serverRequestType){
        ServerMessage serverMessage = takeMessageInternal(MessageType.REQUEST, serverRequestType);
        return (RequestMessage)serverMessage;
    }

    public ResponseMessage takeResponseMessage(ServerRequestType serverRequestType){
        ServerMessage serverMessage = takeMessageInternal(MessageType.RESPONSE, serverRequestType);
        return (ResponseMessage)serverMessage;
    }

    public ServerMessage takeMessageInternal(MessageType messageType, ServerRequestType serverRequestType){
        try {
            return QUEUES.get(messageType.getValue()).get(serverRequestType.getValue()).take();
        }catch (Exception ex){
            log.error("take message from receive queue failed, message type: {}", serverRequestType, ex);
            return null;
        }
    }

    public int countRequestMessage(ServerRequestType serverRequestType){
        return countMessage(MessageType.REQUEST, serverRequestType);
    }

    public int countResponseMessage(ServerRequestType serverRequestType){
        return countMessage(MessageType.RESPONSE, serverRequestType);
    }

    public int countMessage(MessageType messageType, ServerRequestType serverRequestType){
        Map<Integer, LinkedBlockingQueue<ServerMessage>> queueMap = QUEUES.get(messageType.getValue());
        BlockingQueue<ServerMessage> queue = queueMap.get(serverRequestType.getValue());
        if(queue == null){
            return 0;
        }
        return queue.size();
    }

    /**
     * 超市等待消息返回
     * @param serverRequestType
     * @return
     */
    public ServerMessage waitForMessage(MessageType messageType, ServerRequestType serverRequestType) {
        try {
            boolean timeout = false;
            LocalDateTime startTime = LocalDateTimeUtil.now();
            while (countMessage(messageType, serverRequestType) == 0) {
                Thread.sleep(100);
                LocalDateTime now = LocalDateTimeUtil.now();
                if (!LocalDateTimeUtil.between(startTime, now).minusMillis(WAIT_FRO_TIMEOUT).isNegative()) {
                    timeout = true;
                    break;
                }
            }
            if (timeout) {
                return null;
            }
            return QUEUES.get(messageType.getValue()).get(serverRequestType.getValue()).take();
        } catch (Exception ex) {
            log.error("wait response timeout from receive queue failed, message type: {}.", serverRequestType, ex);
            return null;
        }
    }

    public ResponseMessage waitForResponseMessage(ServerRequestType serverRequestType) {
        return (ResponseMessage)waitForMessage(MessageType.RESPONSE, serverRequestType);
    }

    public ServerMessage waitForRequestMessage(ServerRequestType serverRequestType) {
        return waitForMessage(MessageType.REQUEST, serverRequestType);
    }
}
