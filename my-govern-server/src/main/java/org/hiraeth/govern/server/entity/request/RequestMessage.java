package org.hiraeth.govern.server.entity.request;

import cn.hutool.core.bean.BeanUtil;
import lombok.Getter;
import lombok.Setter;
import org.hiraeth.govern.common.domain.MessageType;
import org.hiraeth.govern.server.entity.ServerMessage;

/**
 * @author: lynch
 * @description:
 * @date: 2023/12/4 21:00
 */
@Getter
@Setter
public class RequestMessage extends ServerMessage {
    public RequestMessage(){
        super();
        messageType = MessageType.REQUEST;
    }

    public void buildBuffer(int length) {
        toBuffer(length);
    }

    public void buildBuffer() {
        toBuffer(0);
    }

    public static RequestMessage parseFrom(ServerMessage message){
        return BeanUtil.copyProperties(message, RequestMessage.class);
    }
}
