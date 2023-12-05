package org.hiraeth.govern.server.entity.response;

import cn.hutool.core.bean.BeanUtil;
import lombok.Getter;
import lombok.Setter;
import org.hiraeth.govern.common.domain.MessageType;
import org.hiraeth.govern.common.util.CommonUtil;
import org.hiraeth.govern.server.entity.ServerMessage;

import java.nio.ByteBuffer;

/**
 * @author: lynch
 * @description:
 * @date: 2023/12/4 21:00
 */
@Getter
@Setter
public class ResponseMessage extends ServerMessage {
    // 响应时使用， 标识请求是否已处理完成
    private boolean success;
    private int code;
    private String errorMessage;

    public ResponseMessage(){
        super();
        messageType = MessageType.RESPONSE;
    }

    public void buildBuffer() {
        int strLength = CommonUtil.getStrLength(errorMessage);
        toBuffer(8 + strLength);
    }

    @Override
    protected void writePayload(ByteBuffer buffer) {
        writeBoolean(success);
        writeStr(errorMessage);
    }

    public static ResponseMessage parseFrom(ServerMessage message){
        ResponseMessage responseMessage = BeanUtil.copyProperties(message, ResponseMessage.class);

        ByteBuffer buffer = message.getBuffer();
        boolean success = CommonUtil.readBoolean(buffer);
        String msg = CommonUtil.readStr(buffer);

        responseMessage.setMessageType(MessageType.RESPONSE);
        responseMessage.setErrorMessage(msg);
        responseMessage.setSuccess(success);
        return responseMessage;
    }
}
