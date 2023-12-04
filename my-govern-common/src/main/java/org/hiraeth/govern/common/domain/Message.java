package org.hiraeth.govern.common.domain;

import com.alibaba.fastjson.annotation.JSONField;
import lombok.Getter;
import lombok.Setter;
import org.hiraeth.govern.common.domain.request.Request;
import org.hiraeth.govern.common.domain.response.Response;

import java.nio.ByteBuffer;

import static org.hiraeth.govern.common.constant.Constant.REQUEST_HEADER_LENGTH;

/**
 * @author: lynch
 * @description: 客户端与服务器通信的消息基础组件
 * @date: 2023/12/3 9:47
 */
@Getter
@Setter
public class Message {
    protected MessageType messageType;
    protected int requestType;
    protected long requestId;
    protected long timestamp;

    @JSONField(serialize = false)
    protected ByteBuffer buffer;

    public Message(){
        this.timestamp = System.currentTimeMillis();
    }

    protected void writePayload(){
    }

    protected void buildBufferInternal(int payloadLength){
        int length = 24 + REQUEST_HEADER_LENGTH + payloadLength;
        buffer = ByteBuffer.allocate(length);

        buffer.putInt(length);
        buffer.putInt(messageType.getValue());
        buffer.putInt(requestType);
        buffer.putLong(requestId);
        buffer.putLong(System.currentTimeMillis());

        writePayload();

        buffer.flip();
    }

    public Response toResponse() {
        return Response.toResponse(buffer);
    }

    public Request toRequest() {
        return Request.toRequest(buffer);
    }
}
