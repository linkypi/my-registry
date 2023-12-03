package org.hiraeth.govern.common.domain;

import lombok.Getter;
import lombok.Setter;

import java.nio.ByteBuffer;

/**
 * @author: lynch
 * @description:
 * @date: 2023/11/30 23:12
 */
@Getter
@Setter
public class Response extends Message {

    protected boolean success;

    public Response(){
        messageType = MessageType.RESPONSE;
    }

    public Response(RequestType requestType, boolean success){
        this.requestType = requestType;
        this.success = success;
        messageType = MessageType.RESPONSE;
    }

    @Override
    protected void writePayload() {
        buffer.putInt(success ? 1 : 0);
    }

    public void buildBuffer() {
        buildBufferInternal(4);
    }

    public static Response toResponse(ByteBuffer buffer) {
        int type = buffer.getInt();
        RequestType requestType = RequestType.of(type);

        Response response = new Response();
        response.requestId = buffer.getLong();
        response.timestamp = buffer.getLong();

        int sucInt = buffer.getInt();
        response.success = sucInt == 1;
        response.buffer = buffer;
        response.requestType = requestType;
        return response;
    }
}
