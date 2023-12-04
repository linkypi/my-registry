package org.hiraeth.govern.common.domain.response;

import lombok.Getter;
import lombok.Setter;
import org.hiraeth.govern.common.domain.Message;
import org.hiraeth.govern.common.domain.MessageType;
import org.hiraeth.govern.common.domain.RequestType;

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
        this.requestType = requestType.getValue();
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

        Response response = new Response();
        response.requestId = buffer.getLong();
        response.timestamp = buffer.getLong();

        int sucInt = buffer.getInt();
        response.success = sucInt == 1;
        response.buffer = buffer;
        response.requestType = type;
        return response;
    }
}
