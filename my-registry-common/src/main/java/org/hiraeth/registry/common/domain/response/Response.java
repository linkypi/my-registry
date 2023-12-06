package org.hiraeth.registry.common.domain.response;

import lombok.Getter;
import lombok.Setter;
import org.hiraeth.registry.common.domain.Message;
import org.hiraeth.registry.common.domain.MessageType;
import org.hiraeth.registry.common.domain.RequestType;
import org.hiraeth.registry.common.util.CommonUtil;

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
    protected String statusCode;

    public Response() {
        messageType = MessageType.RESPONSE;
    }

    public Response(String statusCode) {
        this.statusCode = statusCode;
        messageType = MessageType.RESPONSE;
    }

    public Response(RequestType requestType, boolean success) {
        this.requestType = requestType.getValue();
        this.success = success;
        messageType = MessageType.RESPONSE;
    }

    @Override
    protected void writePayload() {
        buffer.putInt(success ? 1 : 0);
        CommonUtil.writeStr(buffer, statusCode);
    }

    public void buildBuffer() {
        buildBufferInternal(8 + CommonUtil.getStrLength(statusCode));
    }

    public static Response toResponse(ByteBuffer buffer) {
        int type = buffer.getInt();

        Response response = new Response();
        response.requestId = buffer.getLong();
        response.timestamp = buffer.getLong();

        int sucInt = buffer.getInt();
        String code = CommonUtil.readStr(buffer);
        response.success = sucInt == 1;
        response.buffer = buffer;
        response.requestType = type;
        response.statusCode = code;
        return response;
    }
}
