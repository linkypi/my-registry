package org.hiraeth.govern.common.domain;

import lombok.Getter;
import lombok.Setter;

import java.nio.ByteBuffer;

/**
 * @author: lynch
 * @description:
 * @date: 2023/11/30 22:29
 */
@Getter
@Setter
public class BaseRequest {

    protected RequestType requestType;
    protected long requestId;
    protected long timestamp;

    protected ByteBuffer buffer;

    protected void writePayload(){
    }

    protected void toBuffer(int payloadLength) {
        int length = 24 + payloadLength;
        buffer = ByteBuffer.allocate(length);
        buffer.putInt(length);
        buffer.putInt(requestType.getValue());
        buffer.putLong(requestId);
        buffer.putLong(System.currentTimeMillis());
        writePayload();
        buffer.flip();
    }

    public static BaseRequest parseFromBuffer(ByteBuffer buffer) {
        int type = buffer.getInt();
        RequestType requestType = RequestType.of(type);

        BaseRequest request = new BaseRequest();
        request.requestId = buffer.getLong();
        request.timestamp = buffer.getLong();
        request.buffer = buffer;
        request.requestType = requestType;
        return request;
    }
}
