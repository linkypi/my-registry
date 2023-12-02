package org.hiraeth.govern.common.domain;

import com.alibaba.fastjson.JSON;
import lombok.Getter;
import lombok.Setter;

import java.nio.ByteBuffer;

import static org.hiraeth.govern.common.constant.Constant.REQUEST_HEADER_LENGTH;

/**
 * @author: lynch
 * @description:
 * @date: 2023/11/30 23:12
 */
@Getter
@Setter
public class BaseResponse {
    protected RequestType requestType;
    protected boolean success;
    protected long requestId;
    protected long timestamp;

    protected ByteBuffer buffer;

    public BaseResponse(){}

    public BaseResponse(RequestType requestType, boolean success){
        this.requestType = requestType;
        this.success = success;
    }

    protected void writePayload(){
    }

    protected void toBuffer(int payloadLength) {
        int length = 24 + REQUEST_HEADER_LENGTH + payloadLength;
        buffer = ByteBuffer.allocate(length);

        buffer.putInt(length);
        buffer.putInt(requestType.getValue());
        buffer.putInt(success?1:0);
        buffer.putLong(requestId);
        buffer.putLong(System.currentTimeMillis());
        writePayload();
        buffer.flip();
    }

    public Response toResponse() {
        toBuffer(0);
        return new Response(requestType, requestId, buffer);
    }

    public static BaseResponse parseFromBuffer(ByteBuffer buffer) {
        int type = buffer.getInt();
        RequestType requestType = RequestType.of(type);

        int sucInt = buffer.getInt();

        BaseResponse request = new BaseResponse();
        request.success = sucInt == 1;
        request.requestId = buffer.getLong();
        request.timestamp = buffer.getLong();
        request.buffer = buffer;
        request.requestType = requestType;
        return request;
    }
}
