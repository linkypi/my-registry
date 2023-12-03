package org.hiraeth.govern.common.domain;

import cn.hutool.core.bean.BeanUtil;
import lombok.Getter;
import lombok.Setter;
import org.hiraeth.govern.common.snowflake.SnowFlakeIdUtil;
import org.hiraeth.govern.common.util.CommonUtil;

import java.nio.ByteBuffer;

/**
 * @author: lynch
 * @description:
 * @date: 2023/12/2 16:42
 */
@Getter
@Setter
public class SubscribeRequest extends Request {
    private String serviceName;
    public SubscribeRequest(String serviceName){
        super();
        this.serviceName = serviceName;
        this.requestType = RequestType.Subscribe;
    }

    public void buildBuffer() {
        int length = 4 + CommonUtil.getStrLength(serviceName);
        buildBufferInternal(length);
    }

    @Override
    protected void writePayload() {
        CommonUtil.writeStr(buffer, serviceName);
    }

    private static SubscribeRequest getRequest(ByteBuffer buffer, Request request) {

        String serviceName = CommonUtil.readStr(buffer);
        SubscribeRequest subscribeRequest = BeanUtil.copyProperties(request, SubscribeRequest.class);
        subscribeRequest.serviceName = serviceName;
        return subscribeRequest;
    }

    public static SubscribeRequest parseFrom(Request request) {
        ByteBuffer buffer = request.getBuffer();
        return getRequest(buffer, request);
    }
}
