package org.hiraeth.govern.common.domain;

import cn.hutool.core.bean.BeanUtil;
import lombok.Getter;
import lombok.Setter;
import org.hiraeth.govern.common.snowflake.SnowFlakeIdUtil;

import java.nio.ByteBuffer;

/**
 * @author: lynch
 * @description:
 * @date: 2023/11/30 22:30
 */
@Getter
@Setter
public class FetchMetaDataRequest extends BaseRequest {

    public FetchMetaDataRequest() {
        requestType = RequestType.FetchMetaData;
        requestId = SnowFlakeIdUtil.getNextId();
    }

    public Request toRequest() {
        toBuffer(0);
        return new Request(requestType, requestId, buffer);
    }

    public static FetchMetaDataRequest parseFrom(ByteBuffer buffer) {
        BaseRequest request = BaseRequest.parseFromBuffer(buffer);
        return BeanUtil.copyProperties(request, FetchMetaDataRequest.class);
    }
}
