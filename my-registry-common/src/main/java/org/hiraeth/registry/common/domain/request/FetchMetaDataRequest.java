package org.hiraeth.registry.common.domain.request;

import lombok.Getter;
import lombok.Setter;
import org.hiraeth.registry.common.domain.RequestType;

/**
 * @author: lynch
 * @description:
 * @date: 2023/11/30 22:30
 */
@Getter
@Setter
public class FetchMetaDataRequest extends Request {

    public FetchMetaDataRequest() {
        super();
        requestType = RequestType.FetchMetaData.getValue();
    }

}
