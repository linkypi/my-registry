package org.hiraeth.govern.common.domain.response;

import lombok.Getter;

/**
 * @author: lynch
 * @description:
 * @date: 2023/12/5 15:44
 */
@Getter
public enum ResponseStatusCode {
    Electing("1001", "Cluster Electing");

    private String code;
    private String desc;
    ResponseStatusCode(String code, String desc){
        this.code = code;
        this.desc = desc;
    }


}
