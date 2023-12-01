package org.hiraeth.govern.common.domain;

import lombok.Getter;
import lombok.Setter;

/**
 * @author: lynch
 * @description:
 * @date: 2023/11/30 11:52
 */
@Getter
@Setter
public class SlotRang{
    private int start;
    private int end;
    public SlotRang(int start,int end){
        this.start = start;
        this.end = end;
    }
}
