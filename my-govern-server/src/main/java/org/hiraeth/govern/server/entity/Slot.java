package org.hiraeth.govern.server.entity;

import lombok.Getter;
import lombok.Setter;

/**
 * @author: lynch
 * @description:
 * @date: 2023/11/30 17:18
 */
@Getter
@Setter
public class Slot {
    private int index;

    public Slot(int index){
        this.index = index;
    }
}
