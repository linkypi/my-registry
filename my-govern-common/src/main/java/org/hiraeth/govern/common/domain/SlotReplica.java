package org.hiraeth.govern.common.domain;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.Setter;
import org.hiraeth.govern.common.domain.SlotRange;


/**
 * @author: lynch
 * @description:
 * @date: 2023/12/4 17:17
 */
@Getter
@Setter
@AllArgsConstructor
public class SlotReplica {
    private String nodeId;

    private SlotRange slotRange;

    public SlotReplica(){
    }
}
