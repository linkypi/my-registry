package org.hiraeth.govern.client;

import static org.junit.Assert.assertTrue;

import com.hiraeth.registry.client.ServiceInstance;
import lombok.extern.slf4j.Slf4j;
import org.hiraeth.registry.common.util.CommonUtil;
import org.junit.Test;

/**
 * Unit test for simple App.
 */
@Slf4j
public class ClientTest
{
    public static void main(String[] args) throws Exception {
        ServiceInstance serviceInstance = new ServiceInstance();
        serviceInstance.init();
        Thread.sleep(10000000);
    }

    @Test
    public void test(){
        int slot = CommonUtil.routeSlot("HeartbeatThread");
        log.info("slot: {}", slot);
    }
}
