package org.hiraeth.govern.client;

import static org.junit.Assert.assertTrue;

import com.hiraeth.govern.client.ServiceInstance;

/**
 * Unit test for simple App.
 */
public class ClientTest
{
    public static void main(String[] args) throws Exception {
        ServiceInstance serviceInstance = new ServiceInstance();
        serviceInstance.init();
        Thread.sleep(10000000);
    }
}
