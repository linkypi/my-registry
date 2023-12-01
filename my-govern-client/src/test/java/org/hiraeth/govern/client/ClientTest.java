package org.hiraeth.govern.client;

import static org.junit.Assert.assertTrue;

import com.hiraeth.govern.client.ClientServer;
import org.junit.Test;

import java.io.IOException;

/**
 * Unit test for simple App.
 */
public class ClientTest
{
    public static void main(String[] args) throws Exception {
        ClientServer clientServer = new ClientServer();
        clientServer.init();
        Thread.sleep(10000000);
    }
}
