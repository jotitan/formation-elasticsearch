package fr.titan.formation;

import junit.framework.Assert;
import org.elasticsearch.client.Client;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static junit.framework.Assert.assertTrue;
import static junit.framework.TestCase.assertNotNull;

/**
 * User: Titan
 * Date: 29/09/13
 * Time: 00:59
 */
public class ClientConnectionTest {
    private final String INDEX_NAME = "index-test";
    private final String INDEX_NAME_DATA = "index-test-width-data";
    private ClientConnection clientConnection;

    @Before
    public void init(){
        clientConnection = new ClientConnection();
    }

    @Test
    public void testConnect()throws Exception{
        //ClientConnection.connectRemote("es-cluster");

       assertNotNull(clientConnection.connectLocal().getClient());
    }

    @Test
    public void testCreateIndex()throws Exception {
        Client client = clientConnection.disconnect().connectLocal().getClient();
        assertTrue("Creation index " + INDEX_NAME, client.admin().indices().prepareCreate(INDEX_NAME).get().isAcknowledged());
        assertTrue("Verification de la creation d'index", client.admin().indices().prepareExists(INDEX_NAME).get().isExists());
    }

    @Test
    public void testAddData()throws Exception{}


    @After
    public void destroy()throws Exception{
        clientConnection.disconnect();
    }

}
