package fr.titan.formation;

import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.node.Node;
import org.elasticsearch.node.NodeBuilder;

/**
 * User: Titan
 * Date: 29/09/13
 * Time: 00:45
 */
public class ClientConnection {

    private Client client = null;
    private Node node = null;

    public ClientConnection connectRemote(String clusterName)throws Exception{
        if(client!=null){
            throw new Exception("Client already exist, disconnect first");
        }
        Settings settings = ImmutableSettings.settingsBuilder().put("cluster.name",clusterName).build();

        client = new TransportClient(settings).addTransportAddress(new InetSocketTransportAddress("192.168.0.12",9300));
        return this;
    }

    public ClientConnection connectLocal()throws Exception{
        if(node != null || client != null){
            throw new Exception("Client already exist, disconnect first");
        }
        Settings settings = ImmutableSettings.settingsBuilder()
                .put("gateway.type", "none")
                .put("index.gateway.type", "none")
                .put("index.store.type", "memory")
                .put("path.data", "target/data")
                .build();
        node = NodeBuilder.nodeBuilder().settings(settings).local(true).node();
        client = node.client();
        return this;
    }

    public Client getClient()throws Exception{
        if(client == null){
            throw new Exception("Client doesn't exist, create first");
        }
        return this.client;
    }

    public ClientConnection disconnect(){
        if(node!=null){
            node.close();
            node = null;
        }
        if(client!=null){
            client.close();
            client = null;
        }
        return this;
    }


}
