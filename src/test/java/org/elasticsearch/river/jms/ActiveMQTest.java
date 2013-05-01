package org.elasticsearch.river.jms;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.TransportConnector;
import org.elasticsearch.action.ListenableActionFuture;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.indices.IndexMissingException;
import org.elasticsearch.node.Node;
import org.elasticsearch.node.NodeBuilder;
import org.junit.Assert;
import org.junit.Test;

import javax.jms.*;
import java.io.IOException;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

public class ActiveMQTest {

    final String message = "{ \"index\" : { \"_index\" : \"test\", \"_type\" : \"type1\", \"_id\" : \"1\" }\n" +
            "{ \"type1\" : { \"field1\" : \"value1\" } }\n" +
            "{ \"delete\" : { \"_index\" : \"test\", \"_type\" : \"type1\", \"_id\" : \"2\" } }\n" +
            "{ \"create\" : { \"_index\" : \"test\", \"_type\" : \"type1\", \"_id\" : \"1\" }\n" +
            "{ \"type1\" : { \"field1\" : \"value1\" } }";

    BrokerService broker;
    Client client;

    private void startActiveMQBroker() throws Exception {
        broker = new BrokerService();
        broker.setUseJmx(true);
        broker.addConnector("tcp://localhost:61616");
        broker.start();
    }

    private void stopActiveMQBroker() throws Exception {
        for (TransportConnector c : broker.getTransportConnectors()) {
            try {
                c.stop();
                broker.removeConnector(c);
            }
            catch (Exception e) {
                e.printStackTrace();
            }
        }

        broker.getBroker().stop();
    }

    private void startElasticSearchInstance() throws IOException {
        Node node = NodeBuilder.nodeBuilder().settings(
        		ImmutableSettings.settingsBuilder().put("gateway.type", "none")).node();
        client = node.client();
        client.prepareIndex("_river", "test1", "_meta").setSource(
        		jsonBuilder()
        			.startObject()
        				.field("type", "jms")
        			.endObject()
        		).execute().actionGet();
    }

    private void stopElasticSearchInstance() {
        System.out.println("shutting down elasticsearch");
        client.admin().cluster().prepareNodesShutdown().execute();
        client.close();
    }


    @Test
    public void testSimpleScenario() throws Exception {
        startActiveMQBroker();
        startElasticSearchInstance();

        // assure that the index is not yet there
        try {
            ListenableActionFuture future = client.prepareGet("test", "type1", "1").execute();
            future.actionGet();
            Assert.fail();
        } catch (IndexMissingException idxExcp) {

        }


        // TODO connect to the ActiveMQ Broker and publish a message into the default queue
        ActiveMQConnectionFactory factory = new ActiveMQConnectionFactory();

        try {
            Connection conn = factory.createConnection();
            Session session = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
            Queue queue = session.createQueue(JmsRiver.defaultSourceName);
            MessageProducer producer = session.createProducer(queue);
            producer.send(session.createTextMessage(message));

            session.close();
            conn.close();
        } catch (JMSException e) {
            Assert.fail("JMS Exception");
        }

        Thread.sleep(3000l);

        {
            ListenableActionFuture future = client.prepareGet("test", "type1", "1").execute();
            Object o = future.actionGet();
            GetResponse resp = (GetResponse) o;
            Assert.assertEquals("{ \"type1\" : { \"field1\" : \"value1\" } }", resp.getSourceAsString());
        }


        stopElasticSearchInstance();
        stopActiveMQBroker();

        Thread.sleep(3000l);
    }
}
