Java Messaging Service (JMS) River Plugin for ElasticSearch
===========================================================

The JMS River provides a means of submitting bulk indexing requests to elasticsearch using a JMS queue. 
The format of the messages follows the elasticsearch bulk API format:

	{ "index" : { "_index" : "twitter", "_type" : "tweet", "_id" : "1" } }
	{ "tweet" : { "text" : "this is a tweet" } }
	{ "delete" : { "_index" : "twitter", "_type" : "tweet", "_id" : "2" } }
	{ "create" : { "_index" : "twitter", "_type" : "tweet", "_id" : "1" } }
	{ "tweet" : { "text" : "another tweet" } }    

The river automatically batches queue messages.  It reads messages from the queue until it either has the maximum number of messages configured by the bulkSize setting, or the bulkTimeout has been reached and no more messages are in the queue. All collected messages are submitted as a single bulk request.

Installation
------------
In order to install the plugin, simply run: 

	bin/plugin -url http://bit.ly/10e749R -install elasticsearch-river-jms`

Creating the JMS river in elasticsearch is as simple as:

	curl -XPUT 'localhost:9200/_river/my_river/_meta' -d '{
	    "type" : "jms",
	    "jms" : {
	        "jndiProviderUrl" : "t3://localhost:7001", 
	        "jndiContextFactory" : "weblogic.jndi.WLInitialContextFactory",
	        "user" : "guest",
	        "pass" : "guest",
	        "connectionFactory" : "jms/ElasticSearchConnFactory",
	        "sourceType" : "queue",
	        "sourceName" : "jms/elasticsearch",
	        "consumerName" : "elasticsearch",
	        "durable" : true,
	        "filter" : "JMSCorrelationID = 'someid'"
	    },
	    "index" : {
	        "bulkSize" : 100,
	        "bulkTimeout" : "10ms",
	        "ordered" : false
	    }
	}'
	
Configuration Settings
----------------------

- jndiProviderUrl: URL of the JNDI directory used to lookup the connection factory and queues/topics.
- jndiContextFactory: The type of initial JNDI context factory.
- user: The user name to be used for a secure JNDI connection.
- pass: The password to be used for a secure JNDI connection.
- connectionFactory: The JMS connection factory.
- sourceType: Indicates whether the source is either a "queue" or "topic". 
- sourceName: The queue or topic name.
- consumerName: The name of the consumer or subscriber.
- durable: Indicates whether the consumer is a durable subscriber.  This option is only available got topics.
- filter: A message selector to only dequeue messages that match the given expression.
- bulkSize: The maximum batch size (bulk actions) the river will submit to elasticsearch.
- bulkTimeout: The length of time the river will wait for new messages to add to a batch.
- ordered: Indicates whether the river should submit the bulk actions in the order it got them from the queue.  This setting can also be used a simple way to throttle indexing.

License
-------

    This software is licensed under the Apache 2 license, quoted below.

    Copyright 2012 ElasticSearch <http://www.elasticsearch.org>

    Licensed under the Apache License, Version 2.0 (the "License"); you may not
    use this file except in compliance with the License. You may obtain a copy of
    the License at

        http://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing, software
    distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
    WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
    License for the specific language governing permissions and limitations under
    the License.
