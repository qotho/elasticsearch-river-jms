Java Messaging Service (JMS) River Plugin for ElasticSearch
===========================================================

The JMS River provides a means of submitting bulk indexing requests to elasticsearch using a JMS queue. 
The format of the messages follows the elasticsearch bulk API format:

	{ "index" : { "_index" : "twitter", "_type" : "tweet", "_id" : "1" } }
	{ "tweet" : { "text" : "this is a tweet" } }
	{ "delete" : { "_index" : "twitter", "_type" : "tweet", "_id" : "2" } }
	{ "create" : { "_index" : "twitter", "_type" : "tweet", "_id" : "1" } }
	{ "tweet" : { "text" : "another tweet" } }    

In order to install the plugin, simply run: `bin/plugin -install iooab23/elasticsearch-river-jms/1.0.0`.

Creating the JMS river is as simple as (all configuration parameters are provided, with default values):

	curl -XPUT 'localhost:9200/_river/my_river/_meta' -d '{
	    "type" : "jms",
	    "jms" : {
	        "providerUrl" : "t3://localhost:7001", 
	        "user" : "guest",
	        "pass" : "guest",
	        "contextFactory" : "weblogic.jndi.WLInitialContextFactory",
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

The river automatically batches queue messages if the queue is overloaded, allowing for faster catchup with the messages streamed into the queue. 

The `ordered` flag indicates whether messages will be indexed in the same order as they arrive in the queue by blocking on the bulk request before picking up the next data to be indexed. It can also be used as a simple way to throttle indexing.

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
