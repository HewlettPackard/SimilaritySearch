/**
“© Copyright 2017  Hewlett Packard Enterprise Development LP
Redistribution and use in source and binary forms, with or without modification, are permitted provided that the following conditions are met:
1. Redistributions of source code must retain the above copyright notice, this list of conditions and the following disclaimer.
2. Redistributions in binary form must reproduce the above copyright notice, this list of conditions and the following disclaimer in the documentation and/or other materials provided with the distribution.
3. Neither the name of the copyright holder nor the names of its contributors may be used to endorse or promote products derived from this software without specific prior written permission.
THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO,
WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE FOR ANY
DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF
USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING
NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.”
*/
package com.hp.hpl.palette.similarity.worker;

import java.util.UUID;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.zeromq.ZMQ;

import com.hp.hpl.palette.similarity.coordinator.CoordinatorClient;
import com.hp.hpl.palette.similarity.coordinator.CoordinatorClientImpl;
import com.hp.hpl.palette.similarity.datamodel.TimeSeriesSearch;
import com.hp.hpl.palette.similarity.datamodel.TimeSeriesSearch.FinalQueryResult;
import com.hp.hpl.palette.similarity.datamodel.TimeSeriesSearch.SearchQuery;

/**
 * The three classes are formed the same test suite to test how the query is submitted, and then the coordinator simulated the final query response
 * 
 * class: CoordinatedClientWithQueryAcceptorAndResultDistributor.java provides the simualted query result producer, which will subscribe to the query 
 * request that is supposed to distribute to the partitioners, and then send the response via the in-proc push channel back to the simulated coordinator.
 * 
 * class: CoordinatedClientWithQueryAcceptorAndResultDistributorClientTest  submits a single query or a batch of the quries, and wait for the response. 
 * 
 * class: CoordinatedClientWithQueryAcceptorAndResultDistributorServerTest accepts the request, and respond with the simulated final query result. 
 *
 */
public class CoordinatedClientWithQueryAcceptorAndResultDistributorClientTest {

	private static final Log LOG = LogFactory.getLog(CoordinatedClientWithQueryAcceptorAndResultDistributorClientTest.class.getName());
	
	public static void main(String[] args) {
		ZMQ.Context context = ZMQ.context(1); //1 is for one single IO thread per socket. 
		String coordinatorIPAddress = "15.25.119.96"; //we test only on a single machine.
	  
		CoordinatorClient client = new CoordinatorClientImpl (context,coordinatorIPAddress);
		
		//NOTE: either choose  a single query, or otherwise to choose multiple queries, to do the testing. 
		//as otherwise, the single query will be blocked, and no multiple quries' query submission can be started, as the single query's
		//wait-for-query-result blocks the entire thread. 
		/*
		{ //a single query 
			SearchQuery query = null;
			{
				String id= UUID.randomUUID().toString();
	        	float[] queryPattern = new float[5];
	        	for (int j=0; j<5;j++) {
	        		queryPattern[j] = (float)j;
	        	}
	        	
	        	int topK = 5;
	        	query = new TimeSeriesSearch.SearchQuery (id, queryPattern, topK);
	        	
			}
			
			client.submitQuery(query);
			
			LOG.info("query search id: " + query.getQuerySearchId());
			 
			FinalQueryResult result =client.retrieveQueryResult(query);
			
			LOG.info("a single query result returned with id: " + result.getQuerySearchId());
			LOG.info("a single query result returned topK number: " + result.getSearchResults().size());
		}
		*/
		//next a multiple of such queries 
		{
			int count = 100;
			SearchQuery queries[] = new SearchQuery[count];
			
			for (int i=0; i<count;i++) {
				SearchQuery item = null;
				{
					String id= UUID.randomUUID().toString();
		        	float[] queryPattern = new float[5];
		        	for (int j=0; j<5;j++) {
		        		queryPattern[j] = (float)j;
		        	}
		        	
		        	int topK = 5;
		        	item = new TimeSeriesSearch.SearchQuery (id, queryPattern, topK);
		        	
				}
				queries[i] = item;
			}
			
			client.submitQueries(queries);
			FinalQueryResult results[] = client.retrieveQueryResults(queries);
			
			LOG.info("=============================issued queries=======================================");
			LOG.info("total number of the query results returned is: " + results.length);
			for (int i=0; i<count; i++) {
				LOG.info("query search id: " + queries[i].getQuerySearchId());
				 
			}
		
			LOG.info("=============================actual returned query results=======================================");
			for (int i=0; i<count; i++) {
				
				LOG.info("multipe batched queries result returned with id: " + results[i].getQuerySearchId());
				LOG.info("multipe batched queriesreturned topK number: " + results[i].getSearchResults().size());
			}
		}
	}
}
