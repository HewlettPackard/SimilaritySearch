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

 
import org.zeromq.ZMQ;
import com.hp.hpl.palette.similarity.coordinator.CoordinatorClient;
import com.hp.hpl.palette.similarity.coordinator.CoordinatorClientImpl;

import java.util.UUID;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
 
import com.hp.hpl.palette.similarity.datamodel.TimeSeriesSearch;
import com.hp.hpl.palette.similarity.datamodel.TimeSeriesSearch.FinalQueryResult;
import com.hp.hpl.palette.similarity.datamodel.TimeSeriesSearch.SearchQuery;

/**
 * The three test classes are formed in the same test suite:
 * 
 *   ClientCoordinatorSimulatedPartitionerClientTest.java for the query submission client
 *   ClientCoordinatorSimualtedPartitionerPartitionerTest.java for the two partitioners (we will need to set the corresponding partitioner number =0, 1) 
 *   and create two instances of the processes from this class.
 *   clientCoordinatorsimulatedPartitionerCoordinatorTest.java for the coordinator (we will need to specify the total partitioner number = 2
 *
 */
public class ClientCoordinatorSimulatedPartitionerClientTest {

	private static final Log LOG = LogFactory.getLog(ClientCoordinatorSimulatedPartitionerClientTest.class.getName());
	
	public static void main(String[] args) { 
		ZMQ.Context context = ZMQ.context(1); //1 is for one single IO thread per socket. 
		String coordinatorIPAddress = "15.25.119.96"; //we test only on a single machine.
	  
		LOG.info("to start the client for the test.....");
		
		CoordinatorClient client = new CoordinatorClientImpl (context,coordinatorIPAddress);
		 
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
	}
}
