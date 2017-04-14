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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.List;
import java.util.ArrayList;
import java.util.AbstractMap.SimpleEntry;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.zeromq.ZMQ;

import com.hp.hpl.palette.similarity.configuration.SupportedSearchCategory;
import com.hp.hpl.palette.similarity.coordinator.CoordinatorMaster;
import com.hp.hpl.palette.similarity.coordinator.CoordinatorMasterImpl;
import com.hp.hpl.palette.similarity.datamodel.TimeSeriesSearch;
import com.hp.hpl.palette.similarity.datamodel.TimeSeriesSearch.SearchResult;
import com.hp.hpl.palette.similarity.configuration.CoordinatorPortConfiguration;

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
public class CoordinatedClientWithQueryAcceptorAndResultDistributor {

	private static final Log LOG = LogFactory.getLog(CoordinatedClientWithQueryAcceptorAndResultDistributor.class.getName());
	
	public static class SimulatedQueryResultProducerThread extends Thread {
		private ZMQ.Context context;
		private ZMQ.Socket queryResultDistributorSocket;
		private  String coordinatorMasterAddress; 
		
		public SimulatedQueryResultProducerThread (ZMQ.Context context, String coordinatorMasterAddress ) {
			this.context = context;
		    this.coordinatorMasterAddress = coordinatorMasterAddress;
		}
		
		
		public void run() {
			
			LOG.info("starting to launch the Simulated Query Result Producer Thread.....");
			
			//wait for the query to be accpeted.
			ZMQ.Socket queryReceiver = context.socket(ZMQ.SUB);
		    int queryDistributionPort = CoordinatorPortConfiguration.QUERY_DISTRIBUTION_PORT;
		    String destinationQueryDistributorAddress = "tcp://" + this.coordinatorMasterAddress
		    		                                          +":" + ( new Integer(queryDistributionPort)).toString();
		    queryReceiver.connect(destinationQueryDistributorAddress);
		    ///subscription topic to be with zero length, that is, to subscribe to any message.
	        queryReceiver.subscribe(new byte[0]); 
	        
	        TimeSeriesSearch.SearchQuery query = null;
	        //  Initialize poll set
	        ZMQ.Poller items = new ZMQ.Poller (1);
	        items.register(queryReceiver, ZMQ.Poller.POLLIN);
	        //  Process messages from both sockets. we are in the loop always. 
	        while (!Thread.currentThread ().isInterrupted ()) {
	            byte[] message = null;
	            items.poll();
	            if (items.pollin(0)) {
	               message = queryReceiver.recv(0);
	               query=processQueryResult(message);
	               prodcueFinalSearchResult(query);
	            }    
	        }
	        
	        {
	        	 LOG.info("in Simulated Query Result Producer, receive the following query:");
	        	 //display the search query that it gets tapped 
	        	 LOG.info("query search id (in simulated query result producer): " + query.getQuerySearchId());
				 LOG.info("query pattern with size of (in simulated query result producer): " + query.getQueryPattern().length);
				 for (int i=0; i<query.getQueryPattern().length;i++) {
					  LOG.info("..." + query.getQueryPattern()[i]);
				 }
				 LOG.info("top k number specification (in simulated query result producer):" + query.getTopK());
	        	
	        }
			

		}
	
		private void prodcueFinalSearchResult(TimeSeriesSearch.SearchQuery query) {
			//bind to the push of the InterProc port
			this.queryResultDistributorSocket= this.context.socket(ZMQ.PUSH);
			String queryResultDistributorAddress="inproc://"  + "finalqueryresult.ipc";
			this.queryResultDistributorSocket.connect(queryResultDistributorAddress);
			
			try {
				Thread.sleep(500);  //simulate the query processing time. 
			} 
			catch(Exception ex) {
				//do nothing. 
			}
			 
		    //prepare for the final result.
			String queryId = query.getQuerySearchId();
			List<SimpleEntry<Integer, SearchResult>> results = new ArrayList<SimpleEntry<Integer, SearchResult>>();
			{
				for (int i=0; i<query.getTopK();i++) {
					
					SearchResult sr = new SearchResult();
					sr.distance= (float) 1000.0;
					sr.id=1;
					sr.offset=2; 
					int partition = 100;
					SimpleEntry<Integer, SearchResult> element = new SimpleEntry<Integer, SearchResult> (new Integer(partition), sr);
					results.add(element);
				}
			}
			
			TimeSeriesSearch.FinalQueryResult finalResult = new TimeSeriesSearch.FinalQueryResult (queryId, results);
			
			//now push the final result to the inproc of the QueryResultDistributor thread. 
			byte[] message = null;
			//do the de-serialization
			{
        		ByteArrayOutputStream out = new ByteArrayOutputStream();
	   	    	DataOutputStream dataOut = new DataOutputStream (out);
	   	    	try {
	   	    	  finalResult.write(dataOut);
	   	    	  dataOut.close();
	   	    	}
	   	    	catch(IOException ex) {
	   	    		LOG.error ("fails to serialize the aggegrator's abandonment command information", ex);
	   	    	}
	   	    
	   	    	
	   	    	message = out.toByteArray();
        	}
			this.queryResultDistributorSocket.send(message, 0, message.length, 0); 
			
			LOG.info("in Simulated Query Result Producer, produce final query result with message length: " + message.length);
		}
		
		
		private TimeSeriesSearch.SearchQuery processQueryResult(byte[] message) {
			TimeSeriesSearch.SearchQuery query = new TimeSeriesSearch.SearchQuery();
	   	     try {
	   	            
		   	        ByteArrayInputStream in = new ByteArrayInputStream(message);
		   	        DataInputStream dataIn = new DataInputStream(in);
		   	        
		   	        query.readFields(dataIn);
		   	        dataIn.close();
	   	        
	   	      }
	   	      catch (Exception ex) {
	   	        	LOG.error("fails to receive/reconstruct the query information", ex);
	   	      }
	   	        
	   	        
	   	      LOG.info("the query received from the partitioner is the following for thread:...." 
	   	    	                                                               + Thread.currentThread().getId());
	   	      LOG.info("query search id: " + query.getQuerySearchId());
			  LOG.info("query pattern with size of: " + query.getQueryPattern().length);
			  for (int i=0; i<query.getQueryPattern().length;i++) {
				  LOG.info("..." + query.getQueryPattern()[i]);
			  }
			  LOG.info("top k number specification:" + query.getTopK());
			  
			  return query;
		}
	}
	
	
	public static class SimulatedCoordinatorInAThread extends Thread {
	 
		private int partitions;
		String privateIPAddress;
		LSHIndexConfigurationParameters  parameters;
		ZMQ.Context context;
		
		public SimulatedCoordinatorInAThread (ZMQ.Context context, int partitions, 
				    String privateIPAddress, LSHIndexConfigurationParameters  parameters){
			this.context = context;
			this.partitions = partitions;
			this.privateIPAddress = privateIPAddress;
			this.parameters = parameters;
		}
		
		public void run() {
			boolean simulated=false; //we are doing the simulated partitioner
			SupportedSearchCategory.SupportedSearch chosenSearch = SupportedSearchCategory.SupportedSearch.LSH_SEARCH; 
			CoordinatorMasterImpl server= 
					 new CoordinatorMasterImpl(context, partitions, privateIPAddress, parameters, chosenSearch, simulated);
			//added initialization of the service.
	     	server.init();
        	server.startService();
		}
	}
 
	
}
