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

package com.hp.hpl.palette.similarity.coordinator;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

import java.util.HashMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.zeromq.ZMQ;

import com.hp.hpl.palette.similarity.datamodel.TimeSeriesSearch.FinalQueryResult;
import com.hp.hpl.palette.similarity.datamodel.TimeSeriesSearch.SearchQuery;
import com.hp.hpl.palette.similarity.configuration.CoordinatorPortConfiguration;
 

public class CoordinatorClientImpl implements CoordinatorClient {
	private String coordinatorIPAddress; 
	private ZMQ.Context context; 
	private ZMQ.Socket  querySubmissionSocket; 
	private ZMQ.Socket  queryResultRetrievalSocket; 
	

	private static final Log LOG = LogFactory.getLog(CoordinatorClientImpl.class.getName());
	
	public CoordinatorClientImpl (ZMQ.Context context, String coordinatorIPAddress) {
		this.context = context;
		this.coordinatorIPAddress = coordinatorIPAddress;
		
		//create a socket to the aggregator
		this.querySubmissionSocket = this.context.socket(ZMQ.REQ);
		String querySubmissionSocketAddress = "tcp://" + this.coordinatorIPAddress + ":" 
		                                        + (new Integer (CoordinatorPortConfiguration.QUERY_REQUEST_ACCEPTOR_PORT)).toString();
		this.querySubmissionSocket.connect(querySubmissionSocketAddress);
		LOG.info("Coordinator Client connects to address (REQ): " + querySubmissionSocketAddress);
		
		//create a subscriber socket for the subscription. 
        this.queryResultRetrievalSocket = this.context.socket(ZMQ.SUB);
        String queryResultRetrievalSocketAddress =  "tcp://" + this.coordinatorIPAddress + ":" 
                                                + (new Integer (CoordinatorPortConfiguration.QUERY_RESULT_DISTRIBUTION_PORT)).toString();
        this.queryResultRetrievalSocket.connect(queryResultRetrievalSocketAddress);
        LOG.info("Coordinator Client connects to address (SBU): " + queryResultRetrievalSocketAddress);
		//this.queryResultRetrievalSocket.subscribe(queryId.getBytes());
    	//we do not have the selective filtering of the search queries at this time.
    	this.queryResultRetrievalSocket.subscribe(new byte[0]);
	}
	
	
	@Override
	public void submitQuery(SearchQuery query) {
		
    	byte[] message = null;
    	{
    		ByteArrayOutputStream out = new ByteArrayOutputStream();
   	    	DataOutputStream dataOut = new DataOutputStream (out);
   	    	try {
   	          query.write(dataOut);
   	    	  dataOut.close();
   	    	}
   	    	catch(IOException ex) {
   	    		LOG.error ("fails to serialize the aggegrator's search query information", ex);
   	    	}
   	    
   	    	
   	    	message = out.toByteArray();
    	}
    	
    	LOG.info("client submit query in bytes with length of: " + message.length);
    	boolean status = this.querySubmissionSocket.send(message, 0, message.length, 0); 
    	if (!status) {
    		LOG.error("fails to submit the query to the coordinator");
    		return;
    	}
    	
     
    	//wait for the REQ's response. 
    	byte[] response= this.querySubmissionSocket.recv(0);
    	if (response!=null) {
    		LOG.info("client query submission gets acknowledged.");
    	}
    	else{
    		LOG.error("client query submission does not receive proper acknowledgement");
    		return; 
    	}
 
	}

	
	/**
	 * NOTE: we are not sure at this time, whether the following sleep is necessary, as the publisher  result will not arrive instantaneously
     * Answer: no need. 
     * 
     * NOTE: at this time, we find that it is easier to just have the client to subscribe to every thing, and then let our LSH framework to do the filtering
     * based on the search id. For batching of the search Id, we will do it seperately. 
     * 
	 * but it may be good for us to prepare the test case at the API testing level.
	 * try {
	 *	   Thread.sleep(100);
	 *	}
	 *	 catch (Exception e) {
	 *		 ///do nothing. 
	 *   }
	 */
	@Override
	public FinalQueryResult retrieveQueryResult(SearchQuery query) {
		FinalQueryResult result  = null;
		 //  Initialize poll set
        ZMQ.Poller items = new ZMQ.Poller (1);
        items.register(this.queryResultRetrievalSocket, ZMQ.Poller.POLLIN);
        //  Process messages from both sockets
        while (!Thread.currentThread ().isInterrupted ()) {
            byte[] message = null;
            items.poll();
            if (items.pollin(0)) {
               message = this.queryResultRetrievalSocket.recv(0);
               result=processQueryResult(message, query);
               if (result != null) {
                  break; //we get our result now.. 
               }
            }    
        }
        
        return result;
	}
	
	private FinalQueryResult processQueryResult (byte[] message, SearchQuery query) {
	    FinalQueryResult result  = new FinalQueryResult();
        try {
            ByteArrayInputStream in = new ByteArrayInputStream(message);
   	        DataInputStream dataIn = new DataInputStream(in);
   	        
   	        result.readFields(dataIn);
   	        dataIn.close();
        
        }
        catch (Exception ex) {
        	LOG.error("fails to receive/reconstruct the final query result", ex);
        }
       
       //I only return when the query id matches my query id. 
       String finalResultQueryId = result.getQuerySearchId();
       String expectedQueryId = query.getQuerySearchId();
       if (finalResultQueryId.compareTo(expectedQueryId) == 0) {
             return result;
       }
       else {
    	   return null;
       }
	}


	@Override
	public void submitQueries(SearchQuery[] queries) {
		//this is the non-blocking call. just to submit the query one-by-one at this time
		if ((queries != null) && (queries.length > 0)) {
			for (int i=0; i<queries.length;i++) {
				this.submitQuery(queries[i]);
			}
		}
	}


	@Override
	public FinalQueryResult[] retrieveQueryResults(SearchQuery[] queries) {
		FinalQueryResult results[] = null;
		if ((queries != null) && (queries.length > 0)) {
			 HashMap <String, FinalQueryResult> resultTracker  = new HashMap <String, FinalQueryResult> ();
			 for (int i=0; i<queries.length;i++) {
				 //populate what we will expect in terms of the search id. 
				 resultTracker.put(queries[i].getQuerySearchId(), new FinalQueryResult()); 
			 }
		     results = new FinalQueryResult[queries.length];
			 //  Initialize poll set
	         ZMQ.Poller items = new ZMQ.Poller (1);
	         items.register(this.queryResultRetrievalSocket, ZMQ.Poller.POLLIN);
	         //  Process messages from both sockets
	         int countForResponseReceived = 0;
	         while (!Thread.currentThread ().isInterrupted ()) {
	           byte[] message = null;
	           items.poll();
	           if (items.pollin(0)) {
	              message = this.queryResultRetrievalSocket.recv(0);
	              //processQueryResult takes care that only the search id that matches one of the registered search queries
	              //defined in the result racker will be returned.
	              FinalQueryResult result= processQueryResult(message, resultTracker);
	              if (result != null) {
	            	 resultTracker.put(result.getQuerySearchId(), result);
	            	 countForResponseReceived ++;
	              }
	           }  
	           
	           if (countForResponseReceived == queries.length) {
	        	  
	        	  //follow the same sequence as that is submitted. 
	        	  for (int i=0; i<queries.length;i++) {
	        		  String queryId= queries[i].getQuerySearchId();
	        		  FinalQueryResult val = resultTracker.get(queryId);
	        		  results[i]=val;
	        	  }
	        	   
	        	   
	        	  break; //we are done. 
	           }
	       }
		}
	
       return results;
	}
	

	/**
	 *processQueryResult takes care that only the search id that matches one of the registered search queries
	 *defined in the result racker will be returned.
	 * @param message contains the search result.
	 * @param resultTracker contains the registered search queries.
	 * @return if the search id matches one of the registered search queries result, we will return it. otherwise, return null.
	 * so it is a filtering mechanism.
	 */
	private FinalQueryResult processQueryResult (byte[] message, HashMap <String, FinalQueryResult> resultTracker) {
	    FinalQueryResult result  = new FinalQueryResult();
        try {
            ByteArrayInputStream in = new ByteArrayInputStream(message);
   	        DataInputStream dataIn = new DataInputStream(in);
   	        
   	        result.readFields(dataIn);
   	        dataIn.close();
        
        }
        catch (Exception ex) {
        	LOG.error("fails to receive/reconstruct the final query result", ex);
        }
       
        //I only return when the query id matches my query id. 
        String finalResultQueryId = result.getQuerySearchId();
        FinalQueryResult expectedPlaceHolder = resultTracker.get(finalResultQueryId);
        if (expectedPlaceHolder!= null)  {
             return result; //that is one of the queries that we expected.
        }
        else {
    	   return null; //that is not what we expect for this client.
        }
	}


}
