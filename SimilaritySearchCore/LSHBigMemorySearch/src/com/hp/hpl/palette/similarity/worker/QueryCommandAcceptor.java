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

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.lang.management.ManagementFactory;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.zeromq.ZMQ;

import com.hp.hpl.palette.similarity.coordinator.CoordinatorMaster;
import com.hp.hpl.palette.similarity.datamodel.TimeSeriesSearch;

import com.hp.hpl.palette.similarity.datamodel.ProcessingUnitRuntimeInformation;
import com.hp.hpl.palette.similarity.configuration.CoordinatorPortConfiguration;

/**
 * this will be run in a single thread launched by the ManagedHostService  
 * to accept queries and commands for each partitioner from the coordinator master. 
 * 
 * @author Jun Li
 *
 */
public class QueryCommandAcceptor {

	private ZMQ.Context context;
	private MultiSearchManager searchManager;
	private String coordinatorMasterAddress;
	private String privateIPAddress;
	
	private static final Log LOG = LogFactory.getLog(QueryCommandAcceptor.class.getName());
	
	
	public static class QueryCommandAcceptorThread extends Thread {
		private ZMQ.Context context;
		private MultiSearchManager searchManager;
	 
		private String coordinatorMasterAddress;
	    private String privateIPAddress;
		
		public QueryCommandAcceptorThread (ZMQ.Context context, String coordinatorMasterAddress, 
				                                 String privateIPAddress, MultiSearchManager searchManager){
			this.context = context;
			this.coordinatorMasterAddress = coordinatorMasterAddress;
			this.privateIPAddress=privateIPAddress;
			this.searchManager = searchManager;
		}
		
		public void run() {
			LOG.info("Query Command Acceptor is starting in the coordinator .....");
			QueryCommandAcceptor  acceptor =
					new QueryCommandAcceptor (this.context, this.coordinatorMasterAddress, this.privateIPAddress, this.searchManager);
			acceptor.acceptQueriesAndCommands();
		}
	}
	
	
	
	public QueryCommandAcceptor (ZMQ.Context context, String coordinatorMasterAddress, String privateIPAddress, MultiSearchManager searchManager) {
		this.coordinatorMasterAddress = coordinatorMasterAddress;
		this.privateIPAddress = privateIPAddress;
		this.context=context;
		this.searchManager=searchManager;
	}
	
	
	/**
	 * to do the actual processing on accept the query and then send them to the search manager's processing queue, and also the abandonment commands.
	 * 
	 */
	public void acceptQueriesAndCommands() {
		    ZMQ.Socket queryReceiver = context.socket(ZMQ.SUB);
		    int queryDistributionPort = CoordinatorPortConfiguration.QUERY_DISTRIBUTION_PORT;
		    String destinationQueryDistributorAddress = "tcp://" + this.coordinatorMasterAddress
		    		                                          +":" + ( new Integer(queryDistributionPort)).toString();
		    queryReceiver.connect(destinationQueryDistributorAddress);
		    ///subscription topic to be with zero length, that is, to subscribe to any message.
	        queryReceiver.subscribe(new byte[0]); 
	        
	        
	        ZMQ.Socket abandonmentCommand = context.socket(ZMQ.SUB);
	        int commandDistributionPort = CoordinatorPortConfiguration.QUERY_COMMAND_DISTRIBUTION_PORT;
	        String destinationCommandDistributorAddress = "tcp://" + this.coordinatorMasterAddress
                                                                  +":" + ( new Integer(commandDistributionPort)).toString();
	        abandonmentCommand.connect(destinationCommandDistributorAddress);
		    ///subscription topic to be with zero length, that is, to subscribe to any message.
	        abandonmentCommand.subscribe(new byte[0]);
	  
	        //then move to the next step, to wait for query commands and the early abandonment commands
	        
	        //  Initialize poll set
	        ZMQ.Poller items = new ZMQ.Poller (2);
	        items.register(queryReceiver, ZMQ.Poller.POLLIN);
	        items.register(abandonmentCommand, ZMQ.Poller.POLLIN);
	         

	        //  Process messages from both sockets
	        while (!Thread.currentThread ().isInterrupted ()) {
	            byte[] message = null;
	            items.poll();
	            if (items.pollin(0)) {
	               message = queryReceiver.recv(0);
	               processQuery(message);
	            }
	            else if (items.pollin(1)){
	               message = abandonmentCommand.recv(0);
	               processAbandonmentCommand(message);
	            }
	             
	        }
	 }
 
	 
	 private void processQuery (byte[] message) {
		 //we are using polling here. 
		 if (LOG.isDebugEnabled()) {
		    LOG.debug("partitioner: " + this.searchManager.getPartitionId() + " receives query related bytes with length of: " + message.length);
		 }
		 
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
	        
	      if (LOG.isDebugEnabled()) {  
		      LOG.debug("the query received from the partitioner: " + this.searchManager.getPartitionId());
		      LOG.debug("has query search id: " + query.getQuerySearchId());
			  LOG.debug("has query pattern with size of: " + query.getQueryPattern().length);
			  for (int i=0; i<query.getQueryPattern().length;i++) {
				  LOG.debug("..." + query.getQueryPattern()[i]);
			  }
			  LOG.debug ("has top k number specification:" + query.getTopK());
	      }
		  
	      this.searchManager.acceptAndDistributeSearchQuery(query);
	 }
	 
	 private void processAbandonmentCommand (byte[] message) {
		//we are using polling here. 
		if (LOG.isDebugEnabled()) {
		   LOG.debug ("partition: "+ this.searchManager.getPartitionId() + " receives abandonment command related bytes with length of: " + message.length);
		}
		
		TimeSeriesSearch.QuerySearchAbandonmentCommand command = new TimeSeriesSearch.QuerySearchAbandonmentCommand();
	    try {
	            
	   	        ByteArrayInputStream in = new ByteArrayInputStream(message);
	   	        DataInputStream dataIn = new DataInputStream(in);
	   	        
	   	        command.readFields(dataIn);
	   	        dataIn.close();
	        
	    }
	    catch (Exception ex) {
	        	LOG.error("fails to receive/reconstruct the abandonment command information", ex);
	    }
	        
	    if (LOG.isDebugEnabled()) {     
	      LOG.debug("the abandonment command received for the paritioner: " + this.searchManager.getPartitionId()
	    		                                                 + " at time stamp: " + System.currentTimeMillis());
	      LOG.debug("has query search id: " + command.getQuerySearchId());
		  LOG.debug("has partition id: " + command.getPartitionId());  
		   
		  LOG.debug("has thread processor id:" + command.getThreadProcessorId());
	    }
	    
	    this.searchManager.acceptAndDistributeSearchQueryAbandonment (command);
	 }
}
