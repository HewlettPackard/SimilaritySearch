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
import java.util.UUID;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.zeromq.ZMQ;

import com.hp.hpl.palette.similarity.datamodel.ProcessingUnitRuntimeInformation;
import com.hp.hpl.palette.similarity.datamodel.TimeSeriesSearch;
import com.hp.hpl.palette.similarity.worker.PartitionAggregatorRuntimeInformationQueuingWithPollingTest.SimulatedAggregator;
import com.hp.hpl.palette.similarity.worker.PartitionAggregatorRuntimeInformationQueuingWithPollingTest.SimulatedPartitioner;

public class PartitionAggregatorQueryAndAbandonmentTest {

	 private static final Log LOG = LogFactory.getLog(PartitionAggregatorQueryAndAbandonmentTest.class.getName());
	 
	 public static class SimulatedPartitioner extends Thread {
		 private  ZMQ.Context context; 
		 
	  
		 public SimulatedPartitioner (ZMQ.Context context) {
			 this.context = context;
		 }
		 
		 public void run() {
	   	    	
			    ZMQ.Socket queryReceiver = context.socket(ZMQ.SUB);
			    queryReceiver.connect("tcp://localhost:5558");
			    ///subscription topic to be with zero length, that is, to subscribe to any message.
	   	        queryReceiver.subscribe(new byte[0]); 
	   	        
	   	        
	   	        ZMQ.Socket abandonmentCommand = context.socket(ZMQ.SUB);
	   	        abandonmentCommand.connect("tcp://localhost:5559");
			    ///subscription topic to be with zero length, that is, to subscribe to any message.
	   	        abandonmentCommand.subscribe(new byte[0]);
	   	        
	   	        //before getting to the polling of the message, let's use REQ and REP to wait for the synchronization point. 
	   	        ZMQ.Socket synchronizationBarrier1 = context.socket(ZMQ.REQ);
	   	        synchronizationBarrier1.connect("tcp://localhost:5560");
	   	        //send the empty message out, and then wait for the response. the response will be received when the number of 
	   	        //partitions have been reached. 
	   	         {
	               String request = "Hello";
	               LOG.info("Sending Hello for hand-shaking" + request);
	               synchronizationBarrier1.send(request.getBytes(), 0);

	               byte[] reply = synchronizationBarrier1.recv(0);
	               LOG.info("Received for hand-shaking " + new String(reply));
	            }
	   	        
	   	        //it block when the aggregator is ready 
	   	        ZMQ.Socket synchronizationBarrier2 = context.socket(ZMQ.SUB);
		        synchronizationBarrier2.connect("tcp://localhost:5561");
		        synchronizationBarrier2.subscribe(new byte[0]);
		        
		        LOG.info("the partitioner in the following thread:...." 
                           + Thread.currentThread().getId() + " is waiting for hand-shaking barrier to proceed...");
		        {
		             byte[] reply = synchronizationBarrier2.recv(0);
		             LOG.info("Partitioner received for hand-shaking " + new String(reply) + " in the following thread:...." 
	                           + Thread.currentThread().getId() );
		         }
	   	       
	   	        
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
		        
		        //then close the synchronization barrier.
	   	        synchronizationBarrier1.close();
	   	        synchronizationBarrier2.close();
		        queryReceiver.close ();
		        abandonmentCommand.close();
	   	        
	   	    }
		 
		 private void processQuery (byte[] message) {
			 //we are using polling here. 
			 LOG.info("partitioner receives query related bytes with length of: " + message.length);
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
			  
		 }
		 
		 private void processAbandonmentCommand (byte[] message) {
			//we are using polling here. 
			LOG.info("partition receives abandonment command related bytes with length of: " + message.length);
			TimeSeriesSearch.QuerySearchAbandonmentCommand command = new TimeSeriesSearch.QuerySearchAbandonmentCommand();
	   	     try {
	   	            
		   	        ByteArrayInputStream in = new ByteArrayInputStream(message);
		   	        DataInputStream dataIn = new DataInputStream(in);
		   	        
		   	        command.readFields(dataIn);
		   	        dataIn.close();
	   	        
	   	      }
	   	      catch (Exception ex) {
	   	        	LOG.error("fails to receive/reconstruct the run time information", ex);
	   	      }
	   	        
	   	        
	   	      LOG.info("the abandonment command received from the paritioner is the following for thread:...." 
	   	    	                                                               + Thread.currentThread().getId());
	   	      LOG.info("query search id: " + command.getQuerySearchId());
			  LOG.info("partition id: " + command.getPartitionId());  
			   
			  LOG.info("thread processor id:" + command.getThreadProcessorId());
		 }
	 }
	 
	 //to distribute the query and also the early abandonment message.
	 public static class SimulatedAggregator extends Thread {
		 private  ZMQ.Context context; 
		 private int clientCounter; 
		 
		 public SimulatedAggregator (ZMQ.Context context, int clientCounter ) {
			this.context = context;
			this.clientCounter = clientCounter;
		 }
		 
		 public void run() {
	   	    	
			    //  Socket to send messages on
		        ZMQ.Socket queryDistributor = context.socket(ZMQ.PUB);
		        queryDistributor.bind("tcp://*:5558");

		        ZMQ.Socket abandonmentCommandDistributor = context.socket(ZMQ.PUB);
		        abandonmentCommandDistributor.bind("tcp://*:5559");
		        
		        //first wait for the synchronization point first.
		        ZMQ.Socket synchronizationBarrier1 = context.socket(ZMQ.REP);
		        synchronizationBarrier1.bind("tcp://*:5560");
		        
		        ZMQ.Socket synchronizationBarrier2 = context.socket(ZMQ.PUB);
		        synchronizationBarrier2.bind("tcp://*:5561");
		        
		        
		        int count = 0; 
		        while (!Thread.currentThread().isInterrupted()) {
		            // Wait for next request from the client
		            byte[] request = synchronizationBarrier1.recv(0);
		            LOG.info("Aggregator Received Hello");

		            // Send reply back to client
		            String reply = "World";
		            synchronizationBarrier1.send(reply.getBytes(), 0);
		            
		            count++;
		            if (count == this.clientCounter) {
		            	break;
		            }
		        }
		        
		        //sleep so that the published commands will not be missed by the partitioner
		        try {
			    	   //take some sleep, before doing the actual send action 
			    	   Thread.sleep (1000); 
			    }
			    catch (Throwable t) {
			    		//do some logging 
			    }
		        
		        LOG.info("Aggregator ask client to move on....");
		        if (count == this.clientCounter) {
		        	 String reply = "movedon";
		        	 {
			           synchronizationBarrier2.send(reply.getBytes(), 0);
		        	 }
		        }
		        
	 
		        //sleep so that the published commands will not be missed by the partitioner
		        try {
			    	   //take some sleep, before doing the actual send action 
			    	   Thread.sleep (1000); 
			    }
			    catch (Throwable t) {
			    		//do some logging 
			    }
		        
		        LOG.info("Aggregator now proceed to send queries...");
		        
		        //now we need to distribute queries and commands
		        int queryCount = 3;
		        for (int i=0; i<queryCount; i++) {
		        	String id= UUID.randomUUID().toString();
		        	float[] queryPattern = new float[5];
		        	for (int j=0; j<5;j++) {
		        		queryPattern[j] = (float)j;
		        	}
		        	
		        	int topK = 5;
		        	TimeSeriesSearch.SearchQuery query = new TimeSeriesSearch.SearchQuery (id, queryPattern, topK);
		        	
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
		        	
		        	LOG.info("aggegrator distributes query in bytes with length of: " + message.length);
		        	queryDistributor.send(message, 0, message.length, 0); 
		        }
		  
		        //then the early abandonment messages
		        int abandonmentCount = 3;
		        for (int i=0; i<abandonmentCount;i++) {
		        	String id= UUID.randomUUID().toString(); 
		        	int partitionId = i;
		        	int threadProcessorId =  Math.abs(id.hashCode())%6;
		        	TimeSeriesSearch.QuerySearchAbandonmentCommand command = 
		        			new TimeSeriesSearch.QuerySearchAbandonmentCommand (id, partitionId, threadProcessorId);
		        	byte[] message = null;
		        	{
		        		ByteArrayOutputStream out = new ByteArrayOutputStream();
			   	    	DataOutputStream dataOut = new DataOutputStream (out);
			   	    	try {
			   	          command.write(dataOut);
			   	    	  dataOut.close();
			   	    	}
			   	    	catch(IOException ex) {
			   	    		LOG.error ("fails to serialize the aggegrator's abandonment command information", ex);
			   	    	}
			   	    
			   	    	
			   	    	message = out.toByteArray();
		        	}
		        
		        	LOG.info("aggegrator distributes abandonment command in bytes with length of: " + message.length);
		        	abandonmentCommandDistributor.send(message, 0, message.length, 0); 
		        }
		        
		        //so that the client can receive and process all of the messages.
		        try {
			    	   //take some sleep, before doing the actual send action 
			    	   Thread.sleep (1000000); 
			    }
			    catch (Throwable t) {
			    		//do some logging 
			    }
		        
		        synchronizationBarrier1.close();
		        synchronizationBarrier2.close();
		        queryDistributor.close();
		        abandonmentCommandDistributor.close();
	   	   }
		 
	
	 }
	 
	 public static void main (String[] args) throws Exception  {
			ZMQ.Context context = ZMQ.context(1); //1 is for one single IO thread per socket. 
			//1 for the number of the client count to be waited.
			int clientCount = 5; 
			SimulatedAggregator aggregator = new SimulatedAggregator(context, clientCount);
			aggregator.start();
			 
		    //need to wait for the server to be ready first. 
			 //so that the client can receive and process all of the messages.
	        try {
		    	   //take some sleep, before doing the actual send action 
		    	   Thread.sleep (1000); 
		    }
		    catch (Throwable t) {
		    		//do some logging 
		    }
	        
			
			int threadCount = clientCount;
			for (int i=0; i<threadCount;i++) {
			 
			  SimulatedPartitioner  partitioner = new SimulatedPartitioner (context);
			  partitioner.start();
		   }
		   
			try {
		    	   //take some sleep, before doing the actual send action 
		    	   Thread.sleep (10000); 
		    }
		    catch (Throwable t) {
		    		//do some logging 
		    }
			
		    aggregator.join();
			
			LOG.info("done with the partitioner/aggregator message passing");
			
			context.term();
			
			
			
		}
}
