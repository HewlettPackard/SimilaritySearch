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

 

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.List;
  
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.hp.hpl.palette.similarity.datamodel.TimeSeriesSearch;
import com.hp.hpl.palette.similarity.datamodel.TimeSeriesSearch.IntermediateQuerySearchResult;

import org.zeromq.ZMQ;

public class SinkAdaptorForNoBufferedCommunicatorImpl {


	private static final Log LOG = LogFactory.getLog(SinkAdaptorForNoBufferedCommunicatorImpl.class.getName());
	
	/**
	 * This is for the debugging and simulation purpose.
	 *
	 */
    public static class ZeroMQBasedAdaptorForNoBufferedCommunicatorImpl  implements  SinkAdaptorForBufferedCommunicator {
    	private ZMQ.Context context;
    	private String  aggegratorIPAddress; 
    	private int portNumber;
    	
    	private static final ThreadLocal<ZMQ.Socket> sender = new ThreadLocal<ZMQ.Socket>();
    	
    	public ZeroMQBasedAdaptorForNoBufferedCommunicatorImpl  (ZMQ.Context context, String aggegratorIPAddress, int portNumber ) {
    		this.context = context;
    		this.aggegratorIPAddress =  aggegratorIPAddress;
    		this.portNumber = portNumber;
    	}
    	
        @Override
    	public void bufferSearchResults(List<TimeSeriesSearch.IntermediateQuerySearchResult> batchedResults) {
        	throw new UnsupportedOperationException ("the method is not implemented for the simulated class");
    		 
    	}
    	
        @Override
   	    public  List<TimeSeriesSearch.IntermediateQuerySearchResult> retrieveBufferedSearchResults() {
        	throw new UnsupportedOperationException ("the method is not implemented for the simulated class"); 
   	    }

        /**
         * We now rely on the thread local storage to store the thread-specific information, that is, the socket that is initialized 
         * after the thread is launched.
         */
		@Override
		public void bufferSearchResult(IntermediateQuerySearchResult result) {
			ZMQ.Socket sink= sender.get();
			
			ByteArrayOutputStream out = new ByteArrayOutputStream();
   	    	DataOutputStream dataOut = new DataOutputStream (out);
   	    	try {
   	    		result.write(dataOut);
   	    	    dataOut.close();
   	    	}
   	    	catch(IOException ex) {
   	    		LOG.error ("fails to serialize the partition's intermediate result information", ex);
   	    	}
   	    
   	    	
   	    	byte[] message = out.toByteArray();
   	        //  Socket to send messages on
   	        
   	    	if (LOG.isDebugEnabled()) {
   	    		LOG.debug("at Sink Adaptor, sending out intermediate search result for partition: " + result.getPartitionId()
   	    				   + " and R value:" + result.getAssociatedRvalue() + " at time stamp: " + System.currentTimeMillis());
   	    	
   	    	}
   	        //now to do the send.the last flag is "no more" to send. 
   	        boolean status = sink.send(message, 0, message.length, 0); 
   	        if (!status) {
   	        	LOG.error("fails to send out the intermediate search result to the aggegrator");
   	        }
   	        
		}

		@Override
		public boolean isBuffered() {
			//the whole method is designed to be buffered. 
			return false;
		}
		
		@Override
		public void init() {
			LOG.info("SinkAdaptorForNoBufferedCommunicator is conducting init at thread: " + Thread.currentThread().getId());
			 //do some thread related stuff here. the socket will have to be created in this particular thread.
			ZMQ.Socket sink = context.socket(ZMQ.PUSH);
			//later, we can optimize using inter-process communication is necessary. 
			String destination = "tcp://" + this.aggegratorIPAddress + ":" + (new Integer(this.portNumber)).toString();
	        sink.connect(destination);
	        LOG.info("SinkAdaptorForNoBufferedCommunicator's init connect to the destination (PUSH) with: " + destination);
	        sender.set(sink);
			
		}
   }
   
   //we will have the Zero-MQ implementation, and then maybe Netty based implementation. for comparison. 
}

