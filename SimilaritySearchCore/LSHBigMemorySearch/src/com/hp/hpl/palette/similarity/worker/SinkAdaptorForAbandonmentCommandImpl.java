
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
import org.zeromq.ZMQ;

import com.hp.hpl.palette.similarity.datamodel.TimeSeriesSearch.QuerySearchAbandonmentCommand;

public class SinkAdaptorForAbandonmentCommandImpl {

	private static final Log LOG = LogFactory.getLog(SinkAdaptorForAbandonmentCommandImpl.class.getName());
	
	public static class ZeroMQBasedSinkAdaptorForAbandonmentCommandImpl implements SinkAdaptorForAbandonmentCommand{

		private ZMQ.Context context;
    	private static final ThreadLocal<ZMQ.Socket> sender = new ThreadLocal<ZMQ.Socket>();
    	
    	
		public ZeroMQBasedSinkAdaptorForAbandonmentCommandImpl (ZMQ.Context context){
			this.context = context;
		}
		
		@Override
		public List<QuerySearchAbandonmentCommand> retrieveBufferedCommands() {
			throw new UnsupportedOperationException ("the method is not implemented for the simulated class");
		}

		@Override
		public void bufferAbandonmentCommands(
				List<QuerySearchAbandonmentCommand> batchedCommands) {
			throw new UnsupportedOperationException ("the method is not implemented for the simulated class");
			
		}

		@Override
		public void bufferAbandonmentCommand( QuerySearchAbandonmentCommand command) {
			ZMQ.Socket abandonmentCommandDistributor= sender.get();
		    //send this over the in-proc socket.
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
        
        	boolean status=  abandonmentCommandDistributor.send(message, 0, message.length, 0); 
			if (!status) {
				LOG.error("fails to distribute abandonment command from the worker thread to the abandonment command assembler in the aggregator" );
			}
		}

		@Override
		public boolean isBuffered() {
			return false;
		}

		/**
		 * we need the inproc socket to push the abandonment commands back to the lister thread that will accept the pull 
		 */
		@Override
		public void init() {
			LOG.info("SinkAdaptorForAbandonmentCommand is conducting init at thread: " + Thread.currentThread().getId());
			//do some thread related stuff here.
			ZMQ.Socket sink = context.socket(ZMQ.PUSH);
			//later, we can optimize using inter-process communication is necessary. 
			String destinationAddress = "inproc://"  + "abandonmentcmd.ipc";
	        sink.connect(destinationAddress);
	        LOG.info("SinkAdaptorForAbandonmentCommand's init connect to the destination (PUSH) with: " + destinationAddress);
	        sender.set(sink);
			
		}
		
	}
}
