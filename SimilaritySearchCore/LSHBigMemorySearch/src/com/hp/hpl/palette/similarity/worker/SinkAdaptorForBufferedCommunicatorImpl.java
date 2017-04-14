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

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.hp.hpl.palette.similarity.datamodel.TimeSeriesSearch;
import com.hp.hpl.palette.similarity.datamodel.TimeSeriesSearch.IntermediateQuerySearchResult;

public class SinkAdaptorForBufferedCommunicatorImpl {


	private static final Log LOG = LogFactory.getLog(SinkAdaptorForBufferedCommunicatorImpl.class.getName());
	
	/**
	 * This is for the debugging and simulation purpose.
	 *
	 */
    public static class SimulatedSinkAdaptorForBufferedCommunicator  implements  SinkAdaptorForBufferedCommunicator {
    	private BlockingQueue <TimeSeriesSearch.IntermediateQuerySearchResult> bufferedSearchResults; 
    	
    	public SimulatedSinkAdaptorForBufferedCommunicator  () {
    		this.bufferedSearchResults = new LinkedBlockingQueue<TimeSeriesSearch.IntermediateQuerySearchResult>(); 
    	}
    	
        @Override
    	public void bufferSearchResults(List<TimeSeriesSearch.IntermediateQuerySearchResult> batchedResults) {
        	if (LOG.isDebugEnabled()) {
        		LOG.debug("Simulated Sink Adaptor receives batched results with size of: " + batchedResults.size()  + " run time thread: " 
        				+ Thread.currentThread().getId());
        	}
    	    this.bufferedSearchResults.addAll(batchedResults);
    		 
    	}
    	
        @Override
   	    public  List<TimeSeriesSearch.IntermediateQuerySearchResult> retrieveBufferedSearchResults() {
   	       List<TimeSeriesSearch.IntermediateQuerySearchResult>  retrievedResults = 
   	    		               new ArrayList<TimeSeriesSearch.IntermediateQuerySearchResult> ();
   	       this.bufferedSearchResults.drainTo(retrievedResults);
   		   return retrievedResults; 
   	    }

		@Override
		public void bufferSearchResult(IntermediateQuerySearchResult result) {
			throw new UnsupportedOperationException ("the method is not implemented for the simulated class");
			
		}

		@Override
		public boolean isBuffered() {
			//the whole method is designed to be buffered. 
			return true;
		}
		
		@Override
		public void init() {
			 //do nothing.
			
		}
   }
   
   //we will have the Zero-MQ implementation, and then maybe Netty based implementation. for comparison. 
}
