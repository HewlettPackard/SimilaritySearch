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


import com.hp.hpl.palette.similarity.comm.ServiceHandler;
import com.hp.hpl.palette.similarity.datamodel.TimeSeriesSearch;
  
import java.util.List;
import java.util.UUID;

/**
 * to perform the search of the time series partition that has been loaded into the local memory of the Map instance, using the LSH index
 * based search against the index that has been built in the local memory. 
 * 
 * The search processor accepts concurrent searches that are issued from the Coordinator Master. The actual search is done by each search worker
 * The actual implementation of the search coordinator will have a maximum number of the concurrent query to be supported. 
 * 
 * 
 * @author Jun Li
 *
 */
public interface MultiSearchManager {
	
	/**
	 * the partition id assigned from Hadoop regarding the assigned data partition. 
	 * NOTE:  we need to have the way that the coordinator knows how many partitions the entire data set has. 
	 * 
	 * @return the partition id. 
	 */
	int getPartitionId (); 
	/**
	 * start the internal worker thread pool that will process the batched queries concurrently, plus the timer thread to flush the communicator buffer.
	 *
	 */
	void startQueryProcessors();
	
	/**
	 * stop the internal worker thread pool that will process the batched queries concurrently, plus the timer thread to flush the communicator buffer.
	 */
	void shutdownQueryProcessors();
	
	/**
	 * wait for the internal worker thread pool to exit.
	 */
	void waitForQueryProcessors() throws InterruptedException ;
	
	/**
	 * accept the search queries coming from the coordinator, and have them to be uniformly distributed into the internal query queue for a pool of the 
	 * worker threads. The queue is a concurrent non-blocking queue (concurrent ConcurrentLinkedQueue) to store the active concurrent query requests. 
	 * @param queries
	 *  
	 */
	void acceptAndDistributeSearchQueries (List<TimeSeriesSearch.SearchQuery> queries);
	
    /**
     * accept a single query from the message queue.
     * @param query
     */
	void acceptAndDistributeSearchQuery (TimeSeriesSearch.SearchQuery query);
	
	/**
	 * This is for debugging purpose, so that we can pull the intermediate results at the adaptor (with interface of:
	 * SinkAdaptorForBufferedCommunicator, to examine the result. 
	 * 
	 * In the real deployment, the intermediate result will be sent to the coordinator directly over the communication channel.
	 * 
	 * @return the current research results that are stored in the sink adaptor.
	 */
    List<TimeSeriesSearch.IntermediateQuerySearchResult> obtainIntermediateSearchResults();
	 
	
	/**
	 * to accept the queries that are current under progress in  
	 * 
	   @param commands to abandonment the query search associated with this partition. 
	 */
	void acceptAndDistributeSearchQueriesAbandonment (List<TimeSeriesSearch.QuerySearchAbandonmentCommand> commands);
	
	/**
	 * to accept the single abandonment command
	 * @param commands
	 */
	void acceptAndDistributeSearchQueryAbandonment (TimeSeriesSearch.QuerySearchAbandonmentCommand command);
	
	
	 /**
	   * TODO: to be further defined once we have better understanding on Netty works. We will need to 
	   * have multiple service listeners:
	   * 
	   * (1) Accept queries;  (and respond based on the acceptance results); 
	   * (2) Accept Abandonment (and then respond to terminate the abandonment); 
	   * 
	   */
	  
	void registerServiceHandlers(List<ServiceHandler> handlers);
	
 
    interface SearchCoordinatorWork  {
         void conductSearchOnQueries();
	} 
    
    interface BufferedCommunicatorWork {
    	  //to allow the implementation to expose whether this communicator is actually doing the buffering or not, 
    	  //or just direct pass through and push the buffering to the next level of the communication framework.
    	  boolean isBuffered(); 
    	  void bufferSearchResult(TimeSeriesSearch.IntermediateQuerySearchResult result); 
    	  void sendSearchResults();
    	  //in the case of direct pass through.
    	  void sendSearchResult();
    	  void setLastFlushTime(long time);
     	  long getLastFlushTime();
     	  
     	  long getScheduledCommunicatorBufferSendingInterval();
     	  
     	  void init(); //to allow the thread-specific work to be done.
    } 
    
   
}
