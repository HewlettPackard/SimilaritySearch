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

import java.util.List;
import java.util.ArrayList;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletionService;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicBoolean;

import java.util.Timer;
import java.util.TimerTask;
import java.lang.reflect.Array;
import java.util.Arrays;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.hp.hpl.palette.similarity.comm.ServiceHandler;
import com.hp.hpl.palette.similarity.configuration.SupportedSearchCategory;
import com.hp.hpl.palette.similarity.datamodel.TimeSeriesSearch;
import com.hp.hpl.palette.similarity.datamodel.TimeSeriesSearch.IntermediateQuerySearchResult;
import com.hp.hpl.palette.similarity.datamodel.TimeSeriesSearch.QuerySearchAbandonmentCommand;
import com.hp.hpl.palette.similarity.datamodel.TimeSeriesSearch.SearchQuery;
import com.hp.hpl.palette.similarity.worker.concurrent.AsynIndexSearchCoorindatorIntf;
import com.hp.hpl.palette.similarity.bridge.SearchCoordinatorAtRProxy;

/**
 * At this time, we do not put the limitation of the search query size for each worker thread. 
 * 
 * @author Jun Li
 *
 */
public class MultiSearchManagerImpl implements MultiSearchManager {

	private static final Log LOG = LogFactory.getLog(MultiSearchManagerImpl.class.getName());

   
    /**
     * A timer task to flush the buffered intermediate query results to be sent to the coordinator. 
     *
     */
    public static class BufferedSearchResultsDisseminator extends TimerTask {

    	private BufferedCommunicatorWork communicator; 
    	private int counter=0; //to indicate that this is the first time flush or not. 
    	 
    	public BufferedSearchResultsDisseminator (BufferedCommunicatorWork communicator){
    	     this.communicator = communicator; 	
    	}
    	
        @Override
        public void run() {
            //it will be run at the fixed schedule. 
        	//it is better to have signalling between this thread and the worker threads.
         
        	long lastTimeStamp = this.communicator.getLastFlushTime();
        	long now = System.currentTimeMillis();
        	
        	if ((now - lastTimeStamp) > 
        	             0.75*(this.communicator.getScheduledCommunicatorBufferSendingInterval())){
        	  if (counter == 0) {
	        	  if (LOG.isDebugEnabled()) {
			    		  LOG.debug("driven by TIMER THREAD, buffered communicator send search results to the adaptor, runtime thread id is:"
			    				  + Thread.currentThread().getId());
			      }
        	  }
        	  
        	  counter++;
        	  
        	  this.communicator.sendSearchResults();
        	  this.communicator.setLastFlushTime(System.currentTimeMillis());
        	}
        	else{
        		//simply return; 
        	}
        }
    }

    public static class NoBufferedCommunicator implements BufferedCommunicatorWork {

    	private SinkAdaptorForBufferedCommunicator adaptor; 
    	private TimeSeriesSearch.IntermediateQuerySearchResult searchResult; 
    	public NoBufferedCommunicator (LSHIndexConfigurationParameters configurationParameters, SinkAdaptorForBufferedCommunicator adaptor){
    		this.adaptor = adaptor;
    		if (this.adaptor.isBuffered()) {
    			throw new RuntimeException ("the provided sink adaptor should not be buffered");
    		}
    	}
    	
		@Override
		public boolean isBuffered() {
			//there is no buffering 
			return false;
		}

		@Override
		public void bufferSearchResult(IntermediateQuerySearchResult result) {
			 this.searchResult = result;
			 //immediately pass through to the adaptor.
			 this.sendSearchResult();
			
		}

		@Override
		public void sendSearchResults() {
			throw new UnsupportedOperationException ("the method is not implemented for this class");
			
		}

		@Override
		public void sendSearchResult() {
			this.adaptor.bufferSearchResult(this.searchResult);
			
		}

		@Override
		public void setLastFlushTime(long time) {
			throw new UnsupportedOperationException ("the method is not implemented for this class");
			
		}

		@Override
		public long getLastFlushTime() {
			throw new UnsupportedOperationException ("the method is not implemented for this class");
		}

		@Override
		public long getScheduledCommunicatorBufferSendingInterval() {
			throw new UnsupportedOperationException ("the method is not implemented for this class");
		}

		@Override
		public void init() {
			this.adaptor.init();
			
		}
    	
    }
    
    /**
     * The work done by the Timer Thread for every 100 millseconds to send out the buffered intermediate results to the coordinator
     * 
     */
    public static class BufferedCommunicator implements BufferedCommunicatorWork {

    	private BlockingQueue<TimeSeriesSearch.IntermediateQuerySearchResult> bufferredSearchResults; 
    	private int bufferLimit; 
    	private AtomicLong lastSendTimeStamp;
    	private LSHIndexConfigurationParameters configurationParameters;
    	private SinkAdaptorForBufferedCommunicator adaptor;
    	
    	/**
    	 * 
    	 * @param bufferSize specifies the capacity of the buffer queue. Say 50 intermediate results to  be packed. 
    	 */
    	public BufferedCommunicator (LSHIndexConfigurationParameters configurationParameters, SinkAdaptorForBufferedCommunicator adaptor) {
    		if (!adaptor.isBuffered()) {
    			//we will have to make sure that the adaptor is buffered 
    			throw new RuntimeException("bothe buffered communicator and its sink adaptor have to support buffering mechanism");
    		}
    		this.bufferLimit = configurationParameters.getCommunicatorBufferLimit();
    		//atomic long is not a regular long; it needs to be initialized
    		this.lastSendTimeStamp = new AtomicLong(0);
    		this.bufferredSearchResults = 
    				   new ArrayBlockingQueue<TimeSeriesSearch.IntermediateQuerySearchResult>(this.bufferLimit);
    		this.configurationParameters = configurationParameters;
    		this.adaptor = adaptor;
    	}
    	
    	/**
    	 * this method will be performed by a worker thread doing query search 
    	 */
		@Override
		public void bufferSearchResult(TimeSeriesSearch.IntermediateQuerySearchResult result) {
		      if (this.bufferredSearchResults.size() == this.bufferLimit) {
		    	  if (LOG.isDebugEnabled()) {
		    		  LOG.debug("identified that buffered communicator has reach the limit of: " + this.bufferLimit + " and ready to send search results");
		    		  LOG.debug("driven by WORKER THREAD, buffered communicator send search results to the adaptor, with runtime thread id:" +
		    				  Thread.currentThread().getId());
			      
		    	  }
		    	  
		    	  this.sendSearchResults();
		      }
		      
		      //then we should flush the buffer now, instead of waiting for the timer thread to kick in. 
		      try {
		    	//if the traffic is so high that when this particular thread reaches this point, the buffer is full again (very unlikely),
		    	//then this thread will be blocked, until the queue is free-up. 
		    	//the method of put allows: Inserts the specified element at the tail of this queue, waiting for space to become available if the queue is full.
		        this.bufferredSearchResults.put(result);
		      }
		      catch(InterruptedException ex) {
		    	  //NOTE: how can I access the MultiSearchManagerImpl.partitionId? Please consult the Hadoop Reducer code.
		    	  LOG.error("fails to buffer search result to the buffered communicator",  ex);
		    	  //do nothing;
		      }

		}

		/**
		 * This method will be performed also by the timer thread. 
		 */
		@Override
		public void sendSearchResults() {
			int size= this.bufferredSearchResults.size();
			if (size > 0) {
				List<TimeSeriesSearch.IntermediateQuerySearchResult> batchedResults  = 
						           new ArrayList<TimeSeriesSearch.IntermediateQuerySearchResult> (size);
				//at least, we will specify the queue size that we are ware of early, 
				//Removes at most the given number of available elements from this queue and adds them to the given collection.
				this.bufferredSearchResults.drainTo(batchedResults, size);
				
				//the actual size may be smaller, as it could be that the timer thread is also working on it. 
				if (LOG.isDebugEnabled()) {
		    		  LOG.debug("buffered communicator send search results to the adaptor with size of: " + batchedResults.size()
		    				  + " runtime thread id: " + Thread.currentThread().getId());
		    	}
				
				sendSearchResults(batchedResults);
				this.lastSendTimeStamp.set(System.currentTimeMillis());
			}
		}
		
		
    	protected void sendSearchResults(List<TimeSeriesSearch.IntermediateQuerySearchResult> batchedResults) {
    		//TODO: do the send related communication. For the simulation, we can have different SINK to it.
    		if (this.adaptor != null) {
    			this.adaptor.bufferSearchResults(batchedResults);
    		}
    		//else: just let it disappear. 
    	}
    	
    	@Override
        public void setLastFlushTime(long val) {
    		 this.lastSendTimeStamp.set(val);
    	}
    	
    	@Override
    	public long getLastFlushTime() {
    		return this.lastSendTimeStamp.get();
    	}

		@Override
		public boolean isBuffered() {
			//we purposely implemented this method with buffering. 
			return true;
		}

		@Override
		public void sendSearchResult() {
			 //since it is buffering, this method is not supported.
			throw new UnsupportedOperationException ("the method is not implemented for the buffered communication");
			
		}

		@Override
		public long getScheduledCommunicatorBufferSendingInterval() {
			return this.configurationParameters.getScheduledCommunicatorBufferSendingInterval();
		}
		
		@Override
		public void init() {
			this.adaptor.init();
			
		}
    }
    
    
    public static class SearchRelatedProcessor implements SearchCoordinatorWork  {

    	private boolean simulated;
    	
    	private LSHIndexConfigurationParameters configurationParameters; 
    	private BufferedCommunicatorWork bufferedCommunicator; 
        private BlockingQueue <SearchQuery> searchQueue;
        private ConcurrentMap <String, Boolean> abandonmentCommandsMap;
        private int threadId; 
        private int partitionId; //this is for global communication with multiple partitions to the coordinator
        private  AtomicBoolean abandonment; 
        //it is sorted 
        private  List<SearchCoordinator> workList;
        //this is to define that for a single thread that is doing the search over multiple R's, how many of such queries can be held for a thread. It depends on 
        //how much memory that needs to be allocated for each thread to carry out the search over these many of the search queries. 
        private  int limitedSizeForWorkList;
        private SupportedSearchCategory.SupportedSearch chosenSearch;
        
        public SearchRelatedProcessor  () {
        	this.chosenSearch = SupportedSearchCategory.SupportedSearch.LSH_SEARCH;
        	this.simulated = false; 
        	//required by the asynchronous interface defined in AsynINdexSearchCoordinatorIntf.java
        	this.abandonment  = new AtomicBoolean (false);
         
        	this.configurationParameters = null;
        	this.bufferedCommunicator = null;
        	this.searchQueue = null;
        	this.abandonmentCommandsMap = null;
        	this.workList = null;
        }
        
        
    	public SearchRelatedProcessor (
    			  SupportedSearchCategory.SupportedSearch chosenSearch,
    			  boolean simulated, LSHIndexConfigurationParameters configurationParameters, 
    			  BlockingQueue <SearchQuery> queue, ConcurrentMap <String, Boolean> map, BufferedCommunicatorWork bufferedCommunicator,
    			  int limitedSizeForWorkList, int threadId, int partitionId) {
    		this.chosenSearch = chosenSearch;
    		this.simulated = simulated;
    		this.abandonment  = new AtomicBoolean (false);
    		
    		this.configurationParameters = configurationParameters;
    		this.searchQueue = queue;
    		this.abandonmentCommandsMap = map;
    		this.bufferedCommunicator = bufferedCommunicator;
    		
    		this.threadId = threadId;
    		this.partitionId = partitionId; 
    		
    		this.limitedSizeForWorkList = limitedSizeForWorkList;
        	this.workList = new ArrayList<SearchCoordinator> (this.limitedSizeForWorkList);
    		
    	}
    	
    	/**
         * to allow the thread specific initialization to be conducted here. 
         */
        public void init() {
        	this.bufferedCommunicator.init();
        }
        
    	
    	/**
    	 * to allow the manager to issue the command to the processor, so that we can have the execution loop to exit.  
    	 */
    	public  void abandonment () {
    		this.abandonment.set(true);
    	}
    	
    	/**
    	 * this will allow the search manager to assign a query into this particular worker thread's queue. 
    	 * @param query
    	 */
    	public void assignQuery (SearchQuery query) {
    		//the query goes to the tail of the queue. 
    		try {
    		   this.searchQueue.put(query);
    		}
    		catch (Exception ex) {
    			LOG.error("worker thread: " + this.threadId + " for concurrent search experiences interrupted exception: " + ex.getMessage());
    			//we simply ignore this  and continue. 
    		}
    	}
    	
    	/**
    	 * this will allow the search manager to relay the abandonment command received from the coordinator to this particular thread-based processor.
    	 * The corresponding removal of the command will be done internally by the thread-based processor. 
    	 * 
    	 * @param searchID
    	 */
    	public void updateAbandonmentCommands (String searchId) {
    		this.abandonmentCommandsMap.put(searchId, new Boolean(true));
    	}
    	
    	/**
    	 * The LinkedBlockingQueue provides the method of "take", which is to retrieve and remove the head of this queue, 
    	 * waiting if necessary until an element becomes available. As a result, we do not need wait/notify signalling for coordination.
    	 */
		@Override
		public void conductSearchOnQueries() {
			SearchQuery headOfQueryForProbing = null;
			LOG.info("work thread with logical id: " + this.threadId 
					 + " is now in the processing loop, with runtime thread id: " + Thread.currentThread().getId());
			while (true) {
				try {
					if (!this.abandonment.get()) {
						headOfQueryForProbing =  doWork (headOfQueryForProbing);
					}
					else {
						LOG.info("worker thread: " + this.threadId + "received user command to abort its processing loop (not query-based early abandonment)");
						break; //and thus exit the function.
					}
				}
				catch (Exception ex) {
					LOG.error("fails to conduct the search loop. runtime thread id: " + Thread.currentThread().getId(), ex);
				}
		    }
			
		}

		
    	/**
    	 * to conduct actual processing. 
    	 * @param query
    	 */
		protected SearchQuery doWork(SearchQuery headOfQueryForProbing) {
			//(1) scan the work list and find whether some of them has been abandoned or not. 
			List<SearchCoordinator> newWorkList = new ArrayList<SearchCoordinator>();
			List<SearchCoordinator> toBeRemoved = new ArrayList<SearchCoordinator>();
			
			for (SearchCoordinator sc: this.workList) {
				String searchId =sc.getQueryIdentifier();
				Boolean result = this.abandonmentCommandsMap.get(searchId);
				if (result == null) {
					//no early abandonment received
					newWorkList.add(sc);
				}
				else {
					if (LOG.isDebugEnabled()) {
						LOG.debug("obtain abandonment command for search id: " + searchId + " at thread: " + this.threadId + " for partition: " + 
					                   this.partitionId + " at Phase1"  + " at time stamp: " + System.currentTimeMillis());
					}
					toBeRemoved.add(sc);
				}
			}
			
			if (toBeRemoved.size() != 0) {
				this.workList.clear();
				this.workList = newWorkList;
				//shrink the abandonment list.
				for (SearchCoordinator sc: toBeRemoved) {
					this.abandonmentCommandsMap.remove(sc.getQueryIdentifier());
				}
			}
			
			//check that the work list is still have room to take the new query in, aggressively. 
			int allowedSize = this.limitedSizeForWorkList - this.workList.size();
			
			boolean previousHeadProcessed = false;  //the passed-in head probing query.
			if (headOfQueryForProbing == null) {
				previousHeadProcessed = true; //nothing passed in, so it is the first time, mark it processed. 
			}
			
			if (allowedSize > 0) {
				if (headOfQueryForProbing != null) {
					  String searchId =  headOfQueryForProbing.getQuerySearchId();
					  //when doing the initialization, the R value is 0;
				      SearchCoordinator coordinator = SearchCoordinatorFactory.createInstance  (chosenSearch, this.simulated,
				    		                              searchId,  headOfQueryForProbing, this.configurationParameters, 
				    		                              this.partitionId, this.threadId);
				      
				      this.workList.add(coordinator);
				      allowedSize--; //one gets occupied.
				      previousHeadProcessed  = true;
				}
				
				//NOTE: this queue size reflects the size at this moment, it can be larger, as more queries will come in after this point. 
				int queueSize = this.searchQueue.size(); 
				
				if (LOG.isDebugEnabled()) {
					LOG.debug("logical thread id: "  + 
				                this.threadId + " current search queue has size: " + queueSize + "at run time thread: " + Thread.currentThread().getId());
				}
				 
				if (queueSize > 0) {
					int size =(allowedSize >= queueSize)? queueSize : allowedSize;
					List<SearchQuery> acceptedNewQueries   = new ArrayList<SearchQuery> (size);
					//at least, we will specify the queue size that we are ware of early, 
					//Removes at most the given number of available elements from this queue and adds them to the given collection.
					this.searchQueue.drainTo(acceptedNewQueries, size);
					
					if (LOG.isDebugEnabled()) {
						LOG.debug("logical thread id: "  + 
					                this.threadId + " pull out search queries from the search queue with total queries pulled: " + acceptedNewQueries.size());
					}
					
					//add this to the search 
					for (SearchQuery query: acceptedNewQueries) {
						  String searchId = query.getQuerySearchId();
						  //when doing the initialization, the R value is 0;
					      SearchCoordinator coordinator = SearchCoordinatorFactory.createInstance  (chosenSearch, this.simulated, 
	                                                           searchId,  query, this.configurationParameters, this.partitionId, this.threadId);
					      
					      this.workList.add(coordinator);
					}
					 
				}
			}
			
			//now sort the search coordinator list, based on the current R value, as the bigger the R, the more expensive it will have to compute.
			if (this.chosenSearch == SupportedSearchCategory.SupportedSearch.LSH_SEARCH) {
				SearchCoordinator sortedWorkList[] = null;
				if (this.workList.size() > 0) {
				 
					if (!this.simulated) {
						SearchCoordinator[] initialArray =
							(SearchCoordinator[]) Array.newInstance(SearchCoordinatorImpl.class, this.workList.size());
						sortedWorkList  = this.workList.toArray(initialArray);
					    Arrays.sort(sortedWorkList, new SearchCoordinatorImpl());
					}
					else {
						SearchCoordinator[] initialArray =
								(SearchCoordinator[]) Array.newInstance(SimulatedSearchCoordinatorImpl.class, this.workList.size());
						sortedWorkList  = this.workList.toArray(initialArray);
					    Arrays.sort(sortedWorkList, new SimulatedSearchCoordinatorImpl());
					}
				    
				    if (LOG.isDebugEnabled()) {
				    	int counter =0; 
				    	LOG.debug("=================print out of the sorted work list=================================");
				    	for (SearchCoordinator sc: sortedWorkList) {
						 LOG.debug("logical thread id: "  + 
					                this.threadId + " has item " + counter++ + "-th "  + " with search id:" +  sc.getQueryIdentifier() +
					                " and search pattern's top-K: " + sc.getQueryPattern().getTopK() + " in the sorted work list");
				    	}
				    	
				    	LOG.debug("==================================================================================");
					}
				}
				
				//when right before the search, check that the abandonment commands come, and if so, abandon it. 
				if ( (sortedWorkList!= null) && (sortedWorkList.length > 0) ){
					this.workList.clear();
					//so that the next time, the sorting will be much faster. 
					for (int i=0; i<sortedWorkList.length;i++) {
						this.workList.add(sortedWorkList[i]); 
					}
				}
			}
			
			//NOTE: please check that the order is R value = 0 will be the first one.
			if (LOG.isDebugEnabled()) {
				LOG.debug("logical thread id: "  + 
			                this.threadId + " the current work list has search queries with size :" + this.workList.size() + " run time thread id: " 
			                + Thread.currentThread().getId());
		    }
			List<SearchCoordinator> abandonmentFromWorkList = new ArrayList<SearchCoordinator> ();
			for (SearchCoordinator sc: this.workList) {
				String searchId = sc.getQueryIdentifier();
			    Boolean abandonmentCommandsReceived =  this.abandonmentCommandsMap.get(searchId);
			    if (abandonmentCommandsReceived == null) {
			      //do sequential work really conduct the single-threaded search.
			      if (LOG.isDebugEnabled()) {
						LOG.debug("logical thread id: "  + 
					                this.threadId + " ready to do sequential work for search:" + searchId + " at time stamp: " + System.currentTimeMillis());
				  }
			    	
			      TimeSeriesSearch.IntermediateQuerySearchResult  result = doSequentialWork(sc);
			      if (LOG.isDebugEnabled()) {
						LOG.debug("logical thread id: "  + 
					                this.threadId + " now back from the sequential work. runtime thread id: " + 
								    Thread.currentThread().getId());
				  }
				  if (result == null) {
					  LOG.error("fails to condcut query for search id: "  + searchId + " at R: " + sc.getCurrentSearchRLevel());
				  }
				  else{
					  this.bufferedCommunicator.bufferSearchResult(result);
					  if (LOG.isDebugEnabled()) {
							LOG.debug("logical thread id: "  + 
						                this.threadId + " now to push result to buffered communicator. runtime thread id: " + 
									    Thread.currentThread().getId());
					  }
					  if (sc.localSearchCompleted()) {
						  if (LOG.isDebugEnabled()) {
								LOG.debug("logical thread id: "  + 
							                this.threadId + " now complete its local search for search id: " + searchId + " because of local evaluation (Phase3).");
						  }
						  
						  abandonmentFromWorkList.add(sc);
					  }
					  else {
						  //cursor.
						  if (LOG.isDebugEnabled()) {
								LOG.debug("logical thread id: "  + 
							                this.threadId + " now move to next search on the work list. runtime thread id: " + 
										    Thread.currentThread().getId());
						  }
					  }
				  }
			    }
			    else {
			    	//we are done. abandon it.
			    	if (LOG.isDebugEnabled()) {
						LOG.debug("obtain abandonment command for search id: " + searchId + " at thread: " + this.threadId + " for partition: " + 
					                   this.partitionId + " at Phase2 (early abandonment command from coordinator)" + " at time stamp: " + System.currentTimeMillis());
					}
			    	abandonmentFromWorkList.add(sc);
			    }
			}
			
			//do clean up the work list and the abandonment command list 
			if (abandonmentFromWorkList.size() > 0){
				for (SearchCoordinator sc: abandonmentFromWorkList) {
					//we abandon the search. 
					sc.abandonSearchAtCurrentRLevel();
					if (LOG.isDebugEnabled()) {
							LOG.debug("logical thread id: "  + 
						                this.threadId + " now remove from its work list the local search for search id: " + sc.getQueryIdentifier()
						                + " due to either early abandonment command from coordinator or local evaluation " + "at time stamp: " + System.currentTimeMillis());
					}
					this.workList.remove(sc);
					this.abandonmentCommandsMap.remove(sc.getQueryIdentifier());
				}
				
				abandonmentFromWorkList.clear();
			}
			
			//no, if nothing to do, we will wait on the blocking queue.
			if (!previousHeadProcessed) {
			   //then simply return the one passed from last time, or otherwise, wait and then pick the new one. 
			}
			else {
				 if ( (this.workList.size() == 0) && (this.searchQueue.size() == 0)) {
						//this is how we can have the thread to be blocked. 
						try {
						 
						   if (LOG.isDebugEnabled()){
							   LOG.debug("work list is empty and search queue is empty, to be blocked by search queue taking, runtime thread: " +
							   		                   Thread.currentThread().getId());
						   }
						   headOfQueryForProbing = this.searchQueue.take();
						   
						}
						catch (InterruptedException ex) {
							LOG.error("fails to retrieve a query from the search queue belong to thread: " + this.threadId);
							//simply ignore it. 
						 
						}
				 }
				 else {
					 //in the next run, we will put the incoming queries out in some loop above. 
					 headOfQueryForProbing = null;
				 }
			}
			
			return headOfQueryForProbing;
		}
		
		/**
		 * This is the step to carry out all of the single threaded work. 
		 * 
		 * @param sc
		 * @return
		 */
		protected TimeSeriesSearch.IntermediateQuerySearchResult  doSequentialWork(SearchCoordinator sc)  {
			sc.conductSearchAtRLevel();
			TimeSeriesSearch.IntermediateQuerySearchResult  result = sc.getSearchResultAtRLevel();
			return result;
		}
		
		/**
		 * for testing purpose, we will have to make sure that at the end, it is zero. 
		 * @return
		 */
		public int getWorkListSize() {
			return this.workList.size();
		}
		
		/**
		 * for testing purpose, we will have to make sure that at the end, it is zero.
		 * @return
		 */
		public int getAbandonmentCommandsMapSize() {
			return this.abandonmentCommandsMap.size();
		}
    	
    }
    
    //to specify whether this is simulated or not
    private boolean simulated;
    private SupportedSearchCategory.SupportedSearch chosenSearch;
    
    //The worker thread pool controller.
    private ExecutorService executor; 
    
    //the timer thread to flush the buffered intermediate results 
    private Timer timerBasedScheduler ;
    private BufferedCommunicatorWork bufferedCommunicator;
    private LSHIndexConfigurationParameters configurationParameters;
    private int partitionId; 
   
    //to hold the thread-based processors 
    private List<SearchRelatedProcessor> threadProcessors;
    //each search query is assigned for each worker thread in the thread pool.
    private List<BlockingQueue<SearchQuery>> searchQueuesForWorkerThreads;
    //each worker thread is assigned with a abandonment commands map to hold the query abandonment commands from the coordinator 
    private List<ConcurrentMap <String, Boolean>> abandonmentCommandsMapForWorkerThreads; 
    
    //the sink adaptor to receive the intermediate query results to be sent out.
    private SinkAdaptorForBufferedCommunicator adaptor;
    
    private int  numberOfWorkerThreadsForSearch; 
    
    //for synchronization purpose
    private Object lock = new Object();
    private volatile boolean done = false;
    
    /**
     * the per-worker thread queue to hold the search queries. note that we need to respect the query incoming sequence in each thread
     * queue's partial order. 
     *
     */
  
    /**
     * The partition id will be passed in from the Hadoop map instance
     * @param configurationParameters
     * @param partitionId we receive this partition id from either the Job Tracker's Hadoop partitioning.
     * @param adaptor the pluggable adaptor as the sink to hold the intermediate query results. 
     * @param simulated to state whether the search is to produce actual search result or just dummy search result.
     */
    public MultiSearchManagerImpl (LSHIndexConfigurationParameters configurationParameters, int partitionId,
    		                                       SinkAdaptorForBufferedCommunicator adaptor, 
    		                                       SupportedSearchCategory.SupportedSearch chosenSearch,
    		                                       boolean simulated) {
    	this.chosenSearch=chosenSearch;
    	this.simulated = simulated;
    	
    	this.partitionId = partitionId; 
    	this.configurationParameters = configurationParameters;
    	this.adaptor = adaptor; 
    	if (this.adaptor.isBuffered()) {
    	   this.bufferedCommunicator = new BufferedCommunicator (this.configurationParameters, this.adaptor);
    	}
    	else {
    		this.bufferedCommunicator = new NoBufferedCommunicator(this.configurationParameters, this.adaptor);
    	}
    	
    	
    	this.abandonmentCommandsMapForWorkerThreads = new ArrayList<ConcurrentMap <String, Boolean>>();
    	
        this.numberOfWorkerThreadsForSearch = this.configurationParameters.getNumberOfThreadsForConcurrentQuerySearch();
        this.searchQueuesForWorkerThreads  = new ArrayList<BlockingQueue<SearchQuery>>();
        
        int  sizeOfSeachQueueForWorkThread = this.configurationParameters.getNumberOfQueriesQueuedForWorkerThread();
        for (int i=0; i<this.numberOfWorkerThreadsForSearch; i++) {
        	//creates a LinkedBlockingDeque with the given (fixed) capacity. Then at some point, when the queue is full, the producer will 
        	//get blocked until the particular queue is available for putting the items in again.  
        	//NOTE: the initial capacity of the list make the initial array size to be 0, not the size being the specified capacity.
        	BlockingQueue<SearchQuery> searchQueue = new  LinkedBlockingQueue<SearchQuery> (sizeOfSeachQueueForWorkThread);
        	this.searchQueuesForWorkerThreads.add(searchQueue);
        	
        	//the abandonment commands map per thread
        	ConcurrentMap <String, Boolean> commandsMap = new ConcurrentHashMap <String, Boolean>();
        	this.abandonmentCommandsMapForWorkerThreads.add(commandsMap);
        }
        
        this.executor = Executors.newFixedThreadPool(this.numberOfWorkerThreadsForSearch); 
         
        //the following list will be populated during the launch time.
        this.threadProcessors = new ArrayList<SearchRelatedProcessor> (this.numberOfWorkerThreadsForSearch);
    }
 
    @Override
	public void startQueryProcessors() {
	    launchSearchWorkerThreads( );
	    if (this.adaptor.isBuffered()) {
	       //we only launch the communication buffering management thread, when the adaptor is buffered.
	      launchCommunicationBufferProcessingThread();
	    }
	}
    
    /**
     * Only handling the launch. we will have the separate method to block on the finishing of these worker threads and under normal condition, 
     * this will not happen is the worker threads will run and never finish and exit.
     * 
     * @param numberOfWorkerThreads
     */
    private void launchSearchWorkerThreads() {
    	
    	 CompletionService<AsynIndexSearchCoorindatorIntf.ConductSearchQueriesResponse> ecs = 
    			  new ExecutorCompletionService<AsynIndexSearchCoorindatorIntf.ConductSearchQueriesResponse> (this.executor);
		 
    	 AsynIndexSearchCoorindatorIntf.SearchCoordinatorWorkAsynService service =
		          new  AsynIndexSearchCoorindatorIntf.SearchCoordinatorWorkAsynService();
	
		 //construct all of the requests that will then be assigned to each thread pool's worker thread.
		 List<AsynIndexSearchCoorindatorIntf.ConductSearchQueriesRequest> requests = 
				                              new ArrayList<AsynIndexSearchCoorindatorIntf.ConductSearchQueriesRequest>();
		
		 //prepare all of the parameters necessary for each different worker thread. 
		 int limitedSizeForWorkList= this.configurationParameters.getNumberOfQueriesInWorkListForWorkerThread();
		 for (int i=0; i<this.numberOfWorkerThreadsForSearch; i++) {
			 int logicalThreadId = i;
			 SearchRelatedProcessor processor = new SearchRelatedProcessor
					          (this.chosenSearch,
					          this.simulated, this. configurationParameters, 
			    			  this.searchQueuesForWorkerThreads.get(i), 
			    			  this.abandonmentCommandsMapForWorkerThreads.get(i), 
			    			  this.bufferedCommunicator,
			    			  limitedSizeForWorkList, logicalThreadId, this.partitionId);
			 this.threadProcessors.add(processor);
			 AsynIndexSearchCoorindatorIntf.ConductSearchQueriesRequest request =
					              new  AsynIndexSearchCoorindatorIntf.ConductSearchQueriesRequest (processor); 
			    			  
			 requests.add(request);
		 }
		 
		 //now submit the collection of requests. 
	     Thread.yield();//so that we can have a temporary pause.
		
		 //then produce the asynchronous response 
		 List<Future<AsynIndexSearchCoorindatorIntf.ConductSearchQueriesResponse>> responses = 
				                   new ArrayList<Future<AsynIndexSearchCoorindatorIntf.ConductSearchQueriesResponse>>();
         try {
		    for (AsynIndexSearchCoorindatorIntf.ConductSearchQueriesRequest request : requests) {
               responses.add(service.conductSearchOnQueriesAsync(ecs, request));
            }
         }
         catch (RuntimeException ex) {
             LOG.fatal("launch worker threads for concurrent search fails......", ex);
             //we will produce the report and send to the coordinator as well.
             return;
         }
       
         //now we need to do the polling to retrieve the result and make progress.
         LOG.info ("successfully launching worker threads for concurrent search with number of worker threads: "
                                                                                          + this.numberOfWorkerThreadsForSearch);
        
    }
    
    private void launchCommunicationBufferProcessingThread () {
    	int timeInterval = this.configurationParameters.getScheduledCommunicatorBufferSendingInterval();
    	BufferedSearchResultsDisseminator disseminator =
    			        new BufferedSearchResultsDisseminator(this.bufferedCommunicator);
    	//Creates a new timer whose associated thread has the specified name, and may be specified to run as a daemon (true)
    	this.timerBasedScheduler = new Timer ("Schedule-Flush-Intermediate-Query-Results", true);
    	this.timerBasedScheduler.scheduleAtFixedRate(disseminator, 0, timeInterval);
    	
    	LOG.info ("successfully launching timer thread to flush buffered search results with period of: " + timeInterval + "(milliseconds)");
    	
    }

    /**
     * the way to allow the main thread to be blocked until all of the  worker threads and the timer thread all exit by themselves.
     * This is to just like thread.join. 
     */
    @Override
    public void waitForQueryProcessors() throws InterruptedException {
           //take the next available response.
    	   if (LOG.isDebugEnabled()){
    		   LOG.debug("search manager waits for query processors to be finished");
    	   }
    	   
           //try {
          		//Retrieves and removes the Future representing the next completed task, waiting if none are yet present.
        	    //WARN: when the main thread is blocking here.  after the thread executor is shutdown. this.ecs.take() still does not get 
        	    //out of the waiting. 
          	   // this.ecs.take();
           // }
           // catch (InterruptedException ex) {
           // 	LOG.error("the search thread pool experienced failure: ", ex);
           //}
           
    	  synchronized (lock) {
    		  while (!done) {
    		        lock.wait();
    		  }
    	  }

    }
    
    /**
     * to shutdown both search threads and the timer threads.
     */
    @Override
    public void shutdownQueryProcessors() {
    	shutdownSearchWorkerThreads();
    	if (this.adaptor.isBuffered()) { 
    	  shutdownTimerThread();
    	}
    	
    }
    
    private void shutdownSearchWorkerThreads() {
    	//then finally do the shutdown
    	this.executor.shutdown(); // Disable new tasks from being submitted
    	LOG.info("thread executor to perform concurrent search is now shutdown (1)");
    	try {
    	     // Wait a while for existing tasks to terminate
    	     if (!this.executor.awaitTermination(3, TimeUnit.SECONDS)) {
    	       this.executor.shutdownNow(); // Cancel currently executing tasks
    	       LOG.info("thread executor to perform concurrent search is now shutdown (2)");
    	       // Wait a while for tasks to respond to being cancelled
    	       if (!this.executor.awaitTermination(3, TimeUnit.SECONDS)) {
    	           LOG.error("thread executor did not terminate");
    	       }
    	       LOG.info("thread executor to perform concurrent search is now shutdown (3)");
    	     }
    	} catch (InterruptedException ex) {
    	      // (Re-)Cancel if current thread also interrupted
    	      this.executor.shutdownNow();
    	      // Preserve interrupt status
    	      Thread.currentThread().interrupt();
        }
    	
    	synchronized (lock) {
    	      done = true;
    	      lock.notify();
    	}
    	
    	LOG.info("thread executor to perform concurrent search is now shutdown (4)");
    	   
        //try {
         //  this.executor.shutdownNow(); 
         //  LOG.info("thread executor to perform concurrent search is now shutdown");
           //NOTE: this method can also throw RuntimePermission, which is not a Throwable subclass and thus can not be captured. 
        //}
        //catch (RuntimeException ex) {
       	//  LOG.error("encounter runtime exceptions during concurrent search executor shutdown...ignore...");
        //}
    }
    
    private void shutdownTimerThread() {
    	this.timerBasedScheduler.cancel();
    	LOG.info("timer thread to buffer communicaion is now cancelled");
    }
    
    
	@Override
	public int getPartitionId() {
		 return this.partitionId;
	}

	

	/**
	 * This will be blocked until the destination per-thread queue has some free space to push the new queries. 
	 * At this time, we do not have a temporary queue to hold the incoming queries before they gets distributed to the per-thread queue. 
	 * @queries the queries that are pulled out from the incoming message queue. 
	 * 
	 */
	@Override
	public void acceptAndDistributeSearchQueries(List<SearchQuery> queries) {
	     for (SearchQuery query: queries) {
	    	 this.acceptAndDistributeSearchQuery (query);
	    	  
	     }
	}


	@Override
	public void acceptAndDistributeSearchQuery (TimeSeriesSearch.SearchQuery query) {
		String searchId = query.getQuerySearchId();
   	 	int hashCode = Math.abs(searchId.hashCode()); //it can be a negative integer...
   	 	//take the reminder.
   	 	int destinationThreadId = hashCode%this.numberOfWorkerThreadsForSearch;
   	 	//NOTE we may need to use the Processor's assignQuery, instead of directly putting into the queue.
   	 
   	 	SearchRelatedProcessor processor = this.threadProcessors.get(destinationThreadId);
   	 
   	 	//put(E): Inserts the specified element at the tail of this queue, waiting if necessary for space to become available
   	 	if (LOG.isDebugEnabled()) {
   		  LOG.debug("to push to the search queue for logical thread id: " + destinationThreadId + " for search query: " + searchId);
   	 	}
   	 
   	 	//the processor handles the exception already.
   	 	processor.assignQuery(query);
	}
	
	
	@Override
	public List<IntermediateQuerySearchResult> obtainIntermediateSearchResults() {
		List<TimeSeriesSearch.IntermediateQuerySearchResult> results = this.adaptor.retrieveBufferedSearchResults();
		return  results;
	}

	@Override
	public void acceptAndDistributeSearchQueriesAbandonment(List<QuerySearchAbandonmentCommand> commands) {
		for (QuerySearchAbandonmentCommand command: commands) {
			 this.acceptAndDistributeSearchQueryAbandonment (command);
		}
		
	}
	
	@Override
	public void acceptAndDistributeSearchQueryAbandonment (TimeSeriesSearch.QuerySearchAbandonmentCommand command) {
		int threadId = command.getThreadProcessorId();
		SearchRelatedProcessor processor = this.threadProcessors.get(threadId);
		String searchId = command.getQuerySearchId();
		processor.updateAbandonmentCommands(searchId);
	}

	@Override
	public void registerServiceHandlers(List<ServiceHandler> handlers) {
		// TODO Auto-generated method stub
		
	}


}
