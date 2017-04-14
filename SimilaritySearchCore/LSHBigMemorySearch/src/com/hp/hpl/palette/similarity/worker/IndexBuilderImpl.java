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
import java.util.concurrent.CompletionService;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import com.hp.hpl.palette.similarity.comm.ServiceHandler;
import com.hp.hpl.palette.similarity.progress.IndexBuildingProgressStatus;
import com.hp.hpl.palette.similarity.worker.IndexBuilder.PartitionOfLTablesAtRForIndexBuilding;
import com.hp.hpl.palette.similarity.worker.concurrent.AsynIndexBuildingIntf;
import com.hp.hpl.palette.similarity.bridge.IndexBuilderProxy;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.commons.logging.Log; 
import org.apache.commons.logging.LogFactory;

public class IndexBuilderImpl implements IndexBuilder {

	private static final Log LOG = LogFactory.getLog(IndexBuilderImpl.class.getName());
	
	private  ExecutorService executor;
	private  AtomicBoolean abortIndexBuilding;
	private  LSHIndexConfigurationParameters configurationParameters; 
	/**
	 * Note that R value starts with 0, and also L table index starts with 0 as well.
	 * 
	 * @author Jun Li
	 *
	 */
	public static class PartitionOfLTablesAtRForIndexBuildingImpl implements  PartitionOfLTablesAtRForIndexBuilding {

	    private int inclusiveLow; //the inclusive low value;
	    private int inclusiveHigh; //the inclusive high value;
	    private int Rvalue;
	   
        public PartitionOfLTablesAtRForIndexBuildingImpl () {
	    	
	    }
	    
	    public PartitionOfLTablesAtRForIndexBuildingImpl (int Rvalue, int low, int high) {
	    	this.inclusiveLow = low;
	    	this.inclusiveHigh = high;
	    	this.Rvalue = Rvalue;
	    }
	    
	
		@Override
		public void setRangeLowForLTables(int val) {
			this.inclusiveLow = val;
		}

		@Override
		public void setRangeHighForLTables(int val) {
			 this.inclusiveHigh = val;	
		}

		@Override
		public int getRangeLowForLTables() {
			return this.inclusiveLow;
		}

		@Override
		public int getRangeHighForLTables() {
		    return this.inclusiveHigh;
		}

		@Override
		public void setRValue(int val) {
		    this.Rvalue =val;
		}

		@Override
		public int getRValue() {
			return this.Rvalue;
		}
		
	}
	
	public IndexBuilderImpl() {
		//
	}
	
	public IndexBuilderImpl (LSHIndexConfigurationParameters configurationParameters) {
		this.configurationParameters = configurationParameters;
		int numberOfWorkerThreads = this.configurationParameters.getNumberofWorkerThreadsForIndexing();
		this.executor = Executors.newFixedThreadPool(numberOfWorkerThreads);
		this.abortIndexBuilding = new AtomicBoolean(false);
	}
	
	//to define the asynchronous interface that we need to have to wrap about the method of conductIndexBuilding
	@Override
	public List<PartitionOfLTablesAtRForIndexBuilding> getRangePartitionForIndexBuilding() {
	   int RNumber  = this.configurationParameters.getRNumber(); 
	   int LNumber  = this.configurationParameters.getLNumber(); 
	   
	   ArrayList<PartitionOfLTablesAtRForIndexBuilding> partitions  = new   ArrayList<PartitionOfLTablesAtRForIndexBuilding>();
	   //the partition is along the L dimension only, as L will be typically bigger than the number of the worker threads.
	   int tablesAssignedToAPartition = this.configurationParameters.getNumberOfLTablesInAIndexingGroup();
	   
	   for (int i= 0; i <= RNumber-1; i++) {
		   int rValue = i;
		   int low = 0; 
		   int high = tablesAssignedToAPartition - 1;  
		   while (true) {
			   if (high > LNumber-1) {
				   high = LNumber-1;  //just put the ceiling at the right. 
			   }
			   PartitionOfLTablesAtRForIndexBuilding parition = new PartitionOfLTablesAtRForIndexBuildingImpl (rValue, low, high);
			   partitions.add(parition);
			   //advance to the next partition 
			   low = low + tablesAssignedToAPartition; 
			   high = high + tablesAssignedToAPartition;
			   if (low > LNumber-1) {
				   break; //no more partition, advance to the next R value
			   }
		   }
	   }
	   
	   return partitions;
	}

	@Override
	public void startIndexBuilding(List<PartitionOfLTablesAtRForIndexBuilding> identifiedPartitions) {
	
		 CompletionService<AsynIndexBuildingIntf.ConductIndexBuildingResponse> ecs = 
	              new ExecutorCompletionService<AsynIndexBuildingIntf.ConductIndexBuildingResponse> (this.executor);
		 
		 AsynIndexBuildingIntf.IndexBuildingWorkAsynService service =
		                new AsynIndexBuildingIntf.IndexBuildingWorkAsynService();
	
		 //construct all of the requests that will then be assigned to each thread pool's worker thread.
		 List<AsynIndexBuildingIntf.ConductIndexBuildingRequest> requests = new ArrayList<AsynIndexBuildingIntf.ConductIndexBuildingRequest>();
		 
		 int size= identifiedPartitions.size();
		 int queryLength = this.configurationParameters.getQueryLength();
		 for (int i=0; i<size; i++) {
			 AsynIndexBuildingIntf.ConductIndexBuildingRequest request =
					 new  AsynIndexBuildingIntf.ConductIndexBuildingRequest (identifiedPartitions, i, queryLength);
			 requests.add(request);
		 }
		 
		 //now submit the collection of requests. 
	     Thread.yield();//so that we can have a temporary pause.
		
		 //then produce the asynchronous response 
		 List<Future<AsynIndexBuildingIntf.ConductIndexBuildingResponse>> responses = 
				                   new ArrayList<Future<AsynIndexBuildingIntf.ConductIndexBuildingResponse>>();
         try {
 		  for (AsynIndexBuildingIntf.ConductIndexBuildingRequest request : requests) {
                 responses.add(service.conductIndexBuildingAsync(ecs, request));
           }
         }
         catch (RuntimeException ex) {
             LOG.fatal("index building fails......", ex);
             //we will produce the report and send to the coordinator as well.
             return;
         }
         
         //now we need to do the polling to retrieve the result and make progress.
         LOG.info ("submit all the index building tasks, proceed to the polling loop...");
         
         
         int successfullPartionIndexBuilt = 0; 
         boolean allPartitionDone = false;
         this.abortIndexBuilding.set(false) ;// this may be updated later by a different thread with the command from the master.
         int counter = 0;
         long specifiedWaitTime = 500;//500 milliseconds, we do not need that fast...
         while((!allPartitionDone && !this.abortIndexBuilding.get())) {
           //take the next available response.
           try {
         		//block until the next result shows.
         	   
         	    Future<AsynIndexBuildingIntf.ConductIndexBuildingResponse> polledResponse= 
         			               ecs.poll(specifiedWaitTime, TimeUnit.MILLISECONDS);
         		if (polledResponse !=null) {
         			AsynIndexBuildingIntf.ConductIndexBuildingResponse result = polledResponse.get();
         		  if (result !=null) {
         			 AsynIndexBuildingIntf.ConductIndexBuildingResponse response = result;
         		    
         			 boolean status = response.getResponse();
         			 int index = response.getPartitionIndex();
         			 PartitionOfLTablesAtRForIndexBuilding partition = identifiedPartitions.get(index);
         			 
         		     //process the response
         		     boolean responseResult = response.getResponse();
                      if (responseResult) {
                    	  successfullPartionIndexBuilt++;
                    	  if (LOG.isInfoEnabled()){
           					LOG.info ("just finish the index building: " + status
                 					  + " for the partition of : " + index  +
                 					  " which corresponding to R value: " + partition.getRValue() + 
                 					  " and range of L tables in [" + partition.getRangeLowForLTables() + ", " + partition.getRangeHighForLTables() + "]");
           				  }
                      }
                      if (successfullPartionIndexBuilt  == identifiedPartitions.size()) {
                    	  allPartitionDone = true;
                    	  if (LOG.isInfoEnabled()){
             					LOG.info ("just finish the index building for all partitions");
             			  }
                      }
         		  }
         		}
         		
           }
           catch (ExecutionException ignore) {
         		LOG.error("encounter thread pool execution error, then ignore...", ignore);
           }
           catch (InterruptedException ignore) {
         		LOG.error("encounter thread pool interrupt exception, then ignore...", ignore);
           }
          
           if (this.abortIndexBuilding.get()) {
        	   LOG.info("index builder proceeds to abort index building....");
        	   break; 
           }
           
         } //end the while loop.
         
         if (this.abortIndexBuilding.get()) {
        	 //cancel the future tasks that are still pending in the queue;
        	 for (Future<AsynIndexBuildingIntf.ConductIndexBuildingResponse> future : responses) {
                future.cancel(true);
             }
        	 
        	 //remove the future tasks that have been cancelled, in an active manner, instead of wait until
             //runtime storage reclamation.
        	 ThreadPoolExecutor theExecutor = (ThreadPoolExecutor)this.executor; 
             theExecutor.purge();
         }
         
         //then finally do the shutdown
         try {
            this.executor.shutdownNow(); 
            LOG.info("thread executor to perform parallel index building is now shutdown");
            //NOTE: this method can also throw RuntimePermission, which is not a Throwable subclass and thus can not be captured. 
         }
         catch (RuntimeException ex) {
        	 LOG.error("encounter runtime exceptions during the index building executor shutdown...ignore...");
         }
         
	}

	@Override
	public boolean conductIndexBuilding(
			List<PartitionOfLTablesAtRForIndexBuilding> identifiedPartitions, int index, int queryLength) {
		 IndexBuilderProxy proxy = new IndexBuilderProxy();
		 //int Rindex, int L_low, int L_high, int querylength
		 int Rindex = identifiedPartitions.get(index).getRValue();
		 int L_low = identifiedPartitions.get(index).getRangeLowForLTables();
		 int L_high= identifiedPartitions.get(index).getRangeHighForLTables();
		 boolean result = proxy.buildIndex(Rindex, L_low, L_high, queryLength);
		 return result;
	}

	
	@Override
	public void updateProgress(IndexBuildingProgressStatus status) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void registerServiceHandlers(List<ServiceHandler> handlers) {
		// TODO Auto-generated method stub
		
	}

	/**
	 * ToDo: Still, if we simply abort then the C++ side will have some tables already built up, and that the occupied memory 
	 * will have to be released. However, to release the memory, we will have to wait until the worker threads in 
	 * the method: startIndexBuilding have exited the loop and the corresponding executors get shut down 
	 */
	@Override
	public void abortIndexBuilding() {
		LOG.info ("index builder accepts to abort index building....");
		this.abortIndexBuilding.set(true);
		
	}

}
