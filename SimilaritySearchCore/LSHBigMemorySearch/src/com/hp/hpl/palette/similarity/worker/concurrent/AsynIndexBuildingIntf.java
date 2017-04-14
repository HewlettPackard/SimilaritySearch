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
package com.hp.hpl.palette.similarity.worker.concurrent;

import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletionService;
import java.util.concurrent.Future;

import com.hp.hpl.palette.similarity.worker.IndexBuilder.PartitionOfLTablesAtRForIndexBuilding;
import com.hp.hpl.palette.similarity.bridge.IndexBuilderProxy;

import com.hp.hpl.palette.similarity.worker.IndexBuilderImpl;

/**
 * To define the necessary data models and interfaces to support asynchronous + future based call invocation launched from the
 * fixed thread pools from the Java executor.  
 *  
 * @author Jun Li
 *
 */
public interface AsynIndexBuildingIntf {
	
	
	interface IndexBuildingWork {
	       boolean conductIndexBuilding (List<PartitionOfLTablesAtRForIndexBuilding> identifiedPartitions, int index, int queryLength);
	}
    
	public class ConductIndexBuildingRequest {
		   private List<PartitionOfLTablesAtRForIndexBuilding> identifiedPartitions;
		   private int index;
		   private int queryLength;
		   
		   public ConductIndexBuildingRequest (List<PartitionOfLTablesAtRForIndexBuilding> identifiedPartitions, int index, int queryLength) {
			  this.identifiedPartitions = identifiedPartitions;
			  this.index= index;
			  this.queryLength=queryLength;
		   }
		   
		   public List<PartitionOfLTablesAtRForIndexBuilding> getIdentifiedParitions () {
			   return this.identifiedPartitions;
		   }
		   
		   public int getIndex () {
			   return this.index;
		   }
		   
		   public int getQueryLength() {
			   return this.queryLength;
		   }
	} 

	public class ConductIndexBuildingResponse {
		private boolean result; 
		private int partitionIndex; //the partition index.
		
		public ConductIndexBuildingResponse (boolean result, int partitionIndex){
			this.result =  result;
			this.partitionIndex = partitionIndex;
		}
		
		public boolean getResponse() {
			return this.result;
		}
		
		public int getPartitionIndex() {
			return this.partitionIndex;
		}
	}

	public interface IndexBuildingWorkLocalAsyncServiceIntf  extends IndexBuildingWork {
 
        public Future<ConductIndexBuildingResponse> conductIndexBuildingAsync(
                      CompletionService<ConductIndexBuildingResponse> ecs, final ConductIndexBuildingRequest request);

	}
	
	public class IndexBuildingWorkAsynService extends IndexBuilderImpl implements IndexBuildingWorkLocalAsyncServiceIntf {

	 
		@Override
		public Future<ConductIndexBuildingResponse> conductIndexBuildingAsync(
				CompletionService<ConductIndexBuildingResponse> ecs,
				final ConductIndexBuildingRequest request) {
			//NOTE: there are possible runtime exceptions of RejectedExecutionException and NullPointerException
	    	//that are thrown from the following method.
	    	Future<ConductIndexBuildingResponse> response =ecs.submit(new Callable<ConductIndexBuildingResponse>() {

	                public ConductIndexBuildingResponse call() {
	                	List<PartitionOfLTablesAtRForIndexBuilding> identifiedPartitions = request.getIdentifiedParitions();
	                	int index = request.getIndex();
	                	int queryLength = request.getQueryLength();
	                	//the following method is defined in the IndexBuildingImpl.
	                	boolean result = conductIndexBuilding(identifiedPartitions, index, queryLength);
	                	
	                    ConductIndexBuildingResponse response = new ConductIndexBuildingResponse (result, index);
	                	return response;
	                }
	            });
	        return response;
		}
		
	}
}
