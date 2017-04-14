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

import com.hp.hpl.palette.similarity.comm.ServiceHandler;
import com.hp.hpl.palette.similarity.datamodel.TimeSeriesSearch;
import com.hp.hpl.palette.similarity.progress.IndexBuildingProgressStatus;

/**
 * to support the functionalities of building the LSH index based on the time series data partition that has been loaded into the 
 * local memory of the Map instace.
 * 
 * @author Jun Li
 *
 */
public interface IndexBuilder {
	  
	  /**
	   * The partition is based on the single value of R and the range values of L tables. 
	   *
	   */
	  interface PartitionOfLTablesAtRForIndexBuilding {
		 void setRangeLowForLTables(int val);  //inclusive
	 	 void setRangeHighForLTables(int val);  //inclusive
	 	 
	 	 int  getRangeLowForLTables();  //inclusive
	 	 int  getRangeHighForLTables();  //inclusive
	 	 
	 	 void setRValue(int val); //to assign the R value level
	 	 int getRValue(); //to get the R value level
	 	
	  }
	  
	  /**
	   * The partition is based on the actual R and L values, and based on the number of the worker threads. 
	   * 
	   * A simple way is to have each R,  we divide the  number of the L tables for the assigned number of the worker threads. Then we have so many of 
	   * such partitions across the different R values.
	   * 
	   * @param parameters
	   * @param numberOfWorkerThreads
	   * @return
	   */
	  List<PartitionOfLTablesAtRForIndexBuilding> getRangePartitionForIndexBuilding ();
	  
	  
	  /**
	   * This is to launch the thread pools (the thread executor) to conduct the index building, given the number of the assigned worker threads from
	   * the thread pool.
	   * 
	   * @param identifiedPartitions
	   */
	  void startIndexBuilding(List<PartitionOfLTablesAtRForIndexBuilding> identifiedPartitions);
		 
	  /**
	   *For each work thread,  there will be range specified in the designated partition for a worker thread to conduct the search. 
	   *
	   *NOTE: this method gets delegated to the corresponding call to the IndexBuilderProxy.
	   *The search result will be held at the C++ side and can be referenced via the SearchCoordiantorProxy.
	   */
	  boolean conductIndexBuilding(List<PartitionOfLTablesAtRForIndexBuilding> identifiedPartitions, int index, int queryLength);
	  
	  /**
	   * In case something goes wrong, the coordinator send the command to the index builder to abort the current index building. 
	   * 
	   */
	  void abortIndexBuilding();
		
	  
	  /**
		 * to update the time series data loading and time series index building progress  to the coordinator master, which can then be 
		 * displayed for progress monitoring purpose. The update progress can be done at the end of each worker thread that finishes the 
		 * assigned index building work. 
		 * 
		 * @param status
		 */
	  void updateProgress(IndexBuildingProgressStatus status); 
	  
	  /**
	   * ToDO: to be further defined once we have better understanding on how Netty works. We will need to 
	   * have multiple service listeners:
	   * 
	   * (1) Accept and then start the index building process;  (and respond based on the acceptance results); 
	   *
	   */
	  
	  void registerServiceHandlers(List<ServiceHandler> handlers);
	 
}
