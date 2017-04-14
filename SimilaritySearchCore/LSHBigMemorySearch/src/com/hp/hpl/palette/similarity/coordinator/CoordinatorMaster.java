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
package com.hp.hpl.palette.similarity.coordinator;

/**
 * The master of the coordinator that coordinates time-series similarity search's indexing and searching with the long-lived 
 * Map instances (worker)
 * 
 * There are three phases: 
 * 
 * (1) time-series data loading (with the initial pointer that holds at the time the Map is started)
 * 
 * (2) index -building
 * 
 * (3) search that can support the early abandonment if it turns out that the search can be done without walking through all of the R's. 
 * 
 * 
 * @author Jun Li
 *
 */
public interface CoordinatorMaster {
       
	  /**
	   * to identify how many partitions for the Map instances, each one of which is assigned to a worker in the MapReduce framework, 
	   * will need for the time series data;
	   *  
	   * @return
	   */
	  int getNumberOfPartition ();
	  
	  /**
	   * to initialize the synchronization and scheduling related ports, instead of wait until very late.
	   */
	  void init();
	  /**
	   * to synchronize all of the partitioners that are ready to accept commands and produce results.
	   */
	  void synchronizeWithAllPartitions();
	  
	  /**
	   * to use the round-robin mechanism to schedule the index building for all of the partitioners that share the sam private IP addresss (
	   * that is, the NUMA node on the large multicore big-memory machine). 
	   */
	  boolean scheduleIndexBuildingForAllPartitions(); 
	  
	  /**
	   * There are three phases of the time series search: (1) data preparation, to upload the time series and LSH parameters into each Map instance's 
	   * local memory;
	   * 
	   * (2) to build the LSH index by each Map instance and hold the index in its local memory;
	   * 
	   * (3) to conduct search queries using LSH index look up and then apply Naive search against the reduced search data set. 
	   * 
	   * Each of the phase is mutual exclusive to the other phase, and the coordinator client can query the current phase and the detailed 
	   * progress associated with the phase. 
	   * 
	   *
	   */
	  enum TimeSeriesSearchPhase {
		  DATA_LOADING (0),
		  INDEX_BUILDING(1),
		  INDEX_SEARCH (-1);
		  
		  int state;
		  private TimeSeriesSearchPhase (int state) {
		    this.state = state;
		  }
	  }
	 
	 
	  /**
	   * to query the current phase that the current system is located. 
	   * 
	   * @return the current search phase 
	   */
	  TimeSeriesSearchPhase getCurrentPhase(); 
	  
	  interface QueryProcessingCoordinator  {
		  
	  }
	  
	  interface QueryResultDistributor {
		  void acceptAndPublishFinalSearchRequest();
	  }
	  
	  interface QueryRequestAcceptorDistributor{
		  void acceptAndPublishTimeSeriesSearchRequest(); 
		  
	  }
	  
	  
}
