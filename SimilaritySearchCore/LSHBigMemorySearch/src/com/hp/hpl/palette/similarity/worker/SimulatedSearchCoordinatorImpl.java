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


import java.util.Comparator;
import java.util.Random;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.hp.hpl.palette.similarity.bridge.SearchCoordinatorAtRProxy;
import com.hp.hpl.palette.similarity.datamodel.TimeSeriesSearch;



public class SimulatedSearchCoordinatorImpl  implements SearchCoordinator, Comparator <SearchCoordinator> {
	
	private static final Log LOG = LogFactory.getLog(SimulatedSearchCoordinatorImpl.class.getName());
	
	
	private  LSHIndexConfigurationParameters configurationParameters; 
	private int currentRvalue;
	private String searchId; 
	private TimeSeriesSearch.SearchQuery searchQuery;
    private TimeSeriesSearch.IntermediateQuerySearchResult searchResultPerR; 
 
    private int partitionId; //we need this for global communication. 
    private int threadProcessorId;
    
    private boolean localSearchCompleted; 
    //NOTE: we will have to set the R value in the configuration file to be larger than this number..
    private final static int TOTOAL_LEVELS_FOR_R = 8;
    
    public SimulatedSearchCoordinatorImpl  () {
    	//required for the comparator. 
    	this.configurationParameters = null;
    	this.searchId = null;
    	this.searchQuery= null;
    	this.currentRvalue = 0;
    	this.localSearchCompleted = false;
    }
    
	/**
	 * The search coordinator is for a particular search query, and the LSH index configuration parameters get passed in 
	 * from MultiSearchManager, the singleton of the entire process. 
	 * 
	 * @param searchId
	 * @param searchQuery
	 * @param configurationParameters
	 */
	public SimulatedSearchCoordinatorImpl  (String searchId, TimeSeriesSearch.SearchQuery searchQuery,
			     LSHIndexConfigurationParameters configurationParameters, int partitionId, int threadProcessorId) {
		this.configurationParameters = configurationParameters; 
		this.searchId = searchId; 
		this.searchQuery = searchQuery;
		this.currentRvalue = 0;
	 
		this.localSearchCompleted = false;
		this.partitionId = partitionId;  //we will need this later. 
		this.threadProcessorId = threadProcessorId; //we will need this later. 
		this.searchResultPerR = new TimeSeriesSearch.IntermediateQuerySearchResult(
				searchId, this.currentRvalue, partitionId, this.threadProcessorId);
	}
	

	@Override
	public String getQueryIdentifier() {
		return this.searchId;
	}


	@Override
	public TimeSeriesSearch.SearchQuery getQueryPattern() {
		return this.searchQuery;
	}


	@Override
	public int getCurrentSearchRLevel() {
		return this.currentRvalue;
	}


	@Override
	public void conductSearchAtRLevel() {
		//we need to clean up the previous R's result, which should already be sent to the coordinator already.
		if (LOG.isDebugEnabled()) {
		   LOG.debug ("conduct search at current R level: " + this.currentRvalue + " for search query: " + this.searchId 
				   + " with runtime thread: " +  Thread.currentThread().getId());
		}
		//this.searchResultPerR.clear();
		//we need to have a different R value
		this.searchResultPerR = new TimeSeriesSearch.IntermediateQuerySearchResult(
				                    searchId, this.currentRvalue, partitionId, threadProcessorId);
		
	    //simulated work on about 30 ms on DL 980 machine after some initial loops 
		int counter=1250000;
		simulateWorkWithCPU(counter);
		
		if (this.currentRvalue < 2) {
			//2 elements in the first 2 runs. then 3 elements 
			for (int i=0; i<2; i++) {
				TimeSeriesSearch.SearchResult result = new TimeSeriesSearch.SearchResult ();
				result.distance = (float)1.0;
				result.id = 1;
				result.offset = 1;
				this.searchResultPerR.addSearchResult(result);
			}
		}
		else if (this.currentRvalue < 6) {
			//combined from two partitions, we now get the good collective top-5 numbers. 
			for (int i=0; i<3; i++) {
				TimeSeriesSearch.SearchResult result = new TimeSeriesSearch.SearchResult ();
				result.distance = (float)1.0;
				result.id = 1;
				result.offset = 1;
				this.searchResultPerR.addSearchResult(result);
			}
		}
		else {
			for (int i=0; i<6; i++) {
				TimeSeriesSearch.SearchResult result = new TimeSeriesSearch.SearchResult ();
				result.distance = (float)1.0;
				result.id = 1;
				result.offset = 1;
				this.searchResultPerR.addSearchResult(result);
			}
		}
		 
		//at the end, advance the current R to the next level. 
		this.currentRvalue++;
		
	}

	 private void simulateWorkWithCPU (int counter) {
		 
		 Random rand = new Random (System.currentTimeMillis());
	 
		 for (int i=0; i<counter;i++) {
			 double val = rand.nextDouble();
			 double result = Math.sqrt(val);
		 }
		
	 }
	 
	@Override
	public boolean localSearchCompleted() {
		
		int resultSize = this.searchResultPerR.size();
		int minimumSize = this.searchQuery.getTopK();
		if (resultSize >= minimumSize) {
			this.localSearchCompleted =true; 
		}
		else {
			if (this.currentRvalue > TOTOAL_LEVELS_FOR_R) {
				this.localSearchCompleted = true;
			}
		}
		
		return this.localSearchCompleted;
	}
	
	/**
	 * We will need to shut-down the search coordinator in the C++ side. 
	 */
	@Override
	public void abandonSearchAtCurrentRLevel() {
		//do nothing, instead, do logging
		if (LOG.isDebugEnabled()) {
		   LOG.debug ("abandon search at current R level: " + this.currentRvalue);
		}
	}

	@Override
	public TimeSeriesSearch.IntermediateQuerySearchResult getSearchResultAtRLevel() {
		return this.searchResultPerR;
	}

	@Override
	public int compare(SearchCoordinator o1, SearchCoordinator o2) {
		if  ((o1 == null ) || (o2 ==null) ) {
		     return -1;
		}
		else if (o1.getCurrentSearchRLevel() == o2.getCurrentSearchRLevel()) {
			return 0;
		}
		else {
			return ((o1.getCurrentSearchRLevel() < o2.getCurrentSearchRLevel())? -1  : 1);
		}
	}


}
