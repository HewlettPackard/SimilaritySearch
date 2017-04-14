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
package com.hp.hpl.palette.similarity.naivesearch;


import java.util.Comparator;
import java.util.PriorityQueue;
import java.util.List;
import java.util.Set;
import java.util.HashSet;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.hp.hpl.palette.similarity.bridge.SearchCoordinatorAtRProxy;
import com.hp.hpl.palette.similarity.datamodel.TimeSeriesSearch;

import com.hp.hpl.palette.similarity.worker.SearchCoordinator;
import com.hp.hpl.palette.similarity.worker.LSHIndexConfigurationParameters;



public class NaiveSearchCoordinatorImpl implements SearchCoordinator, Comparator <SearchCoordinator> {
	
	private static final Log LOG = LogFactory.getLog(NaiveSearchCoordinatorImpl.class.getName());
	
	
	private LSHIndexConfigurationParameters configurationParameters; 
	private int currentRvalue;
	private String searchId; 
	private TimeSeriesSearch.SearchQuery searchQuery;
	//With "set", instead of list, we can eliminate the duplicated candidates from different R values. 
    private  SearchResultAcrossRs searchResultAcrossRs; 
	//holder to the bridge proxy object.
	SearchCoordinatorAtRProxy searchCoordinatorAtRProxy; 
	private int n_cand; //the candidate that is smaller than R^2. 
	private int total_cand_num; //total number of the candidates that have been computed at the current R level.
    
	private int partitionId; // we need this for the global communication. 
	private int threadProcessorId;
	
	
	private static class SearchResultAcrossRs {
	      private Set<TimeSeriesSearch.SearchResult> searchResultAcrossRs;
	      private int currentRvalue; 
	      
	      public  SearchResultAcrossRs () {
	    	 this.searchResultAcrossRs = new HashSet<TimeSeriesSearch.SearchResult> ();
	    	 this.currentRvalue = 0;
	      }
	      
	      public void add (TimeSeriesSearch.SearchResult result) {
	    	  this.searchResultAcrossRs.add(result);
	      }
	      
	      public void updateCurrentRvalue(int val) {
	    	  this.currentRvalue=val;
	      }
	      
	      public int getCurrentRvalue() {
	    	  return this.currentRvalue;
	      }
	      
	      public Set<TimeSeriesSearch.SearchResult> getSearchResults() {
	    	  return this.searchResultAcrossRs;
	      }
	      
	      public void clear() {
	    	  this.searchResultAcrossRs.clear();
	      }
	}
	
    public NaiveSearchCoordinatorImpl() {
    	//required for the comparator. 
    	this.configurationParameters = null;
    	this.searchId = null;
    	this.searchQuery= null;
    	this.currentRvalue = 0;
    }
    
	/**
	 * The search coordinator is for a particular search query, and the LSH index configuration parameters get passed in 
	 * from MultiSearchManager, the singleton of the entire process. 
	 * 
	 * @param searchId
	 * @param searchQuery
	 * @param configurationParameters
	 */
	public NaiveSearchCoordinatorImpl (String searchId, TimeSeriesSearch.SearchQuery searchQuery,
			     LSHIndexConfigurationParameters configurationParameters, int partitionId, int threadProcessorId) {
		this.configurationParameters = configurationParameters; 
		this.searchId = searchId; 
		this.searchQuery = searchQuery;
		this.currentRvalue = 0;
	 
		this.searchCoordinatorAtRProxy = new SearchCoordinatorAtRProxy();
		int initial_part_no = 1; //we do not make partition across different L tables at this time.
		int topK_no= this.searchQuery.getTopK();
		float[] queryPattern = searchQuery.getQueryPattern();
		this.searchCoordinatorAtRProxy.initiateSearchCoordinatorAtR (this.currentRvalue, 
				   this.searchId, initial_part_no, initial_part_no, queryPattern, topK_no);
		this.partitionId= partitionId; //we will need to update this later for global communication
		this.threadProcessorId = threadProcessorId;  //we will need to update this later for global communication
		this.searchResultAcrossRs = new SearchResultAcrossRs();
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


	/**
	 * Note the implementation at this time is to use the single thread to perform this method. 
	 */
	@Override
	public void conductSearchAtRLevel() {
		if (LOG.isDebugEnabled()) {
			LOG.debug("entering local search at R: " + this.currentRvalue + " for search id: " + this.searchId);
		}
		
		searchCoordinatorAtRProxy.testNaiveSearch(0, searchId);
	
	    {
			 
			while(true) {
				TimeSeriesSearch.SearchResult result = searchCoordinatorAtRProxy.getSearchResultAtRLevel(0, this.searchId);
				if(result == null) {
					break; //no more result to pull.
				}
				//totalcandidateSet.add(result);
				//the search result gets keep added, from R0 to R1, to R2. 
				this.searchResultAcrossRs.add(result);
			}
		}
	 
		//now form the top-K result at the Java side, locally.
		this.searchResultAcrossRs.updateCurrentRvalue(this.currentRvalue);
		
		searchCoordinatorAtRProxy.shutdownSearchAtRLevel(0, searchId);
		
		//at the end, advance the current R to the next level. 
		this.currentRvalue++;
		
		if (LOG.isDebugEnabled()) {
			LOG.debug("existing local search at R: " + this.currentRvalue  +  "(next R value to be computed)" + " for search id: " + this.searchId);
		}
	}
	
	/**
	 * This is to decide that locally, we should advance to the next R value or not. Independent of whether we receive 
	 * the early abandonment results from the Coordinator.  
	 * 
	 * Depending on the our test results, we may have more sophisticated and pluggable module to allow to the decision logic of terminating local search
	 * to be specified in a more flexible way.
	 */
	@Override
	public boolean localSearchCompleted() {
		//for Naive search, only R value = 1.
		if (this.currentRvalue == 1) {
			return true;
		}
		return false;
	}

	/**
	 * We will need to shut-down the search coordinator in the C++ side. 
	 */
	@Override
	public void abandonSearchAtCurrentRLevel() {
		//remove this from the C++ side and the over-all search.
		this.searchCoordinatorAtRProxy.removeSearchCoordinator(this.searchId);
	}

	/**
	 * this will be called for each R and then gets sent to the coordinator. We may consider the efficiency later.
	 */
	@Override
	public TimeSeriesSearch.IntermediateQuerySearchResult getSearchResultAtRLevel() {
	   
		TimeSeriesSearch.IntermediateQuerySearchResult resultsAtRLevel = 
				new TimeSeriesSearch.IntermediateQuerySearchResult(this.searchId, 
						this.searchResultAcrossRs.getCurrentRvalue(), 
						this.partitionId, 
						this.threadProcessorId); 
		//so that the searchResultPerR can continue to grow, and take a snapshot to tcurrentResults.
		Set<TimeSeriesSearch.SearchResult> tcurrentResults = this.searchResultAcrossRs.getSearchResults();
		for (TimeSeriesSearch.SearchResult sr: tcurrentResults) {
			resultsAtRLevel.addSearchResult(sr);
		}
		
		// generate top-k, us the priority queue to automatically sort out the distance, with all of the search results for the accumualative R values 
		// so far. 
		PriorityQueue<TimeSeriesSearch.SearchResult> topK = new PriorityQueue<TimeSeriesSearch.SearchResult>();
		int topK_no = this.configurationParameters.getTopK();
		if (resultsAtRLevel.size() > 0) {
			List<TimeSeriesSearch.SearchResult> currentResults = resultsAtRLevel.getSearchResults();
			for (TimeSeriesSearch.SearchResult unorderedSearchResult: currentResults) {
				if(topK.size() < topK_no) {
	    			topK.add(unorderedSearchResult);
	    		} else {
	    			TimeSeriesSearch.SearchResult  mt = topK.peek();
		    		if(mt.distance > unorderedSearchResult.distance) {
			    		topK.poll();
					 	topK.add(unorderedSearchResult);
			    	}
	    		}
			}
			
			//clean up only the temporary result.
			resultsAtRLevel.clear();
            //now we want to take the result back to search result.
            int size = topK.size(); 
			//now we want to take the result back to search result.
			for(int pq=0;pq<size;pq++) { //updated by Mijung 
				TimeSeriesSearch.SearchResult ms = topK.poll();
				resultsAtRLevel.addSearchResult(ms);
			}
		}
				
		
		return resultsAtRLevel;
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

