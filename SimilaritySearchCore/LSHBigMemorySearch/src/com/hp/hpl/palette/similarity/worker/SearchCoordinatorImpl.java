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
import java.util.PriorityQueue;
import java.util.List;
import java.util.Set;
import java.util.HashSet;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.hp.hpl.palette.similarity.bridge.SearchCoordinatorAtRProxy;
import com.hp.hpl.palette.similarity.configuration.SimilaritySearchConfiguration;
import com.hp.hpl.palette.similarity.datamodel.TimeSeriesSearch;



public class SearchCoordinatorImpl implements SearchCoordinator, Comparator <SearchCoordinator> {
	
	private static final Log LOG = LogFactory.getLog(SearchCoordinatorImpl.class.getName());
	
	
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
	
    public SearchCoordinatorImpl() {
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
	public SearchCoordinatorImpl (String searchId, TimeSeriesSearch.SearchQuery searchQuery,
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
		//we need to clean up the previous R's result, which should already be sent to the coordinator already.
		//this.searchResultPerR.clear();
		boolean needToSkip = false; //to decide whether the candidate number is too small and thus we will need to skip to the next R. 
		int i = this.currentRvalue;
		int L= this.configurationParameters.getLNumber();
		int R_num = this.configurationParameters.getRNumber();
		int p = this.configurationParameters.getNumberOfPerturbations();
		this.searchCoordinatorAtRProxy.conductSearchAtRLevel(i, this.searchId, 0, 0, L-1, p); 
	 
		// merge candidate;
		this.searchCoordinatorAtRProxy.mergeSearchCandidateResultAtRLevel(i, searchId, 1);  // to merge different L tables	
		this.total_cand_num = this.searchCoordinatorAtRProxy.getTotalMergedSearchCandidates(i, this.searchId);
		if (LOG.isDebugEnabled()) {
			LOG.debug("local search at R: " + this.currentRvalue + "total candidates identified: "  + this.total_cand_num + " for search id: " + this.searchId);
		}
	 
		//NOTE: we do not have the following logic as we will wait for the global coordinator to tell this partition to stop. 
		//once the total number of the candidates retrieved globally meet the top_k requirement. 
		//if(this.currentRvalue < R_num-1) {
			//if the total number of candidates for each R is less than 3000, 
			//then merge the candidates of the next R without computing the top-k.
		   // if(this.total_cand_num < SimilaritySearchConfiguration.CANDIDATE_NUMBER_THRESHOLD) {
		 	//  needToSkip=true; 
		   // }
		//}
		
		if (!needToSkip) {
			// naive search
			searchCoordinatorAtRProxy.conductNaiveSearchOnMergedCandidatesAtRLevel (i, this.searchId, 0, this.total_cand_num-1, 0);
			// Mijung (4/23/14): the function is not used any more
			//this.n_cand = searchCoordinatorAtRProxy.mergeNaiveSearchResultAtRLevel(i, this.searchId, 1); // to merge different ranges of candidates
			// Added by Mijung(4/23/14): get statistics for search results	
			int[] ncand = searchCoordinatorAtRProxy.getSearchResultStatistics(i, searchId); 
			this.n_cand= ncand[0]; //the candidate that is smaller than R^2.
			
			while(true) {
				TimeSeriesSearch.SearchResult result = searchCoordinatorAtRProxy.getSearchResultAtRLevel(i, this.searchId);
				if(result == null) {
					break; //no more result to pull.
				}
				//totalcandidateSet.add(result);
				//the search result gets keep added, from R0 to R1, to R2. 
				this.searchResultAcrossRs.add(result);
			}
		}
		else {
			//what is the n_cand value then? 
			if (LOG.isDebugEnabled()) {
				LOG.debug("local search at R: " + this.currentRvalue +  " total_cand_num(< 3000): "
			                        + this.total_cand_num + "thus skip the naive search " + " for search id: " + this.searchId);
			}
			this.n_cand=0;
		}
		
		//now form the top-K result at the Java side, locally.
		this.searchResultAcrossRs.updateCurrentRvalue(this.currentRvalue);
		
		if(!needToSkip) {
			if (LOG.isDebugEnabled()) {
				LOG.debug("local search at R: " + this.currentRvalue +  " finding n_candidate (less than R^2): " + this.n_cand +  " top K: " + 
			                                    this.searchResultAcrossRs.getSearchResults().size() + " for search id: " + this.searchId);
			}
		}
		
		searchCoordinatorAtRProxy.shutdownSearchAtRLevel(i, searchId);
		
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
		//stop when the number of the R already reaches. We are done. 
		if (this.currentRvalue == this.configurationParameters.getRNumber()) {
			return true;
		}
		else {
			//we may be able to terminate earlier. 
			//NOTE: let's follow LSHtest.java to have 10 candidates, for the top K = 5. 
			//NOTE: this needs to be the same as IndexBuildingCheckerImpl.java and LSHtest.java
			//if (this.total_cand_num < SimilaritySearchConfiguration.CANDIDATE_NUMBER_THRESHOLD) {
			//	return false; //we will have to move on to the next, as we still have more R's to go. 
			//}
			//else {
				// commented by Mijung because n_cand may have candidates with a partial distance
				//if (this.n_cand >= this.searchQuery.getTopK()*2) {
				 //  return true; 
			    //}
				if (this.searchResultAcrossRs.getSearchResults().size() >= this.searchQuery.getTopK()) {
					return true;
				}
			    else {
				   return false;
			    }
			//}
			
			//NOTE: how to translate the following decision logic from LSHtest.java, line 278
			//if(i < R_num-1) {
			//if the total number of candidates for each R is less than 3000, 
			//then merge the candidates of the next R without computing the top-k.
			//if(total_cand_num < 3000)
			//	continue;
		    //}
		}
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
