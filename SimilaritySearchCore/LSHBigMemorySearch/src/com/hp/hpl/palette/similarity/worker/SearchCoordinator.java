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

import java.util.UUID;
import java.util.List;

import com.hp.hpl.palette.similarity.datamodel.TimeSeriesSearch;
import com.hp.hpl.palette.similarity.datamodel.TimeSeriesSearch.SearchQuery;

/**
 * to support the functionalities of conducting a single query search for a given query pattern, in a single thread (belonging to a thread 
 * in the worker thread pool managed by the MultiSearchManager.
 * 
 * For each query, the search coordinate coordinate the 3 phases of the search that is conducted at the C++ side:
 * 
 *    (1) search against the specified R and  all of the L;
 *    (2) merge the result 
 *    (3) compute the naive search result based on the specified R at this time. 
 *    (4) notify to the master the current search result with the specified R. 
 *    (4) check whether there is an abortion command for the current search. if so, terminate the search 
 *              locally whether the search can be safely terminated already, as locally we get the top-K results. 
 *    (5) otherwise, advance to the next R's search. 
 *    
 * 
 * @author Jun Li
 *
 */
public interface SearchCoordinator {
	 
	
	/**
	 * To obtain the query identifier that is assigned to this search 
	 * @return
	 */
	String getQueryIdentifier();
    
    /**
     * to obtain the query pattern associated with this search;
     * @return
     */
    TimeSeriesSearch.SearchQuery getQueryPattern();
    
    
    /**
     * Each search is started at R level 0, and then advance to R1, R2, if not enough candidates can be found. 
     * @return
     */
    int getCurrentSearchRLevel();
    
    
    /**
     * to start the search level at the specified R. The search query already constraints  the top-K number that needs to be found. 
     */
    void conductSearchAtRLevel();
    
    
    /**
     * there needs to be a local evaluation to decide whether the local search can now be terminated, this is in addition to whether 
     * the abandonment command is received or not from the coordinator. 
     * 
     * @return
     */
    boolean localSearchCompleted();
   
	/**
	 * Abort the search that is currently on going, rather than keeping it advanced to cover all of the R level. 
	 */
	void abandonSearchAtCurrentRLevel();
	
	/**
	 * We will retrieve the final merged research result at the R level, and after that, to ship this intermediate result to the coordinator. 
	 * 
	 * @return the search result, or if not done yet then return <code>null</code>
	 */
	TimeSeriesSearch.IntermediateQuerySearchResult getSearchResultAtRLevel(); 

}
