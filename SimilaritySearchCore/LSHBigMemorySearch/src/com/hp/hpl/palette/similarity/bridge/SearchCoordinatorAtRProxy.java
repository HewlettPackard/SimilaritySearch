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
package com.hp.hpl.palette.similarity.bridge;

import java.util.List;

import com.hp.hpl.palette.similarity.datamodel.TimeSeriesSearch;
import com.hp.hpl.palette.similarity.worker.SearchCoordinator;

/**
 * This class is the proxy to SearchCoordinatorAtR defined within SearchCoordinator. 
 * 
 * @author Jun Li (modified by Mijung Kim)
 *
 */
public class SearchCoordinatorAtRProxy {

	public SearchCoordinatorAtRProxy () {
		//do nothing.
	}
	
	/**
	 * 
	 * @param RIndex the index of the R that will be used to conduct the search
	 * @param searchId the search identifier associated with the time series search 
	 * @param mergeResultSize see the explanation below in naive search size 
	 * @param naiveSearchSize see the explanation below
	 * 
	 * Each MergedResult or NaiveSearch instance corresponds to a thread (worker) that is allocated to handle the merge and the naïve search.
	 * So the total number of MergedResult and NaiveSearch, i.e., the mergeResultSize, and naiveSearchSize,  is the total number of the threads allocated
	 * to handle merging and then doing the naive search.  

     * For example, if we are using three ranges for L tables, and two ranges of the candidates, then mergeResultSize=3 and naiveSearchSize=2. 
     * These two parameters will be defined based on the size of the thread pool. 
    
     *
	 * @param queryPattern the time series search pattern. 
	 * @param topk_no  how many top-most similar time series you to get back finally
	 * @return
	 */
	public  boolean  initiateSearchCoordinatorAtR (int RIndex, String searchId, int mergeResultSize, int naiveSearchSize, float[] queryPattern, int topk_no) {
		//to be filled with the native method
		long lshManagerPointer =  LSHManagerProxy.INSTANCE.getPointer();
		boolean result = this.initiateSearchCoordinatorAtR(lshManagerPointer, RIndex, searchId, mergeResultSize, naiveSearchSize, queryPattern, topk_no);
		return result;
	}

	/**
	 * With the LSH Manager object pointer, and the search query identifier, and the R index, we can uniquely identify the search-coordinator-at-R object held
	 * by the LSH Manager at the C++ side.
	 * 
	 * TODO: this needs to be assigned to the LSH Manager in C++.
	 * @param RIndex
	 * @param searchId
	 * @param mergeResultSize: size of MergeResult Instances running in parallel
	 * @param naiveSearchSize: size of NaiveSearch Instances running in parallel
	 * @param queryPattern
	 * @param topk_no
	 * @return
	 */
	private native boolean initiateSearchCoordinatorAtR(long lshManagerPointer, int RIndex, String searchId, int mergeResultSize, int naiveSearchSize, float[] queryPattern, int topk_no);
	
	
	/**
	 * 
     * @param RIndex the index of the R that will be used to conduct the search
	 * @param searchId the search identifier associated with the time series search 
	 * @param L_range_index see the explanation below
	 * 
	 * L_range_index is the index for the range (row and high). For example, we have 3 ranges for L=30, i.e., (0,9), (10,19), (20,29). 
     * Then L_range_index will be (index=0,index=1,index=2), i.e., for index=0, the range= (0,9), for index=1, the range= (10,19), and for index=2, 
     * the range= (20,29). This L_range_index is used to find the MergeResult instance that takes care of the corresponding L range, 
     * and we will put the MergeResult instance into that SearchCoordinator’s MergedResult vector’s element that has the same specified index. 
     * identifiedParittionLow and identifiedParitionHigh, which are associated with the L table range.

	 * @param identifiedPartitionLow is associated with the L table low range that corresponds to the L_range_index. That is, with the index partition identified 
	 * (with the type of PartitionForSearchOfLTablesAtR), we can find the low range. 
	 * @param identifiedPartitionHigh is associated with the L table high range that corresponds to the L_range_index , that is, with the index partition identified
	 * (with the type of PartitionForSearchOfLTablesAtR), we can find the high range. 
	 * 
	 * @param pert_no the number of the perturbation to be performed for finding the candidates.
	 * 
	 * NOTE: what is the return number at this time, the measured time? 
	 * 
	 * @return the number for ?? 
	 */
	public int conductSearchAtRLevel(int Rindex, String searchId, int L_range_index, int identifiedPartitionLow, int identifiedPartitionHigh,
            int pert_no) {
            long lshManagerPointer =  LSHManagerProxy.INSTANCE.getPointer();
            
            return this.conductSearchAtRLevel(lshManagerPointer, Rindex, searchId, L_range_index, identifiedPartitionLow,
            		                           identifiedPartitionHigh,
            		                           pert_no);
    }
	/**
	 * With the LSH Manager object pointer, and the search query identifier, and the R index, we can uniquely identify the search-coordinator-at-R object held
	 * by the LSH Manager at the C++ side.
	 * 
	 * Note: the return value at this time is the time stamp to measure the time stamp from C++ to pass back to Java. 
	 * 
	 * @param lshManagerPointer
	 * @param Rindex
	 * @param searchId
	 * @param MergeResult_index: MergeResult index that takes care of the given range of L. It corresponds to the public methods' L_range_index parameter. 
	 * @param identifiedPartitionLow: low value of L (included)
	 * @param identifiedPartionHigh: high value of L (included)
	 * @param pert_no: # of perturbations
	 * @return 
	 */
	private native int conductSearchAtRLevel(long lshManagerPointer, int Rindex, String searchId, 
			                       int MergeResult_index, int identifiedPartitionLow, int identifiedPartionHigh, int pert_no);
	
	
	public void conductRandomBitset(String searchId, int cand_cnt) {
		
		long lshManagerPointer = LSHManagerProxy.INSTANCE.getPointer();
        conductRandomBitset(lshManagerPointer, searchId, cand_cnt);
	}
	/**
	 * With the LSH Manager object pointer, and the search query identifier, and the R index, we can set random bitsets of cand_cnt.
	 * 
	 * @param lshManagerPointer
	 * @param searchId
	 * @param cand_cnt: # of random candidates
	 * @return 
	 */
	private native void conductRandomBitset(long lshManagerPointer, String searchId, int cand_cnt);
	
	/**
	 * @param RIndex the index of the R that will be used to conduct the search
	 * @param searchId the search identifier associated with the time series search 
	 * @param mergeResultCnt see the explanation below 
	 * 
	 * The number of total MergeResult instances (i.e., the number of ranges of L) used, and that is, the number of the partitions that
	 * we allocated to conduct the candidate search on the L tables.
   
	 */
	public void mergeSearchCandidateResultAtRLevel(int Rindex, String searchId, int mergeResultCnt) {
		long lshManagerPointer =  LSHManagerProxy.INSTANCE.getPointer();
		boolean result = this.mergeSearchCandidateResultAtRLevel(lshManagerPointer, Rindex, searchId, mergeResultCnt);
	}
	
	/**
	 * With the LSH Manager object pointer, and the search query identifier, and the R index, we can uniquely identify the search-coordinator-at-R object held
	 * by the LSH Manager at the C++ side.
	 * @param lshManagerPointer
	 * @param Rindex
	 * @param searchId
	 * @param mergeResultCnt: total number of MergeResult instances to be merged
	 * @return
	 */
	private native boolean mergeSearchCandidateResultAtRLevel(long lshManagerPointer, int Rindex, String searchId,  int mergeResultCnt);
	
	
	
	public int getTotalMergedSearchCandidates (int Rindex, String searchId) {
		long lshManagerPointer =  LSHManagerProxy.INSTANCE.getPointer();
		int result = this.getTotalMergedSearchCandidates(lshManagerPointer, Rindex, searchId);
		return result; 
	}
	
	/**
	 * With the LSH Manager object pointer, and the search query identifier, and the R index, we can uniquely identify the search-coordinator-at-R object held
	 * by the LSH Manager at the C++ side.
	 * @param lshManagerPointer
	 * @param Rindex
	 * @param searchId
	 * @return
	 */
	private native int getTotalMergedSearchCandidates(long lshManagerPointer, int Rindex, String searchId);
	
	
	/**
	 * @param Rindex
	 * @param searchId
	 * @param identifiedPartitionLow
	 * @param identifiedPartitionHigh
	 * @param index
	 */
	public void conductNaiveSearchOnMergedCandidatesAtRLevel (int Rindex, String searchId, 
			int identifiedPartitionLow, int identifiedPartitionHigh,
            int index) {
		long lshManagerPointer =  LSHManagerProxy.INSTANCE.getPointer();
		//NOTE: in the previous version, the index is positioned incorrectly to be at the last parameter passing. 
		this.conductNaiveSearchOnMergedCandidatesAtRLevel (lshManagerPointer, Rindex, searchId, 
				index,
				identifiedPartitionLow, 
				identifiedPartitionHigh
				); 
	}
	
	/**
	 * With the LSH Manager object pointer, and the search query identifier, and the R index, we can uniquely identify the search-coordinator-at-R object held
	 * by the LSH Manager at the C++ side.
	 * @param lshManagerPointer
	 * @param Rindex
	 * @param searchId
	 * @param NaiveSearch_index: NaiveSearch index that takes care of the given range of candidates. 
	 * The partition is represented by PartitionForMergedResultsFromLTables
	 * @param identifiedPartitionLow: low number of candidates for NaiveSearch (included)
	 * @param identifiedPartitionHigh: high number of candidates for NaiveSearch (included)
	 * @return
	 */
	private native boolean conductNaiveSearchOnMergedCandidatesAtRLevel (long lshManagerPointer, int Rindex, String searchId, 
			int NaiveSearch_index, int identifiedPartitionLow, int identifiedPartitionHigh); 
	
	
	// return value added - number of candidates whose distance is within the threshold (R^2*1.5)
	public int mergeNaiveSearchResultAtRLevel(int Rindex, String searchId, int naiveSearchCnt) {
		long lshManagerPointer =  LSHManagerProxy.INSTANCE.getPointer();
		return this. mergeNaiveSearchReseultAtRLevel(lshManagerPointer, Rindex, searchId, naiveSearchCnt);
	}
	
	/**
	 * With the LSH Manager object pointer, and the search query identifier, and the R index, we can uniquely identify the search-coordinator-at-R object held
	 * by the LSH Manager at the C++ side.
	 * @param lshManager
	 * @param Rindex
	 * @param searchId
	 * @param naiveSearchCnt: total number of NaiveSearch instances to be merged. This is the number of the total partitions (or worker threads) assigned 
	 * to do the naive search on the reduced candidate set. 
	 * 
	 */
	private native int mergeNaiveSearchReseultAtRLevel(long lshManager, int Rindex, String searchId, int naiveSearchCnt);
	
	/**
	 * NOTE: get the search result statistics, e.g., # of candidates < R, # of candidates < R*1.5, etc.
	 * the C++ side. 
	 * 
	 * @param Rindex
	 * @param searchId
	 * @return numbers of candidates < R^2 and 1.5*R^2, etc.
	 */
	public int[] getSearchResultStatistics(int Rindex, String searchId) {
		long lshManagerPointer =  LSHManagerProxy.INSTANCE.getPointer();
		
		return this.getSearchResultStatistics (lshManagerPointer, Rindex, searchId);
	}
	
	/**
	 * With the LSH Manager object pointer, and the search query identifier, and the R index, we can uniquely identify the search-coordinator-at-R object held
	 * by the LSH Manager at the C++ side.
	 * @param lshManager
	 * @param Rindex
	 * @param searchId
	 * @return numbers of candidates < R^2 and 1.5*R^2, etc.
	 */
	private native int[] getSearchResultStatistics(long lshManager, int Rindex, String searchId);
	
	/**
	 * NOTE: at this time, the Java side will have to invoke this function multiple times, in order to get the Top-K results back from
	 * the C++ side. 
	 * 
	 * @param Rindex
	 * @param searchId
	 * @return
	 */
	public TimeSeriesSearch.SearchResult getSearchResultAtRLevel(int Rindex, String searchId) {
		long lshManagerPointer =  LSHManagerProxy.INSTANCE.getPointer();
		//create a holder to be filled in later, rather than having the JNI to create the object. 
		TimeSeriesSearch.SearchResult result= new TimeSeriesSearch.SearchResult();
		
		boolean bres = this.getSearchResultAtRLevel (lshManagerPointer, Rindex, searchId, result);
		if(bres)
			return result;
		else
			return null;
	}
	
	/**
	 * With the LSH Manager object pointer, and the search query identifier, and the R index, we can uniquely identify the search-coordinator-at-R object held
	 * by the LSH Manager at the C++ side.
	 * @param lshManager
	 * @param Rindex
	 * @param searchId
	 * @param result
	 */
	private native boolean getSearchResultAtRLevel(long lshManager, int Rindex, String searchId, TimeSeriesSearch.SearchResult result);
	
	
	public boolean shutdownSearchAtRLevel(int Rindex, String searchId) {
		long lshManagerPointer =  LSHManagerProxy.INSTANCE.getPointer();
		boolean result = this.shutdownSearchAtRLevel(lshManagerPointer, Rindex, searchId);
		return result;
	}
	 
	/**
	 * With the LSH Manager object pointer, and the search query identifier, and the R index, we can uniquely identify the search-coordinator-at-R object held
	 * by the LSH Manager at the C++ side.
	 * clean-up the candidates of bitset
	 * @param lshManager
	 * @param Rindex
	 * @param searchId
	 * @return
	 */
	private native boolean shutdownSearchAtRLevel (long lshManager, int Rindex, String searchId);
	
	public boolean removeSearchCoordinator(String searchId) {
		long lshManagerPointer =  LSHManagerProxy.INSTANCE.getPointer();
		boolean result = this.removeSearchCoordinator(lshManagerPointer, searchId);
		return result;
	}
	
	/**
	 * remove SearchCoordinator instance
	 * @param lshManager
	 * @param searchId
	 * @return
	 */
	private native boolean removeSearchCoordinator (long lshManager, String searchId);
	
	
	// for testing Naive Search
	public void testNaiveSearch(int Rindex, String searchId) {
		long lshManagerPointer =  LSHManagerProxy.INSTANCE.getPointer();
		this.testNaiveSearch(lshManagerPointer, Rindex, searchId);
	}
	
	private native void testNaiveSearch (long lshManager, int Rindex, String searchId);
}
