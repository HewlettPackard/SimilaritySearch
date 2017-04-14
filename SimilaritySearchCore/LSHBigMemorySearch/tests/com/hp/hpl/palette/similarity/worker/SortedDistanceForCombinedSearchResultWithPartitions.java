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


import java.util.ArrayList;
import java.util.Arrays;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.junit.Assert;
import org.junit.Test;

import com.hp.hpl.palette.similarity.coordinator.CoordinatorMasterImpl;
import com.hp.hpl.palette.similarity.datamodel.TimeSeriesSearch;

import junit.framework.TestCase;
 
/**
 * The sorted result is based on the ascending of the distance, which is what we want. 
 *
 */
public class SortedDistanceForCombinedSearchResultWithPartitions extends TestCase {

	 private static final Log LOG = LogFactory.getLog(SortedDistanceForCombinedSearchResultWithPartitions.class.getName());
	 
	 private  CoordinatorMasterImpl.CombinedSearchResultWithPartitionAndR collectedIntermediateResults[]; 
	 
	 @Override
	 protected void setUp() throws Exception{ 
		 
		 this.collectedIntermediateResults  = new CoordinatorMasterImpl.CombinedSearchResultWithPartitionAndR [4];
		 int count=0;
		 {
			 int Rvalue = 1;
			 TimeSeriesSearch.SearchResult result = new  TimeSeriesSearch.SearchResult();
			 result.id=1;
			 result.offset=1;
			 result.distance=(float)100.0;
			 int partitionId = 4; 
		     CoordinatorMasterImpl.CombinedSearchResultWithPartitionAndR p = 
				 new  CoordinatorMasterImpl.CombinedSearchResultWithPartitionAndR (Rvalue, result, partitionId);
		     this.collectedIntermediateResults[count]= p; 
		     count++;
		 }
		 
		 {
			 int Rvalue = 1;
			 TimeSeriesSearch.SearchResult result = new  TimeSeriesSearch.SearchResult();
			 result.id=1;
			 result.offset=4;
			 result.distance=(float)800.0;
			 int partitionId = 4; 
		     CoordinatorMasterImpl.CombinedSearchResultWithPartitionAndR p = 
				 new  CoordinatorMasterImpl.CombinedSearchResultWithPartitionAndR (Rvalue, result, partitionId);
		     this.collectedIntermediateResults[count]= p; 
		     count++;
		 }
		 
		 {
			 int Rvalue = 1;
			 TimeSeriesSearch.SearchResult result = new  TimeSeriesSearch.SearchResult();
			 result.id=4;
			 result.offset=5;
			 result.distance=(float)70.0;
			 int partitionId = 12; 
		     CoordinatorMasterImpl.CombinedSearchResultWithPartitionAndR p = 
				 new  CoordinatorMasterImpl.CombinedSearchResultWithPartitionAndR (Rvalue, result, partitionId);
		     this.collectedIntermediateResults[count]= p; 
		     count++;
		 }
		 
		 {
			 int Rvalue = 1;
			 TimeSeriesSearch.SearchResult result = new  TimeSeriesSearch.SearchResult();
			 result.id=19;
			 result.offset=119;
			 result.distance=(float)56.0;
			 int partitionId = 120; 
		     CoordinatorMasterImpl.CombinedSearchResultWithPartitionAndR p = 
				 new  CoordinatorMasterImpl.CombinedSearchResultWithPartitionAndR (Rvalue, result, partitionId);
		     this.collectedIntermediateResults[count]= p; 
		     count++;
		 }
		 
		 super.setUp();
		 
	 }
	 
	 
	 /**
	  * The actual effect is that: with initial capacity, the actual size after array list construction is 0. 
	  */
	 @Test
	 public void testSortingForDistance () {
		 Arrays.sort(collectedIntermediateResults, new  CoordinatorMasterImpl.CombinedSearchResultWithPartitionAndR());
		 for (int i=0; i<collectedIntermediateResults.length; i++) {
			 CoordinatorMasterImpl.CombinedSearchResultWithPartitionAndR sortedResult = collectedIntermediateResults[i];
			 LOG.info("distance: " + sortedResult.result.distance);
		 }
	 }
	 
	 
	 @Override
	 protected void tearDown() throws Exception{ 
		 //do something first;
		 super.tearDown();
	 }
	 
	 public static void main(String[] args) {
		  
	      junit.textui.TestRunner.run(SortedDistanceForCombinedSearchResultWithPartitions.class);
	 }
}


