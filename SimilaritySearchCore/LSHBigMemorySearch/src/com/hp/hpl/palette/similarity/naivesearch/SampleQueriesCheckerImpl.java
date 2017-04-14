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

import java.util.ArrayList;
import java.util.Date;
import java.util.PriorityQueue;
import java.io.DataOutputStream;
import java.io.FileOutputStream;
import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.hp.hpl.palette.similarity.bridge.SearchCoordinatorAtRProxy;
import com.hp.hpl.palette.similarity.datamodel.TimeSeriesSearch;
import com.hp.hpl.palette.similarity.worker.IndexBuildingChecker;

public class SampleQueriesCheckerImpl implements IndexBuildingChecker {
    
	private int partitionId;
	private int topK_no;
	private ArrayList<ArrayList<Float>> querylist;
	
	private static final Log LOG = LogFactory.getLog(SampleQueriesCheckerImpl.class.getName());
	
	public SampleQueriesCheckerImpl (int partitionId, int topK_no, ArrayList<ArrayList<Float>> querylist){
		this.partitionId=partitionId;
		this.topK_no=topK_no;
		this.querylist = querylist;
	}
	
	
	@Override
	public void checkWithSampleQueries() {
		LOG.info("start checking with sample queries for Naive Search.");
		try {
		   this.conductSearchOnSampleQueries();
		}
		catch (Exception ex) {
			LOG.error("fails to conduct sampe queries checking for Naive Search.", ex);
		}
		LOG.info("end checking with sample queries for Naive Search.");
	}

	protected void conductSearchOnSampleQueries() throws IOException {
       
		//to record the naive search file output
		DataOutputStream fileNaiveResults = new DataOutputStream(new FileOutputStream("naive_resultfile"));
		DataOutputStream theKthResults = new DataOutputStream(new FileOutputStream("theKthResults.txt")); 
		
		PriorityQueue<TimeSeriesSearch.SearchResult> topK = new PriorityQueue<TimeSeriesSearch.SearchResult>();
		
		String searchId = "naive_search";
		int Rindex = 0; // for general interface for both LSH and Naive Search 
		SearchCoordinatorAtRProxy searchCoordinatorAtRProxy = new SearchCoordinatorAtRProxy();
		
		for(int q1=0;q1<querylist.size();q1++) {
	  		ArrayList<Float> query = querylist.get(q1);
		    float[] queryPattern = new float[query.size()];
			for(int l=0;l<query.size();l++) {
				queryPattern[l] = query.get(l);
			}
			searchCoordinatorAtRProxy.initiateSearchCoordinatorAtR (0, searchId, 1, 1, queryPattern, this.topK_no);
			long start1 = new Date().getTime();
			searchCoordinatorAtRProxy.testNaiveSearch(0, searchId);
		
			while(true) {
				TimeSeriesSearch.SearchResult result = searchCoordinatorAtRProxy.getSearchResultAtRLevel(0, searchId);
				
				if(result == null)
					break;

				TimeSeriesSearch.SearchResult mt;
				if(topK.size() < topK_no) {
	    			topK.add(result);
	    		} else {
					mt = topK.peek();
		    		if(mt.distance > result.distance) {
			    		topK.poll();
					 	topK.add(result);
			    	}
	    		}
	    	}
			long end1 = new Date().getTime();
			int n = topK.size();
			StringBuilder builder = new StringBuilder();
		 
			builder.append("\n<<top-" + n + " result>>\n");
			for(int i=0;i<n;i++) {
				TimeSeriesSearch.SearchResult ms = topK.poll();
				//builder.append(ms.id + " " + ms.offset + " " + ms.distance + "\n");
				builder.append("\n"+ q1+" "+ms.id + " " + ms.offset + " " + ms.distance );
				ms.write(fileNaiveResults);
				//the 5-th one is located actually at the first place.
				if (i==0) {
				   String output=  this.partitionId + " " + q1 + " " + ms.id + " " + ms.offset + " " + ms.distance + "\n";
				   theKthResults.writeBytes(output); //the k-th result.
				}
			}
			
			LOG.info(builder.toString());
			
			LOG.info("Naive search done time: " + (end1-start1) + " milliseconds");
		}
		
		
		fileNaiveResults.flush();
		fileNaiveResults.close();
		
		theKthResults.flush();
		theKthResults.close();
	}
}
