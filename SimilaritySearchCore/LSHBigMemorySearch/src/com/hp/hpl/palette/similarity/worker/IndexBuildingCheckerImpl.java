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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashSet;
import java.util.Iterator;
import java.util.PriorityQueue;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import lsh.LSHinf;
import lshbased_lshstore.LSH_Hash;

import com.hp.hpl.palette.similarity.bridge.SearchCoordinatorAtRProxy;
import com.hp.hpl.palette.similarity.configuration.SimilaritySearchConfiguration;
import com.hp.hpl.palette.similarity.datamodel.TimeSeriesSearch;

public class IndexBuildingCheckerImpl  implements IndexBuildingChecker {
    
	private static final Log LOG = LogFactory.getLog(IndexBuildingCheckerImpl.class.getName());
	
	private int R_num;
	private int L_num;
	private int pert_num;
	private int topK_no;
	private LSH_Hash lsh_Hash;
	private ArrayList<ArrayList<Float>> querylist;
	
	public IndexBuildingCheckerImpl (int R_num, int L_num, int pert_num, int topK_no, LSH_Hash lsh_Hash,  
			                                 ArrayList<ArrayList<Float>> querylist) {
		this.R_num = R_num;
		this.L_num = L_num;
		this.pert_num = pert_num;
		this.topK_no = topK_no;
		this.lsh_Hash = lsh_Hash;
		this.querylist = querylist;
	}
	
	
	@Override
	public void checkWithSampleQueries() {
		LOG.info("start index checking with sample queries.");
		try {
		    this.LSH_search(this.R_num, this.L_num, this.pert_num, this.topK_no, this.lsh_Hash); 
		}
		catch (Exception ex) {
			LOG.error("fails to conduct sampe queries verification.", ex);
		}
		LOG.info("end index checking with sample queries");
	}
	
	protected void LSH_search(int R_num, int L_num, int pert_num, int topK_no, LSH_Hash lsh_Hash) throws IOException {
		SearchCoordinatorAtRProxy searchCoordinatorAtRProxy = new SearchCoordinatorAtRProxy();
		PriorityQueue<TimeSeriesSearch.SearchResult> topK = new PriorityQueue<TimeSeriesSearch.SearchResult>();
		Set<TimeSeriesSearch.SearchResult> totalcandidateSet = new HashSet<TimeSeriesSearch.SearchResult>();
		
		ArrayList<TimeSeriesSearch.SearchResult> arrResults = new ArrayList<TimeSeriesSearch.SearchResult>();
		// for each number of L
		
			
		String searchId = "search";
		int initial_part_no = 1; //For now, one task for MergeResult and NaiveSearch respectively
	//	int range_L = 10;
	//	int range_L_cnt = L_num/range_L;
		int nTotalTime = 0;
		output("# of L: " + L_num+ "**************");
		
		output("\n" + "perturbations =" + pert_num + "**************");
		for(int q1=0;q1<querylist.size();q1++) {
			output("\n" + "start query=" + q1 + "==============================");
			output("# of L: " + L_num);
			output(" # of perturbations: " + pert_num);
			
			//LOG.info("\n");
			
			ArrayList<Float> query = querylist.get(q1);
		    float[] queryPattern = new float[query.size()];
			for(int q=0;q<query.size();q++) {
				queryPattern[q] = query.get(q);
			}
			
		
			long ret_cand_time = 0;
			int comp_hindex_time = 0;
			long merge_cand_time = 0;
			long comp_topk_time = 0;
			long merge_topk_time = 0;
			
			int comp_hindex_time_last = 0;
			long merge_cand_time_last = 0;
			long comp_topk_time_last = 0;
			int  totalRs=0;
			int  totalSkippedRs=0;
			
			// search starts
			totalcandidateSet.clear();
			topK.clear();
			
			searchCoordinatorAtRProxy.initiateSearchCoordinatorAtR (0, searchId, initial_part_no, initial_part_no, queryPattern, topK_no);
			long start = new Date().getTime();
			
			for(int i=0;i<R_num;i++) {
																				
				LSHinf lshinf = lsh_Hash.rarray.get(i);
							
				// retrieve candidate
				long start1 = new Date().getTime();
				
				comp_hindex_time_last = searchCoordinatorAtRProxy.conductSearchAtRLevel(i, searchId, 0, 0, L_num-1, pert_num); 
				comp_hindex_time += comp_hindex_time_last;
				long end1 = new Date().getTime();
				ret_cand_time += (end1-start1);
				
				// merge candidate
				start1 = new Date().getTime();
				searchCoordinatorAtRProxy.mergeSearchCandidateResultAtRLevel(i, searchId, 1);  // to merge different L tables	
				int total_cand_num = searchCoordinatorAtRProxy.getTotalMergedSearchCandidates(i, searchId);
				end1 = new Date().getTime();
				merge_cand_time_last = (end1-start1);
				merge_cand_time 	+= merge_cand_time_last;
				
				totalRs++;
				if(i < R_num-1) {
					//if the total number of candidates for each R is less than 3000, 
					//then merge the candidates of the next R without computing the top-k.
					if(total_cand_num < SimilaritySearchConfiguration.CANDIDATE_NUMBER_THRESHOLD){
						
						output("R SKIPPED## R_index: " + i +" candidates:" + total_cand_num + " QUERYID=" + q1);
						output("statistics: "+L_num+" "+ pert_num+" " + i+ " "+ lshinf.parameterR2+" "+ "0" +" "+"0" + " " + total_cand_num +" "+ comp_hindex_time_last/1000+" "+merge_cand_time_last/1000 + " "+"0" +" "+q1);
						
						totalSkippedRs++;
						continue;
					}
				}
				
				
				// naive search
				start1 = new Date().getTime();
				searchCoordinatorAtRProxy.conductNaiveSearchOnMergedCandidatesAtRLevel (i, searchId, 
						0, total_cand_num-1, 0);
				end1 = new Date().getTime();
				//int n_cand = searchCoordinatorAtRProxy.mergeNaiveSearchResultAtRLevel(i, searchId, 1); // Mijung (4/23/14): the function is not used any more
				int[] n_cand = searchCoordinatorAtRProxy.getSearchResultStatistics(i, searchId); // Added by Mijung(4/23/14): get statistics for search results						
				comp_topk_time_last = (end1-start1);
				comp_topk_time +=comp_topk_time_last;
				
				while(true) {
					TimeSeriesSearch.SearchResult result = searchCoordinatorAtRProxy.getSearchResultAtRLevel(i, searchId);
					if(result == null)
						break;
					totalcandidateSet.add(result);
				}
				searchCoordinatorAtRProxy.shutdownSearchAtRLevel(i, searchId);
				output("R_index: " + i);
				output("R^2: " + lshinf.parameterR2);
				output("#cand < R^2: " + n_cand[0] + " QUERYID=" + q1);
				output("#cand < 1.5*R^2: " + n_cand[1]);
				output("total candidate number: " + total_cand_num);
				output("h1h2 Time " + comp_hindex_time_last);
				output("MergeTime " + merge_cand_time_last);
				
				
				output("statistics: " +L_num+" "+ pert_num+" " + i+ " "+ lshinf.parameterR2+" "+ n_cand[0]+" "+n_cand[1] + " " + total_cand_num +" "+ comp_hindex_time_last/1000+" "+merge_cand_time_last/1000 + " "+comp_topk_time_last/1000 +" "+q1);
				// to do stopping condition
				// commented by Mijung because n_cand may have candidates with a partial distance
				//if(n_cand[0] >= 10)
				// break;
				if(totalcandidateSet.size() >= topK_no)
					break;
			} // for R
			// generate top-k
			long start1 = new Date().getTime();
			Iterator<TimeSeriesSearch.SearchResult> iterator = totalcandidateSet.iterator(); 
			TimeSeriesSearch.SearchResult mt;
			while (iterator.hasNext()){
				TimeSeriesSearch.SearchResult result = iterator.next();
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
			merge_topk_time += (end1-start1);

			long end = new Date().getTime();
			
			int n = topK.size();
			
			StringBuilder strbuilder = new StringBuilder();
			strbuilder.append("\n<<LSH top-" + n + " result>>\n");
			
			
			for(int i=0;i<n;i++) {
				TimeSeriesSearch.SearchResult ms = topK.poll();
				strbuilder.append(L_num+" "+ pert_num+" " +i+" "+ q1+" " + ms.id + " " + ms.offset + " " + ms.distance + "\n");
			
			}
			
			output(strbuilder.toString());
			output("computing h1 and h2 index time (ms): " + (comp_hindex_time/1000));
			output("retrieve candidates time (ms): " + (ret_cand_time-comp_hindex_time/1000));
			output("merge candidates time (ms): " + merge_cand_time);
			output("compute top-k time (ms): " + comp_topk_time);
			output("merge top-k time (ms): " + merge_topk_time);
			nTotalTime += (end-start);
			output("total search time (ms): " + (end-start));
			output("total Rs:: " + (totalRs) +" QUERYID=" + q1);
			output("skipped Rs:: " + (totalSkippedRs) +" QUERYID=" + q1 );
			output("times: "+L_num+" "+ pert_num+" " + (comp_hindex_time/1000)+" "+  (ret_cand_time-comp_hindex_time/1000) +" "+merge_cand_time+" "+comp_topk_time +" "+merge_topk_time+" "+(end-start)+" " +" "+ (totalSkippedRs+1)+" "+ (totalRs+1) +" "+q1 +" " + n);
			
			
			searchCoordinatorAtRProxy.removeSearchCoordinator(searchId);
		}// for query
		
		output("\n============================================");
		output("# of L: " + L_num+ "**************");	
		output("# of p: " + pert_num+ "**************");	
		float avgtime = (float)nTotalTime/(float)querylist.size();
		output("average time (ms): " + avgtime);
	}
	
	protected void output(String output) {
		LOG.info(output);
		//System.out.println(output);
	}


}
