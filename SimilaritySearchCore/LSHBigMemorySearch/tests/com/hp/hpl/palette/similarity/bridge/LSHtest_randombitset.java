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

import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.PriorityQueue;
import java.util.Random;
import java.util.Set;
import java.util.StringTokenizer;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Writable;

import lsh.LSHinf;
import lshbased_lshstore.LSH_Hash;

import com.hp.hpl.palette.similarity.bridge.HashIndexProxy;
import com.hp.hpl.palette.similarity.bridge.LSHManagerProxy;
import com.hp.hpl.palette.similarity.bridge.IndexBuilderProxy;
import com.hp.hpl.palette.similarity.bridge.SearchCoordinatorAtRProxy;
import com.hp.hpl.palette.similarity.bridge.TimeSeriesBuilderProxy;
import com.hp.hpl.palette.similarity.datamodel.TimeSeriesSearch;
import com.hp.hpl.palette.similarity.worker.IndexBuilder;
import com.hp.hpl.palette.similarity.worker.IndexBuilderImpl;
import com.hp.hpl.palette.similarity.worker.LSHIndexConfigurationParameters;
import com.hp.hpl.palette.similarity.worker.SearchCoordinator;
import com.hp.hpl.palette.similarity.worker.IndexBuilder.PartitionOfLTablesAtRForIndexBuilding;
import com.hp.hpl.palette.similarity.worker.IndexBuilder.PartitionOfLTablesAtRForIndexBuilding;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import common.MaxHeap;
import common.Measurements;
import common.Utils;
import common.TimeSeries;

public class LSHtest_randombitset {

	/**
	 * @param args
	 * @throws IOException 
	 */
	private static final Log LOG = LogFactory
			.getLog(LSHtest_randombitset.class.getName());
	
	void testLSHManager(String datafile, String lshfile, String queryfile, String outfile, int R_num, int L_num, int topK_no, int start_L, int end_L, int inc_L, int start_p, int end_p, int inc_p, int numRandomCand, int file_idx) throws IOException {
		// create LSHManager
		LSHManagerProxy managerInstance = LSHManagerProxy.INSTANCE;
		managerInstance.start(R_num, L_num, 700000);
	
		LOG.info("LSHManager instace started");
		
		
		File file = new File("naive_resultfile");
		if(!file.exists()) {
			System.out.println("naive_resultfile not exist");
			return;
		}
		DataInputStream fileLSH = new DataInputStream(new FileInputStream(lshfile));
		DataInputStream fileQuery = new DataInputStream(new FileInputStream(queryfile));
		
		
		//PrintStream output = new PrintStream(new BufferedOutputStream(new FileOutputStream(outfile))); //search_result
		
		// Read query list from fileQuery
		ArrayList<ArrayList<Float>> querylist = new ArrayList<ArrayList<Float>>();
		BufferedReader reader = new BufferedReader(new InputStreamReader(fileQuery));
	    while(true) {
			String qline = reader.readLine();
			if(qline == null)
				break;
			ArrayList<Float> query = new ArrayList<Float>();
			StringTokenizer qitr = new StringTokenizer(qline);
			while(qitr.hasMoreTokens()) {
				String t = qitr.nextToken();
				query.add(Float.parseFloat(t));
			}
			querylist.add(query);
		}
		reader.close();
		
		int querylength = querylist.get(0).size();
		
		// create TimeSeriesBuilder to populate time series data
		IndexBuilderProxy indexBuilderProxy = new IndexBuilderProxy();
		TimeSeries data = new TimeSeries(); 
		TimeSeriesBuilderProxy tsBuilder = new TimeSeriesBuilderProxy();
		LOG.info("Starting to read TimeSeries");
		int idx=0;
		if(file_idx == 0) {
			DataInputStream fileTS = new DataInputStream(new FileInputStream(datafile));
			
			while (fileTS.available() != 0) {
				data.readFields(fileTS); // copied from HDFS
				int key = (int)data.id.get();
		
				int l = data.vals.get().length;
				if(l < querylength) {
					LOG.warn("timeseries size is less than query size, id=[" + key + "], ts size=[" + l + "]");
					continue;
				}
				DoubleWritable[] arr = Utils.getDoubleArray(data.vals);
			  	float[] vals =  new float[l];
			  	for(int i=0;i<l;i++) {
			  		vals[i] = (float)arr[i].get();
			  	}
				tsBuilder.buildTimeSeries(managerInstance, key, vals);
			  	//tsBuilder.buildTimeSeries(managerInstance, idx, vals);
				idx++;
			}
		} else {
			for(int p=0;p<file_idx;p++) {
				DataInputStream fileTS = new DataInputStream(new FileInputStream(datafile + "_" + (p+1)));
					
				while (fileTS.available() != 0) {
					data.readFields(fileTS); // copied from HDFS
					int key = (int)data.id.get();
			
					int l = data.vals.get().length;
					if(l < querylist.get(0).size()) {
						LOG.warn("timeseries size is less than query size, id=[" + key + "], ts size=[" + l + "]");
						continue;
					}
					DoubleWritable[] arr = Utils.getDoubleArray(data.vals);
				  	float[] vals =  new float[l];
				  	for(int i=0;i<l;i++) {
				  		vals[i] = (float)arr[i].get();
				  	}
					tsBuilder.buildTimeSeries(managerInstance, key, vals);
					idx++;
				}
			}
		}
		LOG.info("TimeSeries reading done cnt=" + idx);
		
		// load the configuration file
		// create hash functions
		
		LSH_Hash lsh_hash = new LSH_Hash();
		lsh_hash.readFields(fileLSH);
		for(int i=0;i<R_num;i++) {
			LSHinf lshinf = lsh_hash.rarray.get(i);
			managerInstance.setLSH_HashFunction(i, lshinf.pLSH, lshinf.pHash); 
		}
		/*
		if(use_serial == 0) {
			for(int i=0;i<R_num;i++) {
				LOG.info("Starting to singlethreaded buildIndex (" + i + ")");
				long start = new Date().getTime();
						
				indexBuilderProxy.buildIndex(i, 0, L_num-1, querylist.get(0).size());
				long end = new Date().getTime();
				LOG.info("singlethreaded buildIndex done time (" + i + "): " + (end-start) + " milliseconds");
				
			}
			// serialize
			//indexBuilderProxy.serialize(managerInstance);
		} else {
			
			// deserialize
			//indexBuilderProxy.deserialize();
		
		
			LOG.info("Starting to multithreaded buildIndex");
			long start = new Date().getTime();
			LSHIndexConfigurationParameters  otherParameters = new  LSHIndexConfigurationParameters ();
		    otherParameters.setRNumber(R_num);
		    otherParameters.setLNumber(L_num);
		    otherParameters.setQueryLength(querylength);
		    otherParameters.setNumberofWorkerThreadsForIndexing(numThreads);
		    otherParameters.setNumberOfLTablesInAIndexingGroup(numLforOneThread); //we will do it in one single threads.
		    
			//from Jun's code for the multi-threaded index building. 
			IndexBuilder builder = new IndexBuilderImpl(otherParameters);
			List<PartitionOfLTablesAtRForIndexBuilding>  partitions = builder.getRangePartitionForIndexBuilding();
			LOG.info("the partition for indexing is the following: ");
			LOG.info("total number of the paritions is: " + partitions.size());
			int counter=0;
			for (PartitionOfLTablesAtRForIndexBuilding p: partitions) {
				LOG.info( counter + " -th parition has  R value: " + p.getRValue()
						+ " and low range: " + p.getRangeLowForLTables() + " and high range: " + p.getRangeHighForLTables());
			}
			
			builder.startIndexBuilding(partitions);
			LOG.info("we now finish the entire index building for the data parition");
			long end = new Date().getTime();
			LOG.info("multithreaded buildIndex done time " + (end-start) + " milliseconds");
		}
		if(is_post_processing == 1) {
			// for memory checking
			//LOG.info("Run the top command.");
			//System.in.read(); // this is for top
			LOG.info("Starting to postProcessing.");
			HashIndexProxy hashIndexProxy = new HashIndexProxy();
			long totalmem = 0;
			for(int i=0;i<R_num;i++) {
				for(int j=0;j<L_num;j++) {
					hashIndexProxy.postProcessing(managerInstance, i, j);
					int mem = hashIndexProxy.getMemoryOccupied(managerInstance, i, j);
					totalmem += mem;
					//LOG.info(i + "," + j + "=" + mem);
				}
			}
			LOG.info("postProcessing done. total mem=" + totalmem);
			//System.in.read();
		}
		*/
				
		SearchCoordinatorAtRProxy searchCoordinatorAtRProxy = new SearchCoordinatorAtRProxy();
		PriorityQueue<TimeSeriesSearch.SearchResult> topK = new PriorityQueue<TimeSeriesSearch.SearchResult>();
		Set<TimeSeriesSearch.SearchResult> totalcandidateSet = new HashSet<TimeSeriesSearch.SearchResult>();
		
		
		
		ArrayList<TimeSeriesSearch.SearchResult> arrResults = new ArrayList<TimeSeriesSearch.SearchResult>();
		
		//for(int m=0;m<1000;m++) {
			
		// for each number of L
		for(int l=start_L;l<=end_L;l+=inc_L) {
			
			String searchId = "search";
			int initial_part_no = 1; //For now, one task for MergeResult and NaiveSearch respectively
		//	int range_L = 10;
		//	int range_L_cnt = L_num/range_L;
			int nTotalCorrect = 0;
			int nTotalTime = 0;
			int nTotalQueries = 0;
			output("# of L: " + l+ "**************");
			
			// for each number of pert
			for(int p=start_p;p<=end_p;p+=inc_p) {
				
				output("\n" + "perturbations =" + p + "**************");
				DataInputStream fileNaiveResults = new DataInputStream(new FileInputStream("naive_resultfile"));
				
				for(int q1=0;q1<querylist.size();q1++) {
					output("\n" + "start query=" + q1 + "==============================");
					output("# of L: " + l);
					output(" # of perturbations: " + p);
					
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
																						
						LSHinf lshinf = lsh_hash.rarray.get(i);
									
						// retrieve candidate
						searchCoordinatorAtRProxy.conductRandomBitset(searchId, numRandomCand); 
						
						// merge candidate
						long start1 = new Date().getTime();
						searchCoordinatorAtRProxy.mergeSearchCandidateResultAtRLevel(i, searchId, 1);  // to merge different L tables	
						int total_cand_num = searchCoordinatorAtRProxy.getTotalMergedSearchCandidates(i, searchId);
						long end1 = new Date().getTime();
						merge_cand_time_last = (end1-start1);
						merge_cand_time 	+= merge_cand_time_last;
						
						totalRs++;
						if(i < R_num-1) {
							//if the total number of candidates for each R is less than 3000, 
							//then merge the candidates of the next R without computing the top-k.
							if(total_cand_num < 3000){
								
								output("R SKIPPED## R_index: " + i +" candidates:" + total_cand_num);
								output("statistics: "+l+" "+ p+" " + i+ " "+ lshinf.parameterR2+" "+ "0" +" "+"0" + " " + total_cand_num +" "+ comp_hindex_time_last/1000+" "+merge_cand_time_last/1000 + " "+"0" +" "+q1);
								
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
						output("#cand < R^2: " + n_cand[0]);
						output("#cand < 1.5*R^2: " + n_cand[1]);
						output("total candidate number: " + total_cand_num);
						output("h1h2 Time " + comp_hindex_time_last);
						output("MergeTime " + merge_cand_time_last);
						
						
						output("statistics: " +l+" "+ p+" " + i+ " "+ lshinf.parameterR2+" "+ n_cand[0]+" "+n_cand[1] + " " + total_cand_num +" "+ comp_hindex_time_last/1000+" "+merge_cand_time_last/1000 + " "+comp_topk_time_last/1000 +" "+q1);
						// to do stopping condition
						if(n_cand[0] >= 10)
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
					
					arrResults.clear();
					// read Naive results
					output("\n<<naive result>>");
					for(int i=0;i<topK_no;i++) {
						TimeSeriesSearch.SearchResult ms = new TimeSeriesSearch.SearchResult();
						ms.readFields(fileNaiveResults);
						output(ms.id + "-" + ms.offset + "-" + ms.distance);
						arrResults.add(ms);
					}
					
					StringBuilder strbuilder = new StringBuilder();
					strbuilder.append("\n<<LSH top-" + n + " result>>\n");
					
					int nCorrect = 0;
					for(int i=0;i<n;i++) {
						TimeSeriesSearch.SearchResult ms = topK.poll();
						boolean match = false;
						for(int j=0;j<topK_no;j++) {
							if(arrResults.get(j).equals(ms)) {
								match = true;
								break;
							}
						}
						if(match == true) {
							strbuilder.append("correct found: "+l+" "+ p+" " +i+" "+ q1+" " + ms.id + " " + ms.offset + " " + ms.distance + "\n");
							nCorrect++;
						}
						else {
							strbuilder.append("wrong found: "+l+" "+ p+" " +i+" "+ q1+" " + ms.id + " " + ms.offset + " " + ms.distance + "\n");
						}
					}
					nTotalCorrect += nCorrect;
					if(nCorrect == topK_no) {
						nTotalQueries++;
					}
					output(strbuilder.toString());
					output("compute top-k time (ms): " + comp_topk_time);
					nTotalTime += comp_topk_time; //(end-start);
					output("total search time (ms): " + (end-start));
					output("correctness: " + nCorrect + "/" + topK_no);
					output("total Rs:: " + (totalRs+1) );
					output("skipped Rs:: " + (totalSkippedRs+1) );
					
					searchCoordinatorAtRProxy.removeSearchCoordinator(searchId);
				}// for query
				
				output("\n============================================");
				output("# of all correct queries: " + nTotalQueries + " out of " + querylist.size());
				output("# of L: " + l+ "**************");	
				output("# of p: " + l+ "**************");	
				output(nTotalCorrect + "," + nTotalTime);
				float correctness =  (float)nTotalCorrect/(float)(topK_no*querylist.size());
				float avgtime = (float)nTotalTime/(float)querylist.size();
				output("total correctness: " + correctness);
				output("average time (ms): " + avgtime);
			}//for #pert
						
		}//for #L
		
		//}
		
		
		//while(true) {
			
		//}
	}
	
	public void output(String output) {
		//LOG.info(output);
		System.out.println(output);
	}
	
	// to do : first clean up the code and make compilable and check it out

	public static void main(String[] args) throws NumberFormatException, IOException {
		if(args.length < 14) {
			System.out.println("testLSHManager <fileTS> <fileLSH> <fileQuery> <fileOutput> <size_R> <size_L> <top_k> <start_L> <end_L> <inc_L> <start_p> <end_p> <inc_p> <number_of_random_candidate>");
			return;
		}
		
		System.out.println("fileTS=" + args[0]);
	    System.out.println("fileLSH=" + args[1]);
	    System.out.println("fileQuery=" + args[2]);
	    System.out.println("fileOutput=" + args[3]);
	    System.out.println("size_R=" + args[4]);
	    System.out.println("size_L=" + args[5]);
	    System.out.println("top_k=" + args[6]);
	    System.out.println("start_L=" + args[7]);
	    System.out.println("end_L=" + args[8]);
	    System.out.println("inc_L=" + args[9]);
	    System.out.println("start_p=" + args[10]);
	    System.out.println("end_p=" + args[11]);
	    System.out.println("inc_p=" + args[12]);
	    System.out.println("number_of_random_candidates=" + args[13]);
	    
	    
	    int file_idx = 0;
	    if(args.length == 15) {
	    		file_idx = Integer.parseInt(args[14]);
	    }
		System.out.println("file_idx=" + file_idx);
	    
	    LSHtest_randombitset lshTest = new LSHtest_randombitset();
	    
	    System.out.println("lib=" + System.getProperty("java.library.path"));
	    lshTest.testLSHManager(args[0], args[1], args[2], args[3], Integer.parseInt(args[4]), 
	    					Integer.parseInt(args[5]), Integer.parseInt(args[6]), Integer.parseInt(args[7]),
	    					Integer.parseInt(args[8]), Integer.parseInt(args[9]), Integer.parseInt(args[10]),
	    					Integer.parseInt(args[11]), Integer.parseInt(args[12]), Integer.parseInt(args[13]),
	    					file_idx);
	}
}
