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

import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.PriorityQueue;
import java.util.StringTokenizer;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Writable;

import lsh.LSHinf;
import lshbased_lshstore.LSH_Hash;

import com.hp.hpl.palette.similarity.bridge.LSHManagerProxy;
import com.hp.hpl.palette.similarity.bridge.IndexBuilderProxy;
import com.hp.hpl.palette.similarity.bridge.SearchCoordinatorAtRProxy;
import com.hp.hpl.palette.similarity.bridge.TimeSeriesBuilderProxy;
import com.hp.hpl.palette.similarity.datamodel.TimeSeriesSearch;
import com.hp.hpl.palette.similarity.worker.SearchCoordinator;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import common.MaxHeap;
import common.Measurements;
import common.Utils;
import common.TimeSeries;

public class NaiveSearchTest {

	/**
	 * @param args
	 * @throws IOException 
	 */
	private static final Log LOG = LogFactory
			.getLog(NaiveSearchTest.class.getName());
	
	void testLSHManager(String datafile, String queryfile, int topK_no, int file_idx) throws IOException {
		// create LSHManager
		LSHManagerProxy managerInstance = LSHManagerProxy.INSTANCE;
		managerInstance.start(1, 1, 1);
	
		LOG.info("LSHManager instace started");
		
		DataInputStream fileQuery = new DataInputStream(new FileInputStream(queryfile));
		DataOutputStream fileNaiveResults = new DataOutputStream(new FileOutputStream("naive_resultfile"));
		
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
		LOG.info("query file reading done");
		
		// create TimeSeriesBuilder to populate time series data
		TimeSeries data = new TimeSeries(); 
		TimeSeriesBuilderProxy tsBuilder = new TimeSeriesBuilderProxy();
		LOG.info("Starting to read TimeSeries");
		int idx = 0;
		if(file_idx == 0) {
			DataInputStream fileTS = new DataInputStream(new FileInputStream(datafile));
			
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
		} else {
			for(int p=0;p<file_idx;p++) {
				LOG.info("Reading " + datafile + "_" + (p+1) + "...");
				
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
			searchCoordinatorAtRProxy.initiateSearchCoordinatorAtR (0, searchId, 1, 1, queryPattern, topK_no);
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
			}
			LOG.info(builder.toString());
			LOG.info("Naive search done time: " + (end1-start1) + " milliseconds");
		}
		fileNaiveResults.close();
		
	}
	
	// to do : first clean up the code and make compilable and check it out

	public static void main(String[] args) throws NumberFormatException, IOException {
		if(args.length < 3) {
			System.out.println("NaiveSearchTest <fileTS> <fileQuery> <top_k>");
			return;
		}
	    System.out.println("fileTS=" + args[0]);
	    System.out.println("fileQuery=" + args[1]);
	    System.out.println("top_k=" + args[2]);
	    int file_idx = 0;
	    if(args.length == 4) {
	    	file_idx = Integer.parseInt(args[3]);
	    }
	    System.out.println("file_idx=" + file_idx);
	    
	    NaiveSearchTest lshTest = new NaiveSearchTest();
	    
	    System.out.println("lib=" + System.getProperty("java.library.path"));
	    lshTest.testLSHManager(args[0], args[1], Integer.parseInt(args[2]), file_idx);
	}
}
