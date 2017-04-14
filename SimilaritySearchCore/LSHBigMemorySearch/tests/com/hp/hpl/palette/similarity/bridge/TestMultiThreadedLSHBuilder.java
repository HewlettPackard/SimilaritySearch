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
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintStream;
import java.util.ArrayList;
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
import com.hp.hpl.palette.similarity.worker.SearchCoordinator;
import com.hp.hpl.palette.similarity.worker.IndexBuilder;
import com.hp.hpl.palette.similarity.worker.IndexBuilderImpl;
import com.hp.hpl.palette.similarity.worker.IndexBuilder.PartitionOfLTablesAtRForIndexBuilding;

import com.hp.hpl.palette.similarity.worker.LSHIndexConfigurationParameters;

import org.apache.commons.logging.LogFactory;
import org.apache.log4j.LogManager;

import common.MaxHeap;
import common.Measurements;
import common.Utils;
import common.TimeSeries;

public class TestMultiThreadedLSHBuilder {

	/**
	 * @param args
	 * @throws IOException 
	 */
	private static final Log LOG = LogFactory.getLog(TestMultiThreadedLSHBuilder.class.getName());
	
	void testLSHManager(String datafile, String lshfile, String queryfile, String outfile, int R_num, int L_num, 
			int topK_no, int start_L, int end_L, int inc_L, int start_p, int end_p, int inc_p,
			int use_serial, int file_idx, LSHIndexConfigurationParameters  otherParameters
			) throws IOException {
		// create LSHManager
		LSHManagerProxy managerInstance = LSHManagerProxy.INSTANCE;
		managerInstance.start(R_num, L_num, 700000);
	
		LOG.info("LSHManager instace started");
		
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
		
		//to be replaced by the multi-threaded counterpart. 
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
		
		LOG.info("Run the top command");
		System.in.read(); // this is for top
		LOG.info("Starting to postProcessing");
		HashIndexProxy hashIndexProxy = new HashIndexProxy();
		for(int i=0;i<R_num;i++) {
			for(int j=0;j<L_num;j++) {
				int mem = hashIndexProxy.getMemoryOccupied(managerInstance, i, j);
				LOG.info(i + "," + j + "=" + mem);
				hashIndexProxy.postProcessing(managerInstance, i, j);
			}
		}
		 
		//output.close();
		System.out.println("please use ctrl+c to exit the process");
		while(true) {
			
		}
	}
	
	public void output(String output) {
		//LOG.info(output);
		System.out.println(output);
	}
	
	// to do : first clean up the code and make compilable and check it out

	public static void main(String[] args) throws NumberFormatException, IOException {
		if(args.length < 14) {
			System.out.println("testLSHManager <fileTS> <fileLSH> <fileQuery> <fileOutput> <size_R> <size_L> <top_k> <start_L> <end_L> <inc_L> <start_p> <end_p> <inc_p> <use_serial>");
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
	    System.out.println("use_serial=" + args[13]);
	    
	    
	    int file_idx = 0;
	    if(args.length == 15) {
	    	file_idx = Integer.parseInt(args[14]);
	    }
	    TestMultiThreadedLSHBuilder lshTest = new TestMultiThreadedLSHBuilder ();
	    
	    System.out.println("lib=" + System.getProperty("java.library.path"));
	    
	    //we need to set the following parameters required for the processing. 
	    LSHIndexConfigurationParameters  otherParameters = new  LSHIndexConfigurationParameters ();
	    otherParameters.setRNumber(3);
	    otherParameters.setLNumber(100);
	    otherParameters.setQueryLength(48);
	    otherParameters.setNumberofWorkerThreadsForIndexing(3);
	    otherParameters.setNumberOfLTablesInAIndexingGroup(50); //we will do it in one single threads.
	    
	    
	    
	    lshTest.testLSHManager(args[0], args[1], args[2], args[3], Integer.parseInt(args[4]), 
	    					Integer.parseInt(args[5]), Integer.parseInt(args[6]), Integer.parseInt(args[7]),
	    					Integer.parseInt(args[8]), Integer.parseInt(args[9]), Integer.parseInt(args[10]),
	    					Integer.parseInt(args[11]), Integer.parseInt(args[12]), Integer.parseInt(args[13]),
	    					file_idx, otherParameters);
	}
}
