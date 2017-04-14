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
import java.util.ArrayList;
import java.util.StringTokenizer;
import java.util.Date;

import java.io.IOException;
import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintStream;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Writable;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;


import com.hp.hpl.palette.similarity.configuration.SupportedSearchCategory;
import com.hp.hpl.palette.similarity.datamodel.TimeSeriesSearch;
import com.hp.hpl.palette.similarity.worker.SinkAdaptorForBufferedCommunicator;

import com.hp.hpl.palette.similarity.bridge.HashIndexProxy;
import com.hp.hpl.palette.similarity.bridge.LSHManagerProxy;
import com.hp.hpl.palette.similarity.bridge.IndexBuilderProxy;
import com.hp.hpl.palette.similarity.bridge.SearchCoordinatorAtRProxy;
import com.hp.hpl.palette.similarity.bridge.TimeSeriesBuilderProxy;
import com.hp.hpl.palette.similarity.worker.IndexBuilder;
import com.hp.hpl.palette.similarity.worker.IndexBuilderImpl;
import com.hp.hpl.palette.similarity.worker.LSHIndexConfigurationParameters;
import com.hp.hpl.palette.similarity.worker.SearchCoordinator;
import com.hp.hpl.palette.similarity.worker.IndexBuilder.PartitionOfLTablesAtRForIndexBuilding;

import common.MaxHeap;
import common.Measurements;
import common.Utils;
import common.TimeSeries;


import lsh.LSHinf;
import lshbased_lshstore.LSH_Hash;

/**
 * to test under the concurrent search, whether the search results can be finally queued at the final queue. We use SimulatedSearchCoordinatorrImpl class.
 * We control the total size of the input queries and the number of the R that each query will be used, and the total output intermediate results that we 
 * should get.
 * 
 * @author Jun Li
 *
 */
public class MultithreadedSearchManagerIndexSearchCombinedTestNew {

	private static final Log LOG = LogFactory.getLog(MultithreadedSearchManagerIndexSearchCombinedTestNew.class.getName());
	
	private LSHIndexConfigurationParameters configurationParameters;
	private int partitionId;
    private  SinkAdaptorForBufferedCommunicator adaptor;  
    private List<TimeSeriesSearch.SearchQuery>  queries;
    
 
    public MultithreadedSearchManagerIndexSearchCombinedTestNew(int R_num,  int L_num) throws Exception {
    	setUp(R_num, L_num);
    }
    
	protected void setUp(int R_num, int L_num) throws Exception{ 
		 //do something first 
		
		 this.partitionId = 1;
		 this.configurationParameters = new LSHIndexConfigurationParameters();
		 
		 //about indexing. totally use 6 threads.
	 
		 int queryLength = 48; //VERY IMPORTANT to put this here. 
		 int perturbationNumber = 50; 
		 int numIndexingThreads = 6;
		 int numbrOfLTablesInAIndexingGroup = 50;
		 int topK = 5;
		 
		 this.configurationParameters.setRNumber(R_num);
		 this.configurationParameters.setLNumber(L_num);
		 this.configurationParameters.setQueryLength(queryLength);
		 this.configurationParameters.setNumberOfPerturbations(perturbationNumber);
		 this.configurationParameters.setTopK(topK);
		 this.configurationParameters.setNumberofWorkerThreadsForIndexing(numIndexingThreads); //worker threads 
		 this.configurationParameters.setNumberOfLTablesInAIndexingGroup(numbrOfLTablesInAIndexingGroup); 
		    
		    
		    
		 //about search 
		 this.configurationParameters.setCommunicatorBufferLimit(300); 
		 //I think that I do not use this number.
		 this.configurationParameters.setNumberOfConcurrentQueriesAllowed(1000); 
		 this.configurationParameters.setNumberOfQueriesInWorkListForWorkerThread(10); 
		 this.configurationParameters.setNumberOfWorkerThreadsForSearchPerQuery(1); 
		 this.configurationParameters.setNumberOfThreadsForConcurrentQuerySearch(1); //worker threads to be 1, let's make it single thread first.
		 this.configurationParameters.setScheduledCommunicatorBufferSendingInterval(200);//200 ms
		 this.configurationParameters.setNumberOfQueriesQueuedForWorkerThread (10); 
		 
		 this.adaptor = new SinkAdaptorForBufferedCommunicatorImpl.SimulatedSinkAdaptorForBufferedCommunicator();
		 
		 //the actual queries is prepared at the loading/indexing phase. 
		 this.queries = new ArrayList<TimeSeriesSearch.SearchQuery>();
		 
	 
		 
	 }
	 
	 
	 public static class ShutdownWorker extends Thread {
		 private int sleepTime;
		 private MultiSearchManager manager;
		 
		 public ShutdownWorker (int sleepTime, MultiSearchManager manager) {
			 this.sleepTime = sleepTime;
			 this.manager = manager;
		 }
		 
		 public void run() {
	   	    	try {
		    	   //take some sleep, before doing the shutdown action 
	   	    	   Thread.sleep (this.sleepTime); 
	   	    	}
	   	    	catch (Throwable t) {
	   	    		//do some logging 
	   	    	}
	   	    	
	   	    	manager.shutdownQueryProcessors(); 
	   	    	LOG.info("we should now done with the shutdown of the search manager");
	   	    }
	 }
	 
	 public void testIndexBuildingAndQuerySearch (LSHManagerProxy managerInstance) {
		
					
		 conductIndexBuildingPhase(managerInstance); 
		 
		 performQuerySearch();
	 }

	 private void conductIndexBuildingPhase(LSHManagerProxy managerInstance) {
		 //String datafile = "./data/datasets/ts_sample_55G"; //inputDataSet
		 String datafile = "/home/junli/MultiThreadTesting/datasets/ts_sample_55G_1"; //new input data set to test multiple partitions. 
		 String lshfile = "./data/hashes/LSH_HASH_K19_L104_55G_NEWR6_A";//hashFile
		 String queryfile = "./data/queries/experiment1_50/queryfile"; //queryFile
		 String outputfile = "./results/res1"; //outputFile;
		 int topK= this.configurationParameters.getTopK();
		 int sizeR=this.configurationParameters.getRNumber();
		 int sizeL=this.configurationParameters.getLNumber();
		 int startL= this.configurationParameters.getLNumber(); //? why
		 int endL=this.configurationParameters.getLNumber();
		 int  incL=this.configurationParameters.getLNumber();
		 int startP= this.configurationParameters.getNumberOfPerturbations();//the perturbation.
		 int endP=this.configurationParameters.getNumberOfPerturbations();
		 int incP=this.configurationParameters.getNumberOfPerturbations();
		 int useMultithreaded=1;
		 int numberOfWorkerThreadsForIndexing=this.configurationParameters.getNumberofWorkerThreadsForIndexing();
		 int numbrOfLTablesInAIndexingGroup=this.configurationParameters.getNumberOfLTablesInAIndexingGroup();
		 
		 int file_idx=0; 
		 
		 try {
		 testLSHManager(managerInstance, 
				 datafile, lshfile, queryfile, outputfile, sizeR, sizeL, topK, startL, endL, incL, 
				    startP, endP, incP, 
				    useMultithreaded, numberOfWorkerThreadsForIndexing, numbrOfLTablesInAIndexingGroup, 
				    file_idx);
		 }
		 catch (IOException ex) {
			 LOG.error("fails to conduct index building phase for LSHtest....", ex);
		 }

		 
	 }
	 
	 
	 private void testLSHManager(LSHManagerProxy managerInstance, 
			 String datafile, String lshfile, String queryfile, 
			 String outfile, int R_num, int L_num, int topK_no, 
			 int start_L, int end_L, int inc_L, 
			 int start_p, int end_p, int inc_p, 
			 int use_serial,  int numThreads, int numLforOneThread, int file_idx) throws IOException {
		
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
			
			//added by Jun Li, to populate the queries.
			{
				 for (int i=0; i<querylist.size(); i++)
				 {
					     ArrayList<Float> aQuery= querylist.get(i);
					     int patternSize= aQuery.size();
					    
					    //Now input the query 2
						 String id = UUID.randomUUID().toString();
						 float[] queryPattern = new float[patternSize];
						 for (int j=0; j<patternSize; j++) {
							 queryPattern[j]=aQuery.get(j).floatValue();
						 }
						 int topK = this.configurationParameters.getTopK();
						 TimeSeriesSearch.SearchQuery query = new TimeSeriesSearch.SearchQuery(id, queryPattern, topK);
						 
						 this.queries.add(query);
				 }
			}
			
			//NOTE: this number should be identical to the actual specified topK size at the setup. 
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
				
				
				//from Jun's code for the multi-threaded index building. 
				IndexBuilder builder = new IndexBuilderImpl(this.configurationParameters);
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
			// for memory checking
			//LOG.info("Run the top command");
			//System.in.read(); // this is for top
			LOG.info("Starting to postProcessing");
			HashIndexProxy hashIndexProxy = new HashIndexProxy();
			for(int i=0;i<R_num;i++) {
				for(int j=0;j<L_num;j++) {
			//		int mem = hashIndexProxy.getMemoryOccupied(managerInstance, i, j);
			//		LOG.info(i + "," + j + "=" + mem);
					hashIndexProxy.postProcessing(managerInstance, i, j);
				}
			}
			LOG.info("postProcessing done");
	 }
	 

	 
	 /**
	  * this is to use the real LSH searcher.
	  */
	 
	 private void performQuerySearch () {
		 
		 //we will use the real LSH searcher.
		 SupportedSearchCategory.SupportedSearch chosenSearch = SupportedSearchCategory.SupportedSearch.LSH_SEARCH; 
		 MultiSearchManager  manager = 
				  new MultiSearchManagerImpl(this.configurationParameters, this.partitionId, this.adaptor, chosenSearch, false);
		 manager.startQueryProcessors();
		 
		 
		 try {
			   Thread.sleep(10000);
		 }
		 catch (Exception e) {
			  // Do Nothing
		 }
		 
		 LOG.info("the total number of search queries prepared is: " + this.queries.size());
 
		 manager.acceptAndDistributeSearchQueries(this.queries);
		
		 LOG.info("next wait for 100 seconds for potential remaining things to be done and then dispaly the total search results..........");
		 //after 100 seconds
		 try {
			   Thread.sleep(100000);
		 }
		 catch (Exception e) {
			  // Do Nothing
		 }
		 
		 //I should be able to see the result get out of the adaptor. There is no timer thread involved here.
		 List<TimeSeriesSearch.IntermediateQuerySearchResult> result = this.adaptor.retrieveBufferedSearchResults();
		 LOG.info("in test,  the intermediate results retrieved has size of: " + result.size());
		
		 displayQueryAndResults(result);
		 
		 
		 int waitTime= 10*1000; //10 seconds
		 ShutdownWorker worker = new ShutdownWorker(waitTime, manager);
		 worker.start();
		 
		 try {
			   manager.waitForQueryProcessors();
		 }
		 catch (Exception ex) {
				 LOG.error("wait for processor to exit, ignore it", ex);
		 }
		 
			
		 try {
			 worker.join();
		 }
		  catch(InterruptedException ex) {
				 //do nothing.
		 }
			 
		 LOG.info("now we are done with the thread launching testing");
			 
	 }
	 
	 private void displayQueryAndResults(List<TimeSeriesSearch.IntermediateQuerySearchResult> result) {
		 int topK = this.configurationParameters.getTopK();
		 int counter=0;
		 for (TimeSeriesSearch.SearchQuery searchQuery: this.queries) {
			 
			 //for each query. get to the largest R, and find the top-K results.
			 List<TimeSeriesSearch.IntermediateQuerySearchResult> foundResultsForTheQuery= 
					                        new ArrayList<TimeSeriesSearch.IntermediateQuerySearchResult>();
			 //full scan the result 
			 for (TimeSeriesSearch.IntermediateQuerySearchResult intermediateResults: result) {
				 if (intermediateResults.getQuerySearchId().compareTo(searchQuery.getQuerySearchId()) == 0) {
					 foundResultsForTheQuery.add(intermediateResults);
				 }
			 }
			 
			 //all the intermediate results displaying each R's computation. 
			 //NOTE: because of the buffered communicator accumulates the same intermediate result object for each R, and we did not 
			 //use the object copy to copy the result to the output buffer(different from the real deployment, where the message accumulated will have to BE
			 //COPIED and then sent out to the wire. 
			 //NOTE: this needs to be fixed in a distributed environment.
			 //therefore, all of the R's will hold the same query results. 
			 for (TimeSeriesSearch.IntermediateQuerySearchResult foundResultsForEachR: foundResultsForTheQuery) {
				 LOG.info("*******Inspect R: " + foundResultsForEachR.getAssociatedRvalue() 
						                          + " intermediate results for search id: " + searchQuery.getQuerySearchId());
				  
				 StringBuilder strbuilder = new StringBuilder();
				 strbuilder.append("\n<<top-" + topK + " result>>\n");
				 List<TimeSeriesSearch.SearchResult> innerResult = foundResultsForEachR.getSearchResults();
				 if ((innerResult ==null) || (innerResult.size() == 0)) {
					LOG.info("at R: " + foundResultsForEachR.getAssociatedRvalue() + " no search results identified. ");
				 }
				 for(int i=0;i<innerResult.size();i++) {
						TimeSeriesSearch.SearchResult ms = innerResult.get(i);
						strbuilder.append(ms.id + " " + ms.offset + " " + ms.distance + "\n");
				 }
				 LOG.info(strbuilder.toString());
				 
				 LOG.info("*******End of Inspect R: " + foundResultsForEachR.getAssociatedRvalue() 
                         + " intermediate results for search id: " + searchQuery.getQuerySearchId());

			 }
			 
			 //NOTE: Since for each R, we send out the intermediate results, and we know that for a bigger R, that is computed later, the
			 //accumulated result associated with the bigger R should contain the smaller R's search results. So we just need to pick the 
			 //biggest R's search result.
			 int Rvalue = -1;
			 TimeSeriesSearch.IntermediateQuerySearchResult finalResult = null;
			 for (TimeSeriesSearch.IntermediateQuerySearchResult foundResult: foundResultsForTheQuery) {
				 if (foundResult.getAssociatedRvalue() > Rvalue) {
					 finalResult = foundResult;
					 Rvalue = foundResult.getAssociatedRvalue();
				 }
			 }
			 
			 //display the final result:
			 LOG.info("********search query: ** " + counter + "  ** with search id:" + searchQuery.getQuerySearchId() + " has the final following search result:");
			 if (Rvalue == -1) {
				 LOG.error("not search result found for search query: " + counter);
			 }
			 else {
				 List<TimeSeriesSearch.SearchResult> innerResult = finalResult.getSearchResults();
				 
				 int n = this.configurationParameters.getTopK();
				 StringBuilder strbuilder = new StringBuilder();
				 strbuilder.append("\n<<top-" + n + " result>>\n");
				 for(int i=0;i<innerResult.size();i++) {
						TimeSeriesSearch.SearchResult ms = innerResult.get(i);
						strbuilder.append(ms.id + " " + ms.offset + " " + ms.distance + "\n");
				 }
				 LOG.info(strbuilder.toString());
			 }
			 
			 counter++;
			 
		 }
	 }
 
	 
	 public static void main(String[] args) {
		// create LSHManager
		 LSHManagerProxy  managerInstance = LSHManagerProxy.INSTANCE;
		 int R_num = 3;
		 int L_num = 100;
		 managerInstance.start(R_num, L_num, 700000);
			
		 try {
			 MultithreadedSearchManagerIndexSearchCombinedTestNew driver = 
					                new MultithreadedSearchManagerIndexSearchCombinedTestNew (R_num, L_num);
			 driver.testIndexBuildingAndQuerySearch (managerInstance);
		 }
		 catch (Exception ex) {
			 LOG.error("fails to run test: " , ex);
		 }
	     
	 }
}
