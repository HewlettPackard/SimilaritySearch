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
import java.util.Random;
import java.util.ArrayList;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.junit.Assert;
import org.junit.Test;
import junit.framework.TestCase;

import com.hp.hpl.palette.similarity.configuration.SupportedSearchCategory;
import com.hp.hpl.palette.similarity.datamodel.TimeSeriesSearch;
import com.hp.hpl.palette.similarity.worker.SinkAdaptorForBufferedCommunicator;

/**
 * to test earlier abandonment.
 * 
 * @author Jun Li
 *
 */
public class MultiThreadedSearchManagerAbandonmentTest extends TestCase {

	private static final Log LOG = LogFactory.getLog(MultiThreadedSearchManagerAbandonmentTest.class.getName());
	
	private LSHIndexConfigurationParameters configurationParameters;
	private int partitionId;
    private  SinkAdaptorForBufferedCommunicator adaptor;  
    
	 @Override
	 protected void setUp() throws Exception{ 
		 //do something first 
		 this.partitionId = 1;
		 this.configurationParameters = new LSHIndexConfigurationParameters();
		 this.configurationParameters.setCommunicatorBufferLimit(300); 
		 //I think that I do not use this number.
		 this.configurationParameters.setNumberOfConcurrentQueriesAllowed(1000); 
		 this.configurationParameters.setNumberOfQueriesInWorkListForWorkerThread(10); 
		 this.configurationParameters.setNumberOfWorkerThreadsForSearchPerQuery(1); 
		 this.configurationParameters.setNumberOfThreadsForConcurrentQuerySearch(10);
		 this.configurationParameters.setScheduledCommunicatorBufferSendingInterval(200);//200 ms
		 this.configurationParameters.setNumberOfQueriesQueuedForWorkerThread (20); 
		 
		 this.adaptor = new SinkAdaptorForBufferedCommunicatorImpl.SimulatedSinkAdaptorForBufferedCommunicator();
		 
		 super.setUp();
		 
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
	 

	 /**
	  * The configuration setting in "controller.properties" under the "environment" directory should be changed accordingly.
	  *  with 4 queries and then each query requires 4 Rs
	  */
	 @Test
	 public void testSimpleQueryInputAndOutput () {
		 SupportedSearchCategory.SupportedSearch chosenSearch = SupportedSearchCategory.SupportedSearch.LSH_SEARCH; 
		 MultiSearchManager  manager = 
				  new MultiSearchManagerImpl(this.configurationParameters, this.partitionId, this.adaptor, chosenSearch, true);
		 manager.startQueryProcessors();
		 
		 
		 try {
			   Thread.sleep(10000);
		 }
		 catch (Exception e) {
			  // Do Nothing
		 }
		 
		 List<TimeSeriesSearch.SearchQuery> queries = new ArrayList<TimeSeriesSearch.SearchQuery>();
 
		 //this is to test that when the buffer becomes full, the worker thread is also working on pushing the buffered messages out of the communicator
		 //queue.
		 int totalNumberOfQueries= 600; 
		 for (int i=0; i<totalNumberOfQueries; i++)
		 {
			    //Now input the query 2
				 String id = UUID.randomUUID().toString();
				 float[] queryPattern = {(float)4.0, (float)5.0, (float)i};
				 int topK = 5;
				 TimeSeriesSearch.SearchQuery query = new TimeSeriesSearch.SearchQuery(id, queryPattern, topK);
				 
				 queries.add(query);
		 }
		
		
		 //we next create the early abandonment commands that may be get into the system faster (that, the pre-arrival abandonment commands)
         Random rand = new Random(System.currentTimeMillis());
         List<TimeSeriesSearch.QuerySearchAbandonmentCommand> commands = new ArrayList<TimeSeriesSearch.QuerySearchAbandonmentCommand>();
         int totalNumberOfAbandonmentCommands = 100; 
         int counter=0;
		 for (TimeSeriesSearch.SearchQuery searchQuery:queries) {
			 String searchId = searchQuery.getQuerySearchId();
	    	 boolean randVal = rand.nextBoolean();
	    	 int partitionId = 0; //will fill in later via MapReduce
	    	 
	    	 int hashCode = Math.abs(searchId.hashCode()); //it can be a negative integer...
	    	 //take the reminder.
	    	 int threadProcessorId = hashCode%this.configurationParameters.getNumberOfThreadsForConcurrentQuerySearch();
	    	 if (randVal) {
	    		 counter++;
	    		 if (counter > totalNumberOfAbandonmentCommands) {
	    			 break;
	    		 }
	    		TimeSeriesSearch.QuerySearchAbandonmentCommand  command = 
	    				                 new TimeSeriesSearch.QuerySearchAbandonmentCommand (searchId, partitionId, threadProcessorId);
	    		commands.add(command);
	    	 }
 		 }
		 
		 LOG.info("total number of the abandonment commands generated are: " + commands.size() + "============================");
		 manager.acceptAndDistributeSearchQueriesAbandonment(commands);
		 
		 //while we are pushing to the queue that can be potentially blocked. So we will have to push the abandonment list first,
		 //and then the manager list. Otherwise, we will have to get to the end of the query processing, before we can push
		 //the early abandonment.
		 manager.acceptAndDistributeSearchQueries(queries);
		 
		 //after 500 ms, 
		 try {
			   Thread.sleep(60000);
		 }
		 catch (Exception e) {
			  // Do Nothing
		 }
		 
		 //I should be able to see the result get out of the adaptor. There is no timer thread involved here.
		 List<TimeSeriesSearch.IntermediateQuerySearchResult> result = this.adaptor.retrieveBufferedSearchResults();
		 LOG.info("in test,  the intermediate results retrieved has size of: " + result.size());
		 //we will have 4 R always, and we have 4 queries.
		 Assert.assertTrue (result.size() == 4*(totalNumberOfQueries-totalNumberOfAbandonmentCommands));
		 
		 
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
	 
	 
	 @Override
	 protected void tearDown() throws Exception{ 
		 //do something first;
		 super.tearDown();
	 }
	 
	 public static void main(String[] args) {
		  
	      junit.textui.TestRunner.run(MultiThreadedSearchManagerAbandonmentTest.class);
	 }
}
