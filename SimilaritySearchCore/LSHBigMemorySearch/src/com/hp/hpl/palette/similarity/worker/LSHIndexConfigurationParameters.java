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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

public class LSHIndexConfigurationParameters implements Writable {


	// initial capacity for unordered_map hash index
	private int capacity = 700000;
	
	// milliseconds for communicator
	private int timeCommunicator = 300000;
 
 
	// input time series directory
	private String tsdir;
	
	//the total number of the R's that we need to build the Radius.  
	private int RNumber;
	//the total number of the L tables for each R that we will need to build the tables.
	private int LNumber;
	//the expected total number of the element entries in a single L table
	private int capacityForEntriesInLTable; 
	
	//top-K this is also a design parameter.
	private int topK;
    //the query length used for the LSH based search 
	private int queryLength;
	
	//for each partition, how many worker threads on each processing unit to build the index.
	private int numberOfWorkerThreadsForIndexing; 
	//for each partition, how many worker threads on each processing unit to build the index.
	private int numberOfWorkerThreadsForSearching; 
	
	//the total maximum number of the concurrent queries can be accepted. 
	private int numberOfConcurrentQueriesAllowed;
	
	//the total number of the perturbation required to search the candidates. 
	private int numberOfPerturbations; 
	
	//for the indexing, we will need a collection of L tables assigned for a single thread to do the indexing, called indexing group. 
	private int numberOfLTablesInAIndexingGroup;
	
	//for the multi-threaded search, the number of the worker threads assigned for doing concurrent search.
	private int numberOfThreadsForConcurrentSearch; 
	
	//for the concurrent intermediate results aggregation at the aggegator side
	private int numberOfThreadsForConcurrentAggregation;
	
	//for each worker thread, the size of the input query queue 
	private int numberOfQueriesQueuedForWorkerThread;
	
	//for each worker thread, how many concurrent queries that it can handle in the memory.
	private int numberOfQueriesInWorkListForWorkerThread; 
	
	//to define the size of the intermediate query results that need to send to the coordinator 
	private int communicatorBufferLimit; 
	
	//to define the timer thread sleeping milliseconds before to clean up the communication buffer;
	private int scheduledCommunicatorBufferSendingInterval;
	
	//to define the time internal when the time to do the clean up of the trackers at the aggregator component has expired.
	private int waitNumberOfQueriesBeforeCleanUpTracker; 

	
	//to be continued for others;
	@Override
	public void readFields(DataInput dataInput) throws IOException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void write(DataOutput dataOutput) throws IOException {
		// TODO Auto-generated method stub
		
	}

	public int getRNumber() {
		return this.RNumber;
	}
	
	public void setRNumber(int val) {
		this.RNumber = val;
	}
	
	public int getLNumber() {
		return this.LNumber; 
	}
	
	public void setLNumber(int val) {
		this.LNumber = val;
	}
	
	public int getNumberofWorkerThreadsForIndexing (){ 
		return this.numberOfWorkerThreadsForIndexing;
	}
	
	public void setNumberofWorkerThreadsForIndexing (int val) {
		this.numberOfWorkerThreadsForIndexing = val;
	}
	
	public int getNumberOfWorkerThreadsForSearchPerQuery() {
		return this.numberOfWorkerThreadsForSearching; 
	} 
	
	public void setNumberOfWorkerThreadsForSearchPerQuery(int val) {
		this.numberOfWorkerThreadsForSearching= val;
	}
	
	public int getNumberOfConcurrentQueriesAllowed() {
		return this.numberOfConcurrentQueriesAllowed;
	}
	
	public void setNumberOfConcurrentQueriesAllowed(int val) {
		this.numberOfConcurrentQueriesAllowed = val;
	}
	
	public int getTopK() {
		return this.topK;
	}
	
	public void setTopK(int val) {
		this.topK = val;
	}
	
	public int getQueryLength() {
		return this.queryLength;
	}
	
	public void setQueryLength(int val) {
		this.queryLength =val;
	}
	
	public int getNumberOfPerturbations() {
		return this.numberOfPerturbations;
	}
	
	public void setNumberOfPerturbations(int val) {
		this.numberOfPerturbations = val;
	}
	
	public int getNumberOfLTablesInAIndexingGroup() {
		return this.numberOfLTablesInAIndexingGroup;
	}
	
	public void setNumberOfLTablesInAIndexingGroup(int val) {
		this.numberOfLTablesInAIndexingGroup = val;
	}
	
	public void setNumberOfThreadsForConcurrentQuerySearch (int val) {
		this.numberOfThreadsForConcurrentSearch = val;
	}
	
	public int getNumberOfThreadsForConcurrentQuerySearch () {
		return this.numberOfThreadsForConcurrentSearch;
	}
	
 
	public void setNumberOfQueriesInWorkListForWorkerThread (int val) {
		this.numberOfQueriesInWorkListForWorkerThread = val;
	}
	
	public int getNumberOfQueriesInWorkListForWorkerThread() {
		return this.numberOfQueriesInWorkListForWorkerThread; 
	}
	
	
	public void setNumberOfQueriesQueuedForWorkerThread (int val) {
		this.numberOfQueriesQueuedForWorkerThread = val;
	}
	
	public int getNumberOfQueriesQueuedForWorkerThread () {
		return this.numberOfQueriesQueuedForWorkerThread;
	}
	
	public void setCommunicatorBufferLimit (int val)  {
		this.communicatorBufferLimit = val;
	}
	
	public int getCommunicatorBufferLimit() {
		return this.communicatorBufferLimit;
	}
	
	public void setScheduledCommunicatorBufferSendingInterval (int val) {
		this.scheduledCommunicatorBufferSendingInterval = val;
	}
	
	public int getScheduledCommunicatorBufferSendingInterval () {
		return this.scheduledCommunicatorBufferSendingInterval;
	}
	
	public void setCapacityForEntriesInLTable (int capacity) {
		this.capacityForEntriesInLTable = capacity;
	}
	
	public int getCapacityForEntriesInLTable() {
		return this.capacityForEntriesInLTable;
	}
	
	public void setNumberOfThreadsForConcurrentAggregation (int val) {
		this.numberOfThreadsForConcurrentAggregation = val;
	}
	
	public int getNumberOfThreadsForConcurrentAggregation (){
		return this.numberOfThreadsForConcurrentAggregation;
	}
	
	public void setWaitNumberOfQueriesBeforeCleanUpTracker(int val) { 
		this.waitNumberOfQueriesBeforeCleanUpTracker = val;
	}
	
	public int getWaitNumberOfQueriesBeforeCleanUpTracker(int defaultQueries) {
		if (this.waitNumberOfQueriesBeforeCleanUpTracker == 0) {
			return defaultQueries;
		}
		else {
		    return this.waitNumberOfQueriesBeforeCleanUpTracker;
		}
	}
	
    public void setTSdir(String val) {
			this.tsdir = val;
	}
		
	public String getTSdir() {
			return this.tsdir;
    }
	

	public int getTimeCommunicator() {
		return this.timeCommunicator;
	}
	
	public void setTimeCommunicator(int val) {
		this.timeCommunicator = val;
	}

}
