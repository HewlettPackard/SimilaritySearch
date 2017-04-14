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
package com.hp.hpl.palette.similarity.coordinator;

import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.AbstractMap.SimpleEntry;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletionService;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.zeromq.ZMQ;

import com.hp.hpl.palette.similarity.configuration.SupportedSearchCategory;
import com.hp.hpl.palette.similarity.datamodel.TimeSeriesSearch;
import com.hp.hpl.palette.similarity.datamodel.ProcessingUnitRuntimeInformation;
import com.hp.hpl.palette.similarity.datamodel.CoordinatedIndexBuilding;
import com.hp.hpl.palette.similarity.datamodel.CoordinatedIndexBuilding.SchedulingUnit;
import com.hp.hpl.palette.similarity.worker.LSHIndexConfigurationParameters;
import com.hp.hpl.palette.similarity.worker.SinkAdaptorForAbandonmentCommand;
import com.hp.hpl.palette.similarity.worker.SinkAdaptorForFinalQueryResult;
import com.hp.hpl.palette.similarity.worker.SinkAdaptorForAbandonmentCommandImpl;
import com.hp.hpl.palette.similarity.worker.SinkAdaptorForFinalQueryResultImpl;
import com.hp.hpl.palette.similarity.worker.IndexBuildingScheduler;
import com.hp.hpl.palette.similarity.worker.concurrent.AsynAggregationCoordinatorIntf;
import com.hp.hpl.palette.similarity.configuration.CoordinatorPortConfiguration;

/**
 * The coordinator that serves the aggregation of the intermediate results from the distributed partitioners.

 *
 */
public class CoordinatorMasterImpl implements CoordinatorMaster {
	private ZMQ.Context context; 
	private int numberOfPartitions; //we will need to have this one to be fixed at this time.
	private String privateIPAddress; //the private IP address to be  bounded.
	private LSHIndexConfigurationParameters parameters;
	
	private List<Thread> toplevelThreadsManaged; 
    private static final Log LOG = LogFactory.getLog(CoordinatorMasterImpl.class.getName());
    
    private List<ProcessingUnitRuntimeInformation> runtimeInformationFromPartitioners; 
    private IndexBuildingScheduler indexingScheduler; //this variable gets created right after the synchronization is done.
   
    private boolean simulated; 
    private SupportedSearchCategory.SupportedSearch chosenSearch;
	/**
	 * to support the multi-threaded single application test environment, the context will have to come from the main thread, and thus we will 
	 * have to pass the context all the way from the main thread. 
	 *
	 * @param numberOfPartitions
	 * @param privateIPAddress
	 * @param parameters
	 * @param chosenSearch to choose whether we are in LSH search or in Naive search.
	 * @param  simulated to indicate that the partitioners are running in the simulated search coordinator (that is, no actual search is carried out)
	 */
	public CoordinatorMasterImpl(ZMQ.Context context, int numberOfPartitions, String privateIPAddress, LSHIndexConfigurationParameters parameters,
			SupportedSearchCategory.SupportedSearch chosenSearch, 
			boolean simulated) {
		 this.numberOfPartitions = numberOfPartitions;
		 this.privateIPAddress = privateIPAddress;
		 this.parameters = parameters; 
		 //1 is for one single IO thread per socket.
		 //1 is for one single IO thread per socket.
		 this.context = context;
		 
		 this.toplevelThreadsManaged = new ArrayList<Thread>();
		 
		 this.runtimeInformationFromPartitioners = new ArrayList<ProcessingUnitRuntimeInformation> ();
		 
		 this.simulated = simulated;
		 this.chosenSearch= chosenSearch;
	}
	
	/**
	 * The coordinator will start the real processing, only when all of the partitions respond to the coordinator already.
	 */
	@Override
	public int getNumberOfPartition() {
		return  this.numberOfPartitions; 
	}

	private ZMQ.Socket synchronizationBarrier1;
	private ZMQ.Socket synchronizationBarrier2;
	private ZMQ.Socket indexingCoordinationPublisher;
	private ZMQ.Socket indexBuildingNotifier;
	
	@Override
	public void init() {
		//(1) for scheduling.
		//before getting to the polling of the message, let's use REQ and REP to wait for the synchronization point. 
        this.synchronizationBarrier1 = context.socket(ZMQ.REP);
        int synchronizationPort1=CoordinatorPortConfiguration.QUERY_COMMAND_DISTRIBUTION_SYNCHRONIZATION_PORT_1;
        String destinationSynchronizationAddress1 = "tcp://" + this.privateIPAddress
                                                       +":" + ( new Integer(synchronizationPort1)).toString();
        this.synchronizationBarrier1.bind(destinationSynchronizationAddress1);
        
        //it block when the aggregator is ready 
        this.synchronizationBarrier2 = context.socket(ZMQ.PUB);
        int synchronizationPort2=CoordinatorPortConfiguration.QUERY_COMMAND_DISTRIBUTION_SYNCHRONIZATION_PORT_2;
        String destinationSynchronizationAddress2 = "tcp://" + this.privateIPAddress
                                                      +":" + ( new Integer(synchronizationPort2)).toString();
        this.synchronizationBarrier2.bind(destinationSynchronizationAddress2);
        
        LOG.info("finish synchronization related ports initialization");
		
		//(2) for index building: bind the sockets;
		this.indexingCoordinationPublisher = context.socket(ZMQ.PUB);
	    int indexingCoordinationPort = CoordinatorPortConfiguration.COORDINATED_INDEX_BUILDING_PORT_1;
	    String indexingCoordinationAddress = "tcp://" + this.privateIPAddress
	    		                                          +":" + ( new Integer(indexingCoordinationPort)).toString();
	    this.indexingCoordinationPublisher.bind(indexingCoordinationAddress);
	   
        
        this.indexBuildingNotifier = context.socket(ZMQ.REP);
        int indexBuildingNotificationPort = CoordinatorPortConfiguration.COORDINATED_INDEX_BUILDING_PORT_2;
        String indexBuildingNotificationAddress = "tcp://" +  this.privateIPAddress
                                                           +":" + ( new Integer(indexBuildingNotificationPort)).toString();
        this.indexBuildingNotifier.bind(indexBuildingNotificationAddress);
        
        LOG.info("finish index building related ports initialization");
	}
	
	/**
	 * to launch the three top-level threads for this coordinator. 
	 */
	public void startService() {
		//(1) query request acceptor and distributor. this is to communicate with the partitioners.  
		QueryRequestAcceptorDistributorThread thread1 = new QueryRequestAcceptorDistributorThread (this.context, this.privateIPAddress);
		
		//(2) query result distribution back to the client that submits the query to the coordinator 
		QueryResultDistributorThread thread2= new QueryResultDistributorThread (this.context, this.privateIPAddress);
		
		//(3) aggregator coordination thread 
		SinkAdaptorForAbandonmentCommand commandAdaptor = 
				           new SinkAdaptorForAbandonmentCommandImpl.ZeroMQBasedSinkAdaptorForAbandonmentCommandImpl (this.context);
		SinkAdaptorForFinalQueryResult queryResultAdaptor = 
				           new SinkAdaptorForFinalQueryResultImpl.ZeroMQBasedSinkAdaptorForFinalQueryResultImpl(this.context);
		
		this.toplevelThreadsManaged.add(thread1);
		LOG.info("QueryRequestAcceptorDistributor thread gets created....");
		this.toplevelThreadsManaged.add(thread2);
		LOG.info("QueryResultDistributor thread gets created....");
		
		QueryProcessingCoordinatorThread thread3 =  new QueryProcessingCoordinatorThread (this.context, this.privateIPAddress, 
				  this.numberOfPartitions, 
	              this.parameters, 
	              chosenSearch, 
	              commandAdaptor,
	              queryResultAdaptor);
	              
	
		this.toplevelThreadsManaged.add(thread3);
		LOG.info("QueryProcessingCoordinator thread gets created....");
		
		
		//Finally, start them. all will then get blocked until the requests come. 
		thread1.start();
		LOG.info("QueryRequestAcceptorDistributor thread gets started....");
		thread2.start();
		LOG.info("QueryResultDistributor thread gets started....");
		
		//before we launch the query processing. make sure that we have synchronized with all of the partition-related processes already.
		//NOTE: to do this step correctly, the coordinator will have to be started before all of the partitioners are started. 
		this.synchronizeWithAllPartitions();
		//then do the index building for all of the partitioners in the coordinated way. 
		//if it is simulated, then no index building (the most expensive operation at each partitioner) is needed.
		//we only need indexing for LSH search....
		 //NOTE: I choose even for Naive search, we will go ahead to do the dummy index building, as we will need the synchronization barrier before 
		 //moving to wait for the search (loading of large amount of partitioner data into memory on the last step still takes some time.
		 
		if (!simulated) {
		     this.scheduleIndexBuildingForAllPartitions();
		}
		 
		thread3.start();
		LOG.info("QueryProcessingCoordinator thread gets started....");
	}
	

	@Override
	public boolean scheduleIndexBuildingForAllPartitions() {
	   
		//NOTE that the the initial first batch of the index builders;
		//(2) then in the loop, received the ones that are done, then publish the next batch of the index builders;
        //    the loop is to get the scheduling status from the notification port, and then send out the next scheduled indexing command via the 
        //    publication port.
        int schedulingUnitDone= 0;
        while (schedulingUnitDone < this.numberOfPartitions) {
        	byte[] response = indexBuildingNotifier.recv(0);
        	//dummy reply back to the partitioner, as otherwise, it seems that the ZeroMQ has the difficulty for state transition for REQ/REP
        	String ack = "Hello";
            LOG.info("Sending ack on partitioner's notification of index building (already done):" + ack);
            indexBuildingNotifier.send(ack.getBytes(), 0);
            
        	//unmarshalling the status information 
        	CoordinatedIndexBuilding.Status statusInfo = new CoordinatedIndexBuilding.Status();
        	try {
	   	        ByteArrayInputStream in = new ByteArrayInputStream(response);
		   	    DataInputStream dataIn = new DataInputStream(in);
		   	        
		   	    statusInfo.readFields(dataIn);
		   	    dataIn.close();
	   	    }
	   	    catch (Exception ex) {
	   	        LOG.error("fails to receive/reconstruct the scheduling status information received from a partitioenr.", ex);
	   	    }
	   	    
        	
        	LOG.info("receive status info (as part of indexing building result from private ip: "
        	                            + statusInfo.getSchedulingUnit().getPrivateIPAddress() + " and process id: " 
        			                    + statusInfo.getSchedulingUnit().getProcessId());
            //then update the status information.
        	if (statusInfo.getSchedulingUnit()!=null) {
        	   schedulingUnitDone ++;

        	   this.indexingScheduler.updatePartitionerWorkingStatusOnNumaNode(statusInfo.getSchedulingUnit(), statusInfo.getStatus());
        	   String ipAddress = statusInfo.getSchedulingUnit().getPrivateIPAddress();
        	   CoordinatedIndexBuilding.Command nextCommand = this.indexingScheduler.getNextPartitionerForWorkOnNumaNode(ipAddress);
        	   if (nextCommand !=null) {
        		   LOG.info(" the NUMA node with private IP address: " + ipAddress  + " has the next command to send to its partitioner "
        				    + " with process id: " + nextCommand.getSchedulingUnits().get(0).getProcessId());
	        	   //marshall this command, 
				   //turn the status to be bytes
				   ByteArrayOutputStream out = new ByteArrayOutputStream();
			   	   DataOutputStream dataOut = new DataOutputStream (out);
			   	   try {
			   		  nextCommand.write(dataOut);
			   	      dataOut.close();
			   	   }
			   	   catch(IOException ex) {
			   	      LOG.error ("fails to serialize the partition's scheduling unit's command information", ex);
			   	   }
			   	    
			   	   byte[] message = out.toByteArray();
			   	   boolean status = indexingCoordinationPublisher.send(message, 0);
			   	   if (!status) {
			   		   LOG.error("fails to send the next scheduling command to the partitioners...");
			   	   }
        	   }
        	   else {
        		   LOG.info(" **the NUMA node with private IP address: " + ipAddress  + " has all of its partitioners done with indexing");
        	   }
        	}
        	
        	
        }
        
		//(4) until all index builder are done, when it reaches this point. 
		boolean result = true;
		return result;
	}
	/**
	 * The corresponding method for synchronization in each partition is implemented in acceptQueriesAndCommands() in the class of 
	 * QueryCommandAcceptor.
	 * 
	 */
	@Override
	public void synchronizeWithAllPartitions() {
		
        
        //send the empty message out, and then wait for the response. the response will be received when the number of 
        //partitions have been reached. 
        int partitionerCount = 0;
        
        while (partitionerCount < this.numberOfPartitions)
        {
            byte[] response = synchronizationBarrier1.recv(0);
            //we expect this is the actual runtime information that we expect to receive for each partition. 
            ProcessingUnitRuntimeInformation partitionerRuntimeInfo =  getPartitionerRuntimeInformation(response);
            //push this to the list that we will need this later for visualization and scheduling.
            this.runtimeInformationFromPartitioners.add(partitionerRuntimeInfo);
            LOG.info("Received for hand-shaking with runtime information for partitioner: "  + partitionerRuntimeInfo.getPartitionNumber()
            		 + " with process id: " + partitionerRuntimeInfo.getProcessId()
            		 + " with machine IP address: " + partitionerRuntimeInfo.getMachineIPAddress()
            		 + " with private IP address: " + partitionerRuntimeInfo.getPrivateIPAddress());
          
            
            String ack = "Hello";
            LOG.info("Sending Hello for hand-shaking" + ack);
            synchronizationBarrier1.send(ack.getBytes(), 0);
            partitionerCount++;
        }
        
        this.indexingScheduler = new IndexBuildingScheduler(this.runtimeInformationFromPartitioners);
        CoordinatedIndexBuilding.Command initialCommand = this.indexingScheduler.getFirstPartitionersForWorkOnNumaNodes();
        		
        {
        	//prepare the scheduling command unmarshallig.
            byte[] message = null;
			{
        		ByteArrayOutputStream out = new ByteArrayOutputStream();
	   	    	DataOutputStream dataOut = new DataOutputStream (out);
	   	    	try {
	   	    		initialCommand.write(dataOut);
	   	    	    dataOut.close();
	   	    	}
	   	    	catch(IOException ex) {
	   	    		LOG.error ("fails to serialize the aggegrator's initial scheduling command information", ex);
	   	    	}
	   	    
	   	    	
	   	    	message = out.toByteArray();
        	}
        	
			synchronizationBarrier2.send(message, 0);
            LOG.info("the coordinator has sent to all partitioners the initial scheduling command to proceed");
        }
        
        LOG.info("the coordinator has found all of the partitioners with number:  " + this.numberOfPartitions
		          + " initial synchronization is done and initial scheduling units for indexing have been distributed.");
        //let's have the initial command logged. this is not on the critical path of search related computation.
        {
        	LOG.info("the initial command for scheduling is  the following:");
        	List<SchedulingUnit> schedulingUnits= initialCommand.getSchedulingUnits();
        	int counter=0;
        	for (SchedulingUnit su: schedulingUnits) {
        		LOG.info("the " + counter + "-th scheduling unit has private IP adddress: " 
        	                   + su.getPrivateIPAddress() + " and process id: " + su.getProcessId() );
        		counter++;
        	}
        }
       
	}
	
    private ProcessingUnitRuntimeInformation getPartitionerRuntimeInformation(byte[] response) {
    	   ProcessingUnitRuntimeInformation information  = new ProcessingUnitRuntimeInformation();
 	       try {
 	            ByteArrayInputStream in = new ByteArrayInputStream(response);
	   	        DataInputStream dataIn = new DataInputStream(in);
	   	        
	   	        information.readFields(dataIn);
	   	        dataIn.close();
 	        
 	       }
 	       catch (Exception ex) {
 	        	LOG.error("fails to receive/reconstruct the run time information", ex);
 	       }
 	        
 	       return information;
    }
    
    
	public void shutdownService() {
		//to be designed such that the involved threads will be shutdown gracefully by some signal
		throw new UnsupportedOperationException ("the method is not implemented for the simulated class");
	}

	/**
	 * we will need to define more finer progress report first.
	 */
	@Override
	public TimeSeriesSearchPhase getCurrentPhase() {
		throw new UnsupportedOperationException ("the method is not implemented for the simulated class");
	}

	
	/**
	 * the thread that handles the query request acceptance and then redistribute it. 
	 *
	 */
	public static class QueryRequestAcceptorDistributorThread extends Thread {
		 private ZMQ.Context context;
		 private String privateIPAddress;
		 
		 public QueryRequestAcceptorDistributorThread (ZMQ.Context context, String privateIPAddress){
			 this.context = context; 
			 this.privateIPAddress = privateIPAddress;
		 }
		
		 public void run()  {
			 LOG.info("query request acceptor thread starting....");
			 QueryRequestAcceptorDistributorImpl impl =
					   new QueryRequestAcceptorDistributorImpl (this.context, this.privateIPAddress);
			 impl.acceptAndPublishTimeSeriesSearchRequest();
		 }
		
	}
	
	/**
	 * The thread that handles the final query result distribution. 
	 *
	 */
	public static class QueryResultDistributorThread extends Thread {
		private ZMQ.Context context;
		private String privateIPAddress;
		 
		public  QueryResultDistributorThread (ZMQ.Context context, String privateIPAddress) {
			this.context = context;
			this.privateIPAddress = privateIPAddress;
		}
		
		public void run()  {
			 LOG.info("query result distributor thread starting");
			 QueryResultDistributorImpl impl = new QueryResultDistributorImpl  (this.context, this.privateIPAddress);
			 impl.acceptAndPublishFinalSearchRequest();
		}
				
	}
	
	/**
	 * The master thread that handles query processing coordinator at the aggegrator. It will launch its own worker thread pool.
	 *
	 *
	 */
	public static class QueryProcessingCoordinatorThread extends Thread {
		private ZMQ.Context context;
		private String privateIPAddress;
		private int totalNumberOfPartitions;
		private LSHIndexConfigurationParameters parameters;
		private SinkAdaptorForAbandonmentCommand commandAdaptor;
		private SinkAdaptorForFinalQueryResult queryResultAdaptor;
		private QueryProcessingCoordinatorImpl impl; 
	 
		
		public QueryProcessingCoordinatorThread (ZMQ.Context context, String privateIPAddress, int totalNumberOfPartitions, 
				              LSHIndexConfigurationParameters parameters, 
				              SupportedSearchCategory.SupportedSearch chosenSearch, 
				              SinkAdaptorForAbandonmentCommand commandAdaptor,
				              SinkAdaptorForFinalQueryResult queryResultAdaptor){
			this.context = context;
			this.privateIPAddress = privateIPAddress;
			this.totalNumberOfPartitions = totalNumberOfPartitions;
			this.parameters = parameters;
			this.commandAdaptor = commandAdaptor;
			this.queryResultAdaptor = queryResultAdaptor;
			this.impl = new QueryProcessingCoordinatorImpl (this.context, this.privateIPAddress, 
					  this.totalNumberOfPartitions, 
		              this.parameters, chosenSearch, this.commandAdaptor, this.queryResultAdaptor);
		}
		
		public void run() {
			LOG.info("query processing coordinator thread starting");
			this.impl.startProcessing();
		}
	}
	
	
	/**
	 * The helper class to help combine the intermediate search result from the partition with the corresponding R value. 
	 * @author junli
	 *
	 */
	public static class CombinedIntermediateQueryResultsWithR {
		public TimeSeriesSearch.IntermediateQuerySearchResult resultsFromPartitions[]; 
		public int currentRvalue;
		
		public CombinedIntermediateQueryResultsWithR (int totalNumberOfPartitions, int rValue) {
			if (LOG.isDebugEnabled()) {
				LOG.debug("in CombinedIntermediateQueryResultsWithR construction, total number of partitions specified: " + totalNumberOfPartitions);
			}
			this.resultsFromPartitions = new  TimeSeriesSearch.IntermediateQuerySearchResult[totalNumberOfPartitions];
			this.currentRvalue=rValue;
		}
		
		/**
		 * to free the received intermediate results gathered from all of the partitions. 
		 */
		public void clear() {
			 //at the construction time, we have the array to be initialized with the total number of the partitions, but 
			 //each of the entries can all be null, for some big R. 
			 if ((this.resultsFromPartitions != null) && (this.resultsFromPartitions.length > 0)) {
				 for (int i=0; i<this.resultsFromPartitions.length;i++) {
					 TimeSeriesSearch.IntermediateQuerySearchResult result = this.resultsFromPartitions[i];
					 //each partition should not be NULL, as otherwise, we have not received all of the contribution in terms of the partiton-contributed
					 //intermediate results for a particular R.
					 if (LOG.isDebugEnabled()) {
						    if (result == null) {
							    LOG.debug("in CombinedIntermediateQueryResultsWithR, when i=" + i + " R value = " + this.currentRvalue  + " the intermediate result is NULL");
						    }
						    else {
						    	LOG.debug("in CombinedIntermediateQueryResultsWithR, when i=" + i +  " R value = " + this.currentRvalue  + " the intermediate result is Not NULL");
						    }
					 }
					 //thus the array element itself can be null.
					 if (result!=null)
					 {
					   result.clearWithNullification(); //deep cleaning.
					   this.resultsFromPartitions[i] = null;
					 }
				 }
			 }
			 
			 this.resultsFromPartitions = null;
		}
	}
	
	/**
	 * NOTE: to check what is the result, is that ascending or descending, in terms of the distance.
	 * use it to do the sorting based on the distance result. 
	 *
	 */
	public static class CombinedSearchResultWithPartitionAndR implements Comparator <CombinedSearchResultWithPartitionAndR >{
		  public int Rvalue; //all of the objects to be compared should share the same R value.
		  public TimeSeriesSearch.SearchResult result;
		  public int partitionId; 
		  
		  public  CombinedSearchResultWithPartitionAndR() {
			  //default constructor.
		  }
		  
		  public CombinedSearchResultWithPartitionAndR (int Rvalue,TimeSeriesSearch.SearchResult result, int partitionId) {
			  this.Rvalue = Rvalue;
			  this.result = result;
			  this.partitionId = partitionId;
		  }
		  
		  @Override
		  public int compare(CombinedSearchResultWithPartitionAndR o1, CombinedSearchResultWithPartitionAndR o2) {
				if  ((o1 == null ) || (o2 ==null) ) {
				     return -1;
				}
				else if ( (o1.result.id == o2.result.id) 
						  && (o1.result.offset == o2.result.offset)
						  && (o1.partitionId == o2.partitionId)){
					return 0;
				}
				else {
					//return ((o1.result.distance < o2.result.distance)? -1  : 1);
					//NOTE: it could be the situation that the distance actually has the exact match, and does introduce
					//illegal argument exception.
					return (Float.compare(o1.result.distance, o2.result.distance));
				}
		 }
		 
	}
	
	/**
	 * The data structure that the aggregator will need to keep, in order to produce the final query result
	 * and also the abandonment command. Each tracker is assigned to a thread that is doing the aggregation on the intermediate 
	 * results that fall into the same partition bags (hash partitioned). 
	 *
	 */
	public static class IntermediateResultAggregationTracker {
		//for each R value, we will keep track of how many partitions that results have been reported.
		private int numberOfPartitionsWithResultsCommunicated[]; 
		//for some R, such as R0, it may contain less than top-K value for evaluation. 
		private boolean skipForRvalueEvaluation[]; 
		
		//for each R value, we will have a partition list.
		private List<CombinedIntermediateQueryResultsWithR> resultsFromPartitions; 
		
		private String searchId; 
		private int maximumRvalue; 
		private int totalNumberOfPartitions; 
		private int topKNumber; 
		
		private boolean isDisabled;
		//we record the time stamp when it is disabled. and we leave say 3 minutes, before the queue has the corresponding entry emptied. 
		private long disableTimeStamp; 
		
		public IntermediateResultAggregationTracker (String searchId, int maximumRvalue, int totalNumberOfPartitions, int topKNumber) {
			this.isDisabled= false;  
			this.disableTimeStamp = 0;
			this.searchId = searchId;
			
			this.maximumRvalue = maximumRvalue;
			this.totalNumberOfPartitions = totalNumberOfPartitions; 
			this.topKNumber =  topKNumber;
			
			this.numberOfPartitionsWithResultsCommunicated = new int[maximumRvalue];
		 
			this.resultsFromPartitions = new ArrayList<CombinedIntermediateQueryResultsWithR> (this.maximumRvalue);
			this.skipForRvalueEvaluation = new boolean[this.maximumRvalue];
			//to do the initialization. we intializ such that for each R value, we would except to have the combined-intermediate-query-result-with-R.
			//but in reality, we get enough result, we will stop and when later doing the clean-up, some big R will have the corresponding
			//combined-inter-mediate-query-results with empty internal partition-based contributed intermediate results for each partition. 
			for (int i=0; i<this.maximumRvalue;i++){
				this.numberOfPartitionsWithResultsCommunicated [i]=0;
				this.skipForRvalueEvaluation[i] = false;
				CombinedIntermediateQueryResultsWithR val = new CombinedIntermediateQueryResultsWithR(this.totalNumberOfPartitions, i);
				this.resultsFromPartitions.add(val);
			}
		}
		
		
		public boolean isDisabled() {
			//this will be true, when the corresponding search query has already sent out the search result to the client, and thus there
			//is no need for the search tracker for this query. 
			return this.isDisabled; 
		}
		
		public long getDisableTimeStamp() {
			return this.disableTimeStamp;
		}
		
		public void addIntermediateResult (TimeSeriesSearch.IntermediateQuerySearchResult result) {
			int Rvalue = result.getAssociatedRvalue();
			//we have to make sure that the partition number received is from 0, 1, .... (totalNumberOfPartitions-1). 
			int partition = result.getPartitionId();
			
			this.numberOfPartitionsWithResultsCommunicated[Rvalue] = this.numberOfPartitionsWithResultsCommunicated[Rvalue] + 1; 
			CombinedIntermediateQueryResultsWithR val = this.resultsFromPartitions.get(Rvalue);
			//val's rValue has been set already. 
			if (LOG.isDebugEnabled()) {
				if (val == null) {
					//should this never comes, when it is running correctly?
				    LOG.debug("addIntermediateResult, partition number specified is: " + partition + " and R value: " + Rvalue  + " and val is NULL ");
				}
				else {
					LOG.debug("addIntermediateResult, partition number specified is: " + partition + " and R value: " + Rvalue  + " and val is not NULL ");
				}
			}
			
			val.resultsFromPartitions[partition] = result;
		}
		
		/**
		 * to evaluate the R value that is the one that is just add to the tracker. 
		 * 
		 * @return final query result, if it is available; if no combined query result, then we still return the NULL final resultt. 
		 */
		public TimeSeriesSearch.FinalQueryResult evaluate(int rValueIndex) {
			TimeSeriesSearch.FinalQueryResult finalResult = null;
		    //by the time we get to this method, the corresponding intermediate result has been already added to the e
			if (this.numberOfPartitionsWithResultsCommunicated[rValueIndex] == this.totalNumberOfPartitions) {
				
				if (LOG.isDebugEnabled()) {
					LOG.debug("IntermediateResultAggregationTracker has all intermediate results from all partitions " + " with R value: " + rValueIndex 
							+ " to conduct evaluation at thread: " + Thread.currentThread().getId() + " at time stamp: " + System.currentTimeMillis());
				}
				
				
				//all results have been obtained, including the intermediate result that does not have any values at all. 
				int totalCount = 0;
				CombinedIntermediateQueryResultsWithR resultsFromPartitions = this.resultsFromPartitions.get(rValueIndex);
				for (int i=0; i<this.totalNumberOfPartitions;i++) {
					List<TimeSeriesSearch.SearchResult>  searchResults = resultsFromPartitions.resultsFromPartitions[i].getSearchResults();
					if ( (searchResults !=null) && (searchResults.size() >0) ){
						totalCount = totalCount + searchResults.size();
					}
				}
				
				if (totalCount <this.topKNumber) {
					//we will decide whether this is the last R, if so, just package whatever we have so far and send to the client. so that we gurantee
					//that we will have some returns. 
					if (rValueIndex < maximumRvalue-1) {
						//we have done all of the partitions for this particular R, and still, no enough number of the non-zero search results 
						//to contribute to the top-K. 
						this.skipForRvalueEvaluation[rValueIndex] = true; 
						
						//to clean up the current result
						resultsFromPartitions.clear();
					}
					else{
						//we are at last R value now. if total count is 0, then we will return null.
						if (totalCount !=0) {
							if (LOG.isDebugEnabled()) {
								LOG.debug("IntermediateResultAggregationTracker have final result to be PARTIAL for search id: " + searchId  + " with R value: "  + rValueIndex);
							}
							//package whatever you have....
							//we now can evaluate the top-K results, by sorting it first. 
							CombinedSearchResultWithPartitionAndR sortedDistances[] = new CombinedSearchResultWithPartitionAndR[totalCount];
							int count=0;
							for (int i=0; i<totalNumberOfPartitions;i++) {
								List<TimeSeriesSearch.SearchResult>  searchResults = resultsFromPartitions.resultsFromPartitions[i].getSearchResults();
								if ((searchResults!=null) && (searchResults.size() > 0)){
									for (TimeSeriesSearch.SearchResult sr: searchResults) {
										CombinedSearchResultWithPartitionAndR wrappedVal = new  CombinedSearchResultWithPartitionAndR(rValueIndex, sr, i);
										sortedDistances[count] = wrappedVal;
										count++;
									}
								}
							}
							
						    Arrays.sort(sortedDistances, new CombinedSearchResultWithPartitionAndR());
						    
						    List<SimpleEntry<Integer, TimeSeriesSearch.SearchResult>> identifiedResults  = new ArrayList<SimpleEntry<Integer, TimeSeriesSearch.SearchResult>>();
						    for (int k=0; k<totalCount;k++) {
						    	CombinedSearchResultWithPartitionAndR wrappedVal = sortedDistances[k];
						    	SimpleEntry<Integer, TimeSeriesSearch.SearchResult> identifiedElement  =
						    			new SimpleEntry<Integer, TimeSeriesSearch.SearchResult> (new Integer (wrappedVal.partitionId), wrappedVal.result);
						    	identifiedResults.add(identifiedElement);
						    }
							
						    finalResult = new TimeSeriesSearch.FinalQueryResult  (this.searchId, identifiedResults);
						}
						else {
							//empty, we still want to get empty result
							//we will have to send even the NULL value back to the client.
							finalResult =  new TimeSeriesSearch.FinalQueryResult (searchId, null);
							if (LOG.isDebugEnabled()) {
								LOG.debug("IntermediateResultAggregationTracker have final result to be EMPTY (NOT NULL) for search id: " + searchId  + " with R value: "  + rValueIndex);
							}
							
						}
					}
				}
				else {
					//we now can evaluate the top-K results, by sorting it first. 
					CombinedSearchResultWithPartitionAndR sortedDistances[] = new CombinedSearchResultWithPartitionAndR[totalCount];
					int count=0;
					for (int i=0; i<totalNumberOfPartitions;i++) {
						List<TimeSeriesSearch.SearchResult>  searchResults = resultsFromPartitions.resultsFromPartitions[i].getSearchResults();
						if ((searchResults!=null) && (searchResults.size() > 0)){
							for (TimeSeriesSearch.SearchResult sr: searchResults) {
								CombinedSearchResultWithPartitionAndR wrappedVal = new  CombinedSearchResultWithPartitionAndR(rValueIndex, sr, i);
								sortedDistances[count] = wrappedVal;
								count++;
							}
						}
					}
					
				    Arrays.sort(sortedDistances, new CombinedSearchResultWithPartitionAndR());
				    
				    //NOTE: to check whether this will produce the ascending array with the distance.
				    //if so, pick the first top-k
				    List<SimpleEntry<Integer, TimeSeriesSearch.SearchResult>> identifiedResults  = new ArrayList<SimpleEntry<Integer, TimeSeriesSearch.SearchResult>>();
				    for (int k=0; k<this.topKNumber;k++) {
				    	CombinedSearchResultWithPartitionAndR wrappedVal = sortedDistances[k];
				    	SimpleEntry<Integer, TimeSeriesSearch.SearchResult> identifiedElement  =
				    			new SimpleEntry<Integer, TimeSeriesSearch.SearchResult> (new Integer (wrappedVal.partitionId), wrappedVal.result);
				    	identifiedResults.add(identifiedElement);
				    }
					
				    finalResult = new TimeSeriesSearch.FinalQueryResult  (this.searchId, identifiedResults);
				    if (LOG.isDebugEnabled()) {
						LOG.debug("IntermediateResultAggregationTracker have final result to be FULL for search id: " + searchId  + " with R value: "  + rValueIndex);
					}
				    
				} //the immediate scope
			} //the condition when all of the partitions at this R value have been accumulated 
		 
			
			return finalResult;
		} //the method body
		
	    /**
	     * to clear up 	
	     */
  		public void clear() {
  			this.isDisabled = true;
  			this.disableTimeStamp = System.currentTimeMillis();
  			
			this.numberOfPartitionsWithResultsCommunicated = null;
			this.skipForRvalueEvaluation = null;
			
			//the size of this.resultsFromPartitions is maximum R. and we may not advance to some big R before we declare that we are done with
			//the search. 
			for (CombinedIntermediateQueryResultsWithR result: this.resultsFromPartitions) {
				result.clear();
			}
			
			//then clear the entire map.
			this.resultsFromPartitions.clear();
		}
	}
	
	/**
	 * 
	 * The core processing unit for the aggregator/coordinator. 
	 *
	 */
	public static class AggregationRelatedProcessor {
		private LSHIndexConfigurationParameters configurationParameters;
		private BlockingQueue <TimeSeriesSearch.IntermediateQuerySearchResult> processingQueue;
		private  SinkAdaptorForAbandonmentCommand commandAdaptor;
		private  SinkAdaptorForFinalQueryResult queryResultAdaptor;
		private  int logicalThreadId; 
		private  int totalNumberOfPartitions;
		//wait for the number of the queries in the tracker queue reaches to the following threshold.
		private int waitNumberOfQueriesBeforeCleanUpTracker; 
		
		//the key is the search id, and the value is the tracker.
		private HashMap<String, IntermediateResultAggregationTracker> aggregationTracker; 
		
		private static final int WAIT_NUMBER_OF_QUERIES_BEFORE_CLEAN_UP=100; //100 queries.
		private static final int WAIT_TIME_TO_DO_TRACKER_TABLE_CLEAN_UP = 1000*60*3 ; //3 minutes.
		
		//the chosen search category
		private SupportedSearchCategory.SupportedSearch chosenSearch;
		
		public AggregationRelatedProcessor (LSHIndexConfigurationParameters configurationParameters, 
				                                      int threadId, 
    			                                      BlockingQueue <TimeSeriesSearch.IntermediateQuerySearchResult> processingQueue, 
    			                                      SinkAdaptorForAbandonmentCommand commandAdaptor, 
    			                                      SinkAdaptorForFinalQueryResult queryResultAdaptor,
    			                                      int totalNumberOfPartitions,
    			                                      SupportedSearchCategory.SupportedSearch chosenSearch) {
			this.configurationParameters = configurationParameters;
			this.totalNumberOfPartitions = totalNumberOfPartitions;
			this.logicalThreadId = threadId;
			this.processingQueue = processingQueue;
			this.commandAdaptor = commandAdaptor;
			this.queryResultAdaptor = queryResultAdaptor;
			
			/* we do not know at this time, how many distinct queries are out there not finished. So we can not have the initial capacity.
			 */
			this.aggregationTracker = new HashMap<String, IntermediateResultAggregationTracker> ();
			
			this.waitNumberOfQueriesBeforeCleanUpTracker= 
					 this.configurationParameters.getWaitNumberOfQueriesBeforeCleanUpTracker(WAIT_NUMBER_OF_QUERIES_BEFORE_CLEAN_UP);
			
			this.chosenSearch=chosenSearch;
		}
		
		public void init() {
			LOG.info("AggregationRelatedProcessor is conducting init for adaptors at thread: " + Thread.currentThread().getId());
			this.commandAdaptor.init();
			this.queryResultAdaptor.init();
		}
		
		/**
		 * this will be done by the QueryProcessingCoordinatorImpl, this method is called from the other thread.  
		 * 
		 * @param intermediateResult
		 */
		public void assignIntermediateResult(TimeSeriesSearch.IntermediateQuerySearchResult intermediateResult) {
			if (LOG.isDebugEnabled()) {
				LOG.debug("to push to the intermediate processing queue with size: " + this.processingQueue.size() + " with search id: " + intermediateResult.getQuerySearchId() + " for R value: " +
			                 intermediateResult.getAssociatedRvalue() + " with partition id: " + intermediateResult.getPartitionId() +
			                 " at the thread: " + Thread.currentThread().getId());
			}
			//the query goes to the tail of the queue. 
    		try {
    			this.processingQueue.put(intermediateResult);
    		}
    		catch (Exception ex) {
    			LOG.error("worker thread: " + this.logicalThreadId
    					+ " for concurrent aggregatio of intermediate results experiences interrupted exception: ", ex);
    			//we simply ignore this  and continue. 
    		}
		}
		
		 
		/**
		 * This method is invoked in  the asynchronous call class AggregationCoordinatorWorkAsynService. 
		 * it will pull out from the blocking queue, each item at the time, and then do processing. If there is no item in the queue, the thread will
		 * be blocked at this call. 
		 */
		public void conductAggregationOnQueries () {
			LOG.info("AggregationRelatedProcessor starts to conduct query aggregations at thread: " + Thread.currentThread().getId());
			while (true) {
				if (LOG.isDebugEnabled()) {
					LOG.debug("take the intermediate result from thread processor-specific processing queue with size: " + this.processingQueue.size()  +
				                 " at the thread: " + Thread.currentThread().getId());
				}
				try {
					  //the following will be block if the queue is empty.
					  TimeSeriesSearch.IntermediateQuerySearchResult intermediateResult = this.processingQueue.take();
					  checkAndClearProcessedQueries();
					  conductAggregationOnQuery(intermediateResult);
				}
				catch (InterruptedException ex) {
					 LOG.error("fails to retrieve an intermediate result from the processing queue belong to thread: " + this.logicalThreadId, ex);
						//simply ignore it. 
				}
				//NOTE: the following is very important to capture all of the other exceptions during the thread execution loop!!
				catch (Exception ex) {
					LOG.error("fails to handle processing of the intermediate result from the processing queue belong to thread: " + this.logicalThreadId, ex);
				}
			}
		}
		
		 
		/**
		 * check with the number of the accumulated queries is larger than a threshold, if so, do some cleaninng.
		 */
		protected void checkAndClearProcessedQueries() {
		    int size = this.aggregationTracker.size();
		     
		    
		    if (size > this.waitNumberOfQueriesBeforeCleanUpTracker) {
		
		    	List<String> queriesToBeCleanedUp = new ArrayList<String>();
		    	 
		    	LOG.info ("AggregationRelatedProcessor is now clearing the aggregation tracker's internal data structure....");
				long currentTimeStamp = System.currentTimeMillis();
				    
		    	//do the scanning, and if the corresponding tracker has been disabled, remove it. 
		    	for (Map.Entry<String, IntermediateResultAggregationTracker > e: this.aggregationTracker.entrySet()) {
		    		IntermediateResultAggregationTracker tracker = e.getValue();
		    		
		    		//I need the following time to be elapsed, as it could be that after the disable of the tracker, there 
		    		//are still incoming intermediate results from some partitioning, because the abandonment command is sent out 
		    		//while the intermediate result is coming in.
		    		if ((tracker.isDisabled() && 
		    				        (tracker.getDisableTimeStamp()-currentTimeStamp) > WAIT_TIME_TO_DO_TRACKER_TABLE_CLEAN_UP)){
		    			queriesToBeCleanedUp.add(e.getKey());
		    		}
		    	}
		  
		        for (String trackerKey: queriesToBeCleanedUp) {
		    	   //at the time, the tracker's detailed internal data structure has been cleaned up already.
		    	    this.aggregationTracker.remove(trackerKey);
		        }
		    
		        queriesToBeCleanedUp.clear();
		    }
		}
		
		
		/**
		 * We will need to prevent when the final result has been distributed, but still, there is some incoming intermediate results that were
		 * sent in transit when the abandonment request has been sent out.
		 * 
		 * @param intermediateResult the received intermediate result from the partition.
		 */
		protected void conductAggregationOnQuery (TimeSeriesSearch.IntermediateQuerySearchResult intermediateResult) {
			if (LOG.isDebugEnabled()) {
				LOG.debug("AggregationRelatedProcessor receives the intermediate result with search id: " + intermediateResult.getQuerySearchId()
						+ " from partition: " + intermediateResult.getPartitionId() + " with R value: " + intermediateResult.getAssociatedRvalue()
						+ " at thread: " + Thread.currentThread().getId() + " at time stamp: " + System.currentTimeMillis());
			}
			String searchId = intermediateResult.getQuerySearchId();
			IntermediateResultAggregationTracker tracker = this.aggregationTracker.get(searchId);
			
			int maximumRvalue = this.configurationParameters.getRNumber();
			
			if (tracker == null) {
				
			    int topKNumber = this.configurationParameters.getTopK();
				tracker =  new  IntermediateResultAggregationTracker (searchId, maximumRvalue, this.totalNumberOfPartitions, topKNumber);
			    this.aggregationTracker.put(searchId, tracker);
			}
			
			if (tracker.isDisabled()) {
				return;
			}
			else {
				tracker.addIntermediateResult(intermediateResult);
				int rValueIndex =  intermediateResult.getAssociatedRvalue();
				if (LOG.isDebugEnabled()) {
					LOG.debug("AggregationRelatedProcessor is to evaluate tracker's at rValue: " + rValueIndex + " for search id: " + searchId);
				}
				TimeSeriesSearch.FinalQueryResult finalResult = tracker.evaluate(rValueIndex);
				//for no result, if we exhaust all R and no top-k, we will still produce some final result. 
				if (finalResult!=null){
					if (LOG.isDebugEnabled()) {
						LOG.debug("AggregationRelatedProcessor concludes final result for search id: " + finalResult.getQuerySearchId() + 
								 " with R value: " + intermediateResult.getAssociatedRvalue() );
					}
					//we do have the final result to be produced.
					this.queryResultAdaptor.bufferFinalQueryResult(finalResult);
					
					//we onlys end the early abandonment command when R is smaller than the last one. Otherwise, no need.
					if ( rValueIndex <(maximumRvalue-1)){
						//then send out the abandonment command. 
						int threadProcessorId = intermediateResult.getThreadProcessorId();
						int partitionId = intermediateResult.getPartitionId();
						TimeSeriesSearch.QuerySearchAbandonmentCommand command = 
								          new TimeSeriesSearch.QuerySearchAbandonmentCommand  (searchId, partitionId, threadProcessorId);
						if (LOG.isDebugEnabled()){
							LOG.debug("coordinator issues early abandonment command for search id: " + searchId + " partition id: " + partitionId
									 + " thread processor id: " + threadProcessorId + " at time stamp: " + System.currentTimeMillis());
						}
						
						if (this.chosenSearch==SupportedSearchCategory.SupportedSearch.LSH_SEARCH) {
						  //we do early abandonment only for LSH search, not for naive search.
						  this.commandAdaptor.bufferAbandonmentCommand(command);
						}
					}
					
					//finally clear up the space for this particular search, as we are done with it, and set the tracker is disabled. 
					tracker.clear();
					//but still the hash map is still keep growing, as more number of the search will be going into this place. 
					//so we will have to set a limit, to do some clean up, using the time stamp, say 5 minutes. 
					
				}
				
			}
			
		}
	}
	
	/**
	 * This QueryProcessingCoordinator has similarities compared to MultiSearchManagerImpl.
	 * 
	 * the data flow is:
	 * (1) accept the intermediate results from the partitioners. 
	 * (2) put the intermediate results into the separated non-blocking queues that are then handled by the thread pools. 
	 * (3) within each thread pool, for each query, decide whether the query result is now achieved at certain R.  If so, send out the abandonment command
	 * from the adaptor.  
	 *
	 */
	public static class QueryProcessingCoordinatorImpl implements CoordinatorMaster.QueryProcessingCoordinator {
		private ZMQ.Context context;
		private ZMQ.Socket intermediateResultAcceptor;
		private ZMQ.Socket abandonmentcommandDistributor;
		private ZMQ.Socket internalAbandonmentCommandDistributor;
		private String privateIPAddress;
		private  LSHIndexConfigurationParameters parameters;
		
		private int totalNumberOfPartitions;
		
		private SinkAdaptorForAbandonmentCommand abandonmentCommandAdaptor;
		private SinkAdaptorForFinalQueryResult finalQueryResultAdaptor;
		
		private List<BlockingQueue<TimeSeriesSearch.IntermediateQuerySearchResult>> queuedIntermediateResultsForWorkerThreads;
		
		private ExecutorService executor; 
		//to hold the thread-based processors 
	    private List<AggregationRelatedProcessor> threadProcessors;
	    
	    private SupportedSearchCategory.SupportedSearch chosenSearch;
	   
	    
		/**
		 * NOTE: this constructor will have to be done in a particular thread where the socket will be bound.
		 * @param context
		 * @param privateIPAddress
		 */
		public QueryProcessingCoordinatorImpl (ZMQ.Context ctxt, String privateIPAddress, int totalNumberOfPartitions, 
				              LSHIndexConfigurationParameters parameters, 
				              SupportedSearchCategory.SupportedSearch chosenSearch,
				              SinkAdaptorForAbandonmentCommand commandAdaptor,
				              SinkAdaptorForFinalQueryResult queryResultAdaptor) {
			this.context = ctxt;
			this.privateIPAddress = privateIPAddress;
			this.parameters = parameters;
			this.chosenSearch=chosenSearch;
		 
			this.totalNumberOfPartitions = totalNumberOfPartitions; 
			
			this.abandonmentCommandAdaptor = commandAdaptor;
			this.finalQueryResultAdaptor = queryResultAdaptor;
			
			//socket for intermediate results from the partitioner to the aggregator
			this.intermediateResultAcceptor = this.context.socket(ZMQ.PULL);
			String acceptorDestinationAddress = "tcp://" + this.privateIPAddress +":"  + 
			                                    new Integer (CoordinatorPortConfiguration.INTERMEDIATE_SEARCH_RESULT_REPORT_PORT);
			this.intermediateResultAcceptor.bind(acceptorDestinationAddress);

			//socket for abandonment command to the partitioner from the aggregator;
			this.abandonmentcommandDistributor = this.context.socket(ZMQ.PUB);
			String distributorDestinationAddress = "tcp://" + this.privateIPAddress +":"  + 
                                                       new Integer (CoordinatorPortConfiguration.QUERY_COMMAND_DISTRIBUTION_PORT);
			this.abandonmentcommandDistributor.bind(distributorDestinationAddress);
			
			//socket for abandonment command from the internal aggregator to the internal aggregator. 
			this.internalAbandonmentCommandDistributor=this.context.socket(ZMQ.PULL);
			String internalDestinationAddress = "inproc://"  + "abandonmentcmd.ipc";
			this.internalAbandonmentCommandDistributor.bind(internalDestinationAddress);
			
			//the following list will be populated during the launch time.
			int numberOfThreadsForAggregation = this.parameters.getNumberOfThreadsForConcurrentAggregation();
			//other for : internal processing queue, and executor service.s
			int sizeOfQueues = numberOfThreadsForAggregation;
			this.queuedIntermediateResultsForWorkerThreads = 
					         new ArrayList<BlockingQueue<TimeSeriesSearch.IntermediateQuerySearchResult>> (sizeOfQueues);
			for (int i=0; i<sizeOfQueues;i++){
				BlockingQueue<TimeSeriesSearch.IntermediateQuerySearchResult> element =
						 new LinkedBlockingQueue<TimeSeriesSearch.IntermediateQuerySearchResult>(); //initial capacity is 0
				this.queuedIntermediateResultsForWorkerThreads.add(element);
			}
			
		    this.executor = Executors.newFixedThreadPool(numberOfThreadsForAggregation); 
		    this.threadProcessors = new ArrayList<AggregationRelatedProcessor> (numberOfThreadsForAggregation);
		}

		public void startProcessing() {
			 //1. launch the worker thread pool.
			 LOG.info("in QueryProcessingCoordination, lauching the worker threads for concurrent aggregation of intermediate results... ");
			 this.launchWorkerThreads();
			 //2. start the accept loop
			 LOG.info("in QueryProcessingCoordination, ready to accept intermediate result and abandonment coomand...");
			 this.acceptIntermediateResultsAndDistributeAbandonmentCommands();
		}
		
		protected void launchWorkerThreads() {
			 CompletionService<AsynAggregationCoordinatorIntf.ConductAggregationResponse> ecs = 
	    			  new ExecutorCompletionService<AsynAggregationCoordinatorIntf.ConductAggregationResponse> (this.executor);
			 
			 AsynAggregationCoordinatorIntf.AggregationCoordinatorWorkAsynService  service =
			          new  AsynAggregationCoordinatorIntf.AggregationCoordinatorWorkAsynService();
		
			 //construct all of the requests that will then be assigned to each thread pool's worker thread.
			 List<AsynAggregationCoordinatorIntf.ConductAggregationRequest> requests = 
					                              new ArrayList<AsynAggregationCoordinatorIntf.ConductAggregationRequest>();
			 //the abandonment command communicator adaptor 
			  
			 //prepare all of the parameters necessary for each different worker thread. 
			 int numberOfWorkerThreadsForAggregation = this.parameters.getNumberOfThreadsForConcurrentAggregation();
			 for (int i=0; i<numberOfWorkerThreadsForAggregation; i++) {
				 int threadId = i;
				 AggregationRelatedProcessor processor = new AggregationRelatedProcessor (this.parameters, 
						 threadId, 
                         this.queuedIntermediateResultsForWorkerThreads.get(i), 
                         this.abandonmentCommandAdaptor, this.finalQueryResultAdaptor, this.totalNumberOfPartitions, this.chosenSearch);
						                                 
				 this.threadProcessors.add(processor);
				 AsynAggregationCoordinatorIntf.ConductAggregationRequest request =
						              new  AsynAggregationCoordinatorIntf.ConductAggregationRequest (processor); 
				    			  
				 requests.add(request);
			 }
			 
			 //now submit the collection of requests. 
		     Thread.yield();//so that we can have a temporary pause.
			
			 //then produce the asynchronous response 
			 List<Future<AsynAggregationCoordinatorIntf.ConductAggregationResponse>> responses = 
					                   new ArrayList<Future<AsynAggregationCoordinatorIntf.ConductAggregationResponse>>();
	         try {
			    for (AsynAggregationCoordinatorIntf.ConductAggregationRequest request : requests) {
	               responses.add(service.conductAggregationOnQueriesAsync(ecs, request));
	            }
	         }
	         catch (RuntimeException ex) {
	             LOG.fatal("launch worker threads for concurrent aggregation fails......", ex);
	             //we will produce the report and send to the coordinator as well.
	             return;
	         }
	       
	         //now we need to do the polling to retrieve the result and make progress.
	         LOG.info ("successfully launching worker threads for concurrent aggregation with number of worker threads: " + numberOfWorkerThreadsForAggregation);
		}
		
	 
		/**
		 * this will be assigned as an active thread to handle the processing, INCLUDING the constructor.
		 * NOTE: we need to separately test whether the inproc and the regular socket can work together in the polling loop.
		 */
		protected void acceptIntermediateResultsAndDistributeAbandonmentCommands() {
			 //  Initialize poll set
	        ZMQ.Poller items = new ZMQ.Poller (2);
	        items.register(this.intermediateResultAcceptor, ZMQ.Poller.POLLIN);
	        items.register(this.internalAbandonmentCommandDistributor, ZMQ.Poller.POLLIN);
	         

	        //  Process messages from both sockets
	        while (!Thread.currentThread ().isInterrupted ()) {
	            byte[] message = null;
	            items.poll();
	            if (items.pollin(0)) {
	               message = this.intermediateResultAcceptor.recv(0);
	               distributeIntermediateResult(message);
	            }
	            else if (items.pollin(1)){
	               message = this.internalAbandonmentCommandDistributor.recv(0);
	               distributeAbandonmentCommand(message);
	            }
	        }
		}
		
		/**
		 * Identify the destination blocking queue that is associated with the thread, based on the search id hash-code. 
		 * 
		 * @param message received from a partitioner.
		 */
		protected void distributeIntermediateResult(byte[] message) {
			//de-serialize the message first.
		    TimeSeriesSearch.IntermediateQuerySearchResult intermediateResult = new TimeSeriesSearch.IntermediateQuerySearchResult();
			//we are using polling here. 
			try {
			 
			  ByteArrayInputStream in = new ByteArrayInputStream(message);
			  DataInputStream dataIn = new DataInputStream(in);
			 
			  intermediateResult.readFields(dataIn);
			  dataIn.close();
			
			}
			catch (Exception ex) {
				LOG.error("fails to receive/reconstruct the intermediate result information", ex);
				return;
			}
			
			if (LOG.isDebugEnabled()) {
				int thePartitionId = intermediateResult.getPartitionId();
				int theAssociatedRValue= intermediateResult.getAssociatedRvalue();
				
			    LOG.debug ("the intermediate result information received from the paritioner: " + thePartitionId
			    		+ " associated R value: " + theAssociatedRValue
		                + " with query id: " + intermediateResult.getQuerySearchId()
				        + " thread processor id: " + intermediateResult.getThreadProcessorId()
				        + " number of search results is: " + intermediateResult.size()
				        + " at time stamp: " + System.currentTimeMillis());
				List<TimeSeriesSearch.SearchResult> results = intermediateResult.getSearchResults();
				if (results!=null) {
				    int totalNumber= results.size();
					for (int i=0; i<results.size();i++) {
						LOG.debug("=======search result (begin) for partitionId=" + thePartitionId + " Rvalue=" + theAssociatedRValue + " totalNumber=" +
					                        totalNumber + "=========");
						TimeSeriesSearch.SearchResult sr = results.get(i);
						LOG.debug("{id: " + sr.id + " offset: " + sr.offset +  " distance: " + sr.distance 
								    + "} with partition id: " + thePartitionId +  " R value:" + theAssociatedRValue + " total number expected: "  + totalNumber);
							
					}
					
					LOG.debug ("=======search result (end) for partition Id: " + thePartitionId + " R value:" + theAssociatedRValue
							        +  " total Number: "+  totalNumber + "=========");
				}
				else {
					LOG.debug("=======search result IS EMPTY for partitionId=" + thePartitionId + " Rvalue=" + theAssociatedRValue);
				}
			}
			
			String searchId = intermediateResult.getQuerySearchId(); 
			int hashCode = Math.abs(searchId.hashCode()); //it can be a negative integer...
	   	 	//take the reminder.
			int numberOfWorkerThreadsForAggregation = this.parameters.getNumberOfThreadsForConcurrentAggregation();
	   	 	int destinationThreadId = hashCode%numberOfWorkerThreadsForAggregation;
	   	 	//NOTE we may need to use the Processor's assignQuery, instead of directly putting into the queue.
	   	 
	   	    AggregationRelatedProcessor processor = this.threadProcessors.get(destinationThreadId);
	   	 
	   	 	//put(E): Inserts the specified element at the tail of this queue, waiting if necessary for space to become available
	   	 	if (LOG.isDebugEnabled()) {
	   		  LOG.debug("to push to the aggregation processing queue for logical thread id: " + destinationThreadId + " for search query: " + searchId);
	   	 	}
	   	 
	   	 	//the processor handles the exception already.
	   	 	processor.assignIntermediateResult(intermediateResult);
	   	 	
		}
		
		/**
		 * simply relay the message to the remote based publish/subscribe.
		 * @param message
		 */
		protected void distributeAbandonmentCommand (byte[] message) {
           this.abandonmentcommandDistributor.send(message, 0, message.length, 0); 
		}
		
		
	    public void shutdownQueryProcessing() {
			//need to be implemented later.
		}
	}
	
	/**
	 * to distribute the final query result from the aggregator processor, via the publish/subscribe channel, to the client who will subscribe it.
	 * 
	 *
	 */
	public static class  QueryResultDistributorImpl implements CoordinatorMaster.QueryResultDistributor {
		private ZMQ.Context context;
		private String privateIPAddress; 
		
		private ZMQ.Socket queryResultReceiverSocket; //to receive the final query result from the aggregator. 
		private ZMQ.Socket queryResultDistributorSocket; //to publish the final query result to the client
		
		public QueryResultDistributorImpl  (ZMQ.Context context, String privateIPAddress) {
			this.context = context;
			this.privateIPAddress = privateIPAddress;
			
			//two sockets, the PUSH/PULL from the aggegator results 
			this.queryResultDistributorSocket = this.context.socket(ZMQ.PUB);
			String queryResultDistributorAddress = "tcp://" + this.privateIPAddress + ":"  
					+ (new Integer (CoordinatorPortConfiguration.QUERY_RESULT_DISTRIBUTION_PORT)).toString();
			this.queryResultDistributorSocket.bind(queryResultDistributorAddress);
			
			this.queryResultReceiverSocket = this.context.socket(ZMQ.PULL);
			String queryResultReceiverAddress="inproc://"  + "finalqueryresult.ipc";
			this.queryResultReceiverSocket.bind(queryResultReceiverAddress);
		}
		
		
		/**
		 * this will be call in the thread run, to accept the search request, and then publish the
		 * query to the partitioner.
		 */
		@Override
		public void acceptAndPublishFinalSearchRequest() {
			 //  Initialize poll set
	        ZMQ.Poller items = new ZMQ.Poller (1);
	        items.register(this.queryResultReceiverSocket, ZMQ.Poller.POLLIN);
	         

	        //  Process messages from both sockets
	        while (!Thread.currentThread ().isInterrupted ()) {
	            byte[] message;
	            items.poll();
	            if (items.pollin(0)) {
	               message = this.queryResultReceiverSocket.recv(0);
	               publish(message);
	            }
	             
	        }
	 
		}
		
		/**
		 * re-route the final search result back to the client.
		 * @param message
		 */
		private void publish(byte[] message) {
			  if (LOG.isDebugEnabled()) {
	              TimeSeriesSearch.FinalQueryResult queryResult = new TimeSeriesSearch.FinalQueryResult();
		          try {
		             ByteArrayInputStream in = new ByteArrayInputStream(message);
		   	         DataInputStream dataIn = new DataInputStream(in);
		   	        
		   	         queryResult.readFields(dataIn);
		   	         dataIn.close();
		        
		          }
		          catch (Exception ex) {
		        	 LOG.debug("fails to receive/reconstruct the query information", ex);
		          }
		        
		        
		          LOG.debug("the final query search result received at the QueryResultDistributor is: ");
		          LOG.debug("query search id (QueryResultDistributor): " + queryResult.getQuerySearchId());
		          List<SimpleEntry<Integer, TimeSeriesSearch.SearchResult>> results = queryResult.getSearchResults();
		          if ((results != null) && (results.size()>0)) {
			          for (SimpleEntry<Integer, TimeSeriesSearch.SearchResult> pair: results) {
			        	  int partitionId = pair.getKey().intValue();
			        	  TimeSeriesSearch.SearchResult sr = pair.getValue();
			        	  LOG.debug("search result with partition id (QueryResultDistributor): " + partitionId 
			        			          + " with id: " + sr.id + " with offset: " + sr.offset + " with distance: " + sr.distance);
			          }
		          }
		          else {
						LOG.debug ("******search result is empty!!************");
				  }
		          
		          LOG.debug ("proceed to distribute the final result to the client (QueryResultDistributor)");
			  }
			  
			  boolean status = this.queryResultDistributorSocket.send(message, 0, message.length, 0); 
			  
			  if (!status) {
				  LOG.error("fails to publish query to the partitioners");
			  }
			  
			  if (LOG.isDebugEnabled()) {
				  LOG.debug ("the satus to distribute the final result to the client (QueryResultDistributor) is: " + status);
			  }

		  }
	}
	
	
	/**
	 * this will be deployed as a worker thread.
	 *
	 */
	public static class QueryRequestAcceptorDistributorImpl implements CoordinatorMaster.QueryRequestAcceptorDistributor {
		private ZMQ.Context context;
		private String privateIPAddress; 
		private ZMQ.Socket queryRequestorSocket; 
		private ZMQ.Socket queryDistributorSocket;
		 
		
		public QueryRequestAcceptorDistributorImpl (ZMQ.Context context, String privateIPAddress) {
			this.context = context;
			this.privateIPAddress = privateIPAddress;
			
			//two sockets. the REQUEST/RESPONSE socket from the client 
			this.queryRequestorSocket = this.context.socket(ZMQ.REP);
			String requestorAddress = "tcp://" + this.privateIPAddress + ":" 
			                 + (new Integer (CoordinatorPortConfiguration.QUERY_REQUEST_ACCEPTOR_PORT)).toString();
			this.queryRequestorSocket.bind(requestorAddress);
			
			LOG.info("Query Request Acceptor binds to address (REP): " + requestorAddress);
			
			this.queryDistributorSocket = this.context.socket(ZMQ.PUB);
			String distributorAddress = "tcp://" + this.privateIPAddress + ":" 
	                                         + (new Integer (CoordinatorPortConfiguration.QUERY_DISTRIBUTION_PORT)).toString();
			this.queryDistributorSocket.bind(distributorAddress);
			
			LOG.info("Query Request Acceptor binds to address (PUB): "+ distributorAddress);
			
		}
		
		/**
		 * this will be call in the thread run, to accept the search request, and then publish the
		 * query to the partitioner.
		 */
		@Override
		public void acceptAndPublishTimeSeriesSearchRequest() {
	        //  Initialize poll set
	        ZMQ.Poller items = new ZMQ.Poller (1);
	        items.register(this.queryRequestorSocket, ZMQ.Poller.POLLIN);
	         
            LOG.info("Query Request Acceptor proceeds to wait for incoming query request....");
	        //  Process messages from both sockets
	        while (!Thread.currentThread ().isInterrupted ()) {
	            byte[] message;
	            items.poll();
	            if (items.pollin(0)) {
	               message = this.queryRequestorSocket.recv(0);
	               if (LOG.isDebugEnabled()) {
	            	   LOG.debug("Query Request Acceptor receives message (REP) with length: " + message.length);
	               }
	               //need to reply back for the OK message, as this is REP/REQ. 
	               // Send reply back to client
		           String reply = "OK";
		           this.queryRequestorSocket.send(reply.getBytes(), 0);
		           
		           if (LOG.isDebugEnabled()) {
	            	   LOG.debug("Query Request Acceptor sends ACK message (REP) back to the client");
	               }
		           
		           publish(message);
	            }
	             
	        }
	  
		}

	   /**
	    * basically to re-route the message to the publish/subscribe channel.
	    */
	   private void publish(byte[] message) {
		  if (LOG.isDebugEnabled()) {
              TimeSeriesSearch.SearchQuery query = new TimeSeriesSearch.SearchQuery();
	          try {
	             ByteArrayInputStream in = new ByteArrayInputStream(message);
	   	         DataInputStream dataIn = new DataInputStream(in);
	   	        
	   	         query.readFields(dataIn);
	   	         dataIn.close();
	        
	          }
	          catch (Exception ex) {
	        	 LOG.debug("fails to receive/reconstruct the query information", ex);
	          }
	       
	          LOG.debug("the query received at the query aggregator");
	          LOG.debug("query search id: " + query.getQuerySearchId());
	          LOG.debug("query pattern with size of: " + query.getQueryPattern().length);
		      for (int i=0; i<query.getQueryPattern().length;i++) {
		    	  LOG.debug("..." + query.getQueryPattern()[i]);
		      }
		      LOG.debug("top k number specification:" + query.getTopK());
		  }
		  
		  boolean status = this.queryDistributorSocket.send(message, 0, message.length, 0); 
		  if (!status) {
			  LOG.error("fails to publish query to the partitioners");
		  }

	  }
	}

	
	
}
