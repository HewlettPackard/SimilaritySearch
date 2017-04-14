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

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.lang.management.ManagementFactory;

import lsh.LSHinf;
import lshbased_lshstore.LSH_Hash;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.Reporter;
import org.zeromq.ZMQ;

import com.hp.hpl.palette.similarity.bridge.LSHManagerProxy;
import com.hp.hpl.palette.similarity.configuration.SupportedSearchCategory;
import com.hp.hpl.palette.similarity.coordinator.CoordinatorMaster;
import com.hp.hpl.palette.similarity.datamodel.ProcessingUnitRuntimeInformation;
import com.hp.hpl.palette.similarity.datamodel.CoordinatedIndexBuilding;
import com.hp.hpl.palette.similarity.worker.IndexBuilder.PartitionOfLTablesAtRForIndexBuilding;
import com.hp.hpl.palette.similarity.worker.QueryCommandAcceptor;

import com.hp.hpl.palette.similarity.job.TimeSeriesSearchCommunicator;
import org.apache.hadoop.mapred.JobConf;
import com.hp.hpl.palette.similarity.job.TimeSeriesDataLoader;
import com.hp.hpl.palette.similarity.configuration.CoordinatorPortConfiguration;

import  com.hp.hpl.palette.similarity.naivesearch.SampleQueriesCheckerImpl;

/**
 * The service that is hosted in each of the long-lived Map instances, or to be hosted as a standalone service, 
 * that receives request from the Coordinator Master, and deliver partition-based query results to the Coordinate Master. 
 * 
 *
 */
public class MapHostedService implements Partitioner {
	 //the context will have to be created once in the whole application;
     private ZMQ.Context context; 
     private Configuration configuration;
     private LSHIndexConfigurationParameters lshConfigurationParameters; 
     private static final int NUMBER_OF_IO_THREADS_PER_ZMQ_SOCKET= 1;
     
     //the child services;
     private DataLoader dataLoader;
     private IdleCommunicator idleCommunicator;
     private IndexBuilder indexBuilder;
     private MultiSearchManager searchManager;
     
     //the runtime information 
     private int partitionNumber; 
     private String publicIPAddress; 
     private int processId; 
     private String coordinatorMasterAddress; //the aggegrator's private IP address 
     private String privateIPAddress; //the private IP address allocated for this partition's data tracker.
     private CoordinatedIndexBuilding.Command initialSchedulingCommand;//received from the synchronization's final returns.
     
     private String dataFile;
     private String lshFile;
     
     private boolean simulated = false; 
     private SupportedSearchCategory.SupportedSearch chosenSearch;
     private boolean serviceStarted=false; //to indicate whether the whole service has been started or not.
     
     private static final Log LOG = LogFactory.getLog(MapHostedService.class.getName());
     
     /**
      * NOTE: by having the context to be created in this class, the creation of the class will always have to be at the top of the thread hierarchy, if 
	  * there is  a main thread that invokes his method and other threads under the same main thread, then all of the threads will have to share the same unique
	  * context in the entire threading hierarchy. 
	  * 
      * This is used for the non-map-reduce environment 
      * we will need to pass in the Hadoop configuration that is passed from the Map Reduce job
      * @param hdConfiguration the Hadoop configuration
      * @param datafile the time series data file to be loaded
      * @param lshFile  the lsh function parameters related file to be loaded.
      * @param partitionNumber the specified partition that will be handled by this Map instance.
      * @param simualted to specify whether we will use the simulated search process or not. if it is true, then 
      * data file and lsh file will be set as NULL.
      */
	 public MapHostedService (Configuration hdConfiguration, String dataFile, String lshFile, 
			             String coordinatorMasterAddress, String privateIPAddress, int partitionNumber,  
			             SupportedSearchCategory.SupportedSearch chosenSearch, 
			             boolean simulated) {
		 this.configuration=hdConfiguration;
		 this.dataFile = dataFile;
		 this.lshFile = lshFile;
		 
		 this.partitionNumber = partitionNumber;
		 this.coordinatorMasterAddress = coordinatorMasterAddress;
		 this.privateIPAddress = privateIPAddress;
		 this.simulated = simulated;
		 this.chosenSearch = chosenSearch;
		 //1 is for one single IO thread per socket.
		 this.context = ZMQ.context(NUMBER_OF_IO_THREADS_PER_ZMQ_SOCKET);  
		 
	 }
	 
	 public MapHostedService (Configuration hdConfiguration,  
             String coordinatorMasterAddress, String privateIPAddress, int partitionNumber,  boolean simulated) {
		this.configuration=hdConfiguration;
		this.dataFile = null;
		this.lshFile = null;
		
		this.partitionNumber = partitionNumber;
		this.coordinatorMasterAddress = coordinatorMasterAddress;
		this.privateIPAddress = privateIPAddress;
		this.simulated = simulated;
		//1 is for one single IO thread per socket.
		this.context = ZMQ.context(NUMBER_OF_IO_THREADS_PER_ZMQ_SOCKET);  

     }
	 
	 /**
	  * this method only used in the non-map-reduce environment.
	  * we make it such that the report runtime information will be done right after the process is launched,
	  * instead of waiting until the time series data loading/indexing is done.
	  * 
	  */
	 public void init() {
		 this.loadLSHConfigurationParameters();
		 //start the LSH Manager instance 
		 if (!simulated)
		 {
				 LSHManagerProxy  managerInstance = LSHManagerProxy.INSTANCE;
				 int R_num = this.lshConfigurationParameters.getRNumber();  //3; 
				 int L_num = this.lshConfigurationParameters.getLNumber(); //100;
				 //WARNING: this capacity should be made to become configurable. 
				 int capacityForEntriesInLTable = this.lshConfigurationParameters.getCapacityForEntriesInLTable();
				 //NOTE that we will have the capacity to be reserved only at the index building time. not at the initialization time
				 managerInstance.start(R_num, L_num, capacityForEntriesInLTable);
		 }
	
		 if (!simulated) {
			 this.dataLoader = this.createDataLoader(this.dataFile, this.lshFile);
			 if (this.chosenSearch == SupportedSearchCategory.SupportedSearch.LSH_SEARCH) {
			    this.idleCommunicator = this.createIdleCommunicator(); //this is actuall null, after the creation for the non-map-reduce environment.
			 }
			 else{
				 this.idleCommunicator = null;
			 }
			 
			 if (this.chosenSearch == SupportedSearchCategory.SupportedSearch.LSH_SEARCH) {
			    this.indexBuilder = this.createIndexBuilder();
			 }
			 else{
				 this.indexBuilder = null;
			 }
		 }
		 
		 //independent of whether the search is simulated or not.
		 this.searchManager = this.createSearchManager();
		 
	 }
	 
	 /**
	  * This init method is invoked from the MapReduce job, as the initialization of the LSH parameters has already been done in the MapReduce job's 
	  * configuration phase. 
	  * @lsh_Hash the LSH function parameters that have been loaded before the Map instance get called.
	  * 
	  */
	 public void init(LSHIndexConfigurationParameters parameters, LSH_Hash lsh_Hash, Reporter reporter, JobConf jobconf) {
		 this.lshConfigurationParameters=parameters; 
		 //start the LSH Manager instance 
		 if (!simulated)
		 {
				 LSHManagerProxy  managerInstance = LSHManagerProxy.INSTANCE;
				 int R_num = this.lshConfigurationParameters.getRNumber();  //3; 
				 int L_num = this.lshConfigurationParameters.getLNumber(); //100;
				 //WARNING: this capacity should be made to become configurable. 
				 int capacityForEntriesInLTable = this.lshConfigurationParameters.getCapacityForEntriesInLTable();
				 managerInstance.start(R_num, L_num, capacityForEntriesInLTable);
				 
				 //loading the LSH hash function to the C++ side.
				 for(int i=0;i<R_num;i++) {
						LSHinf lshinf = lsh_Hash.rarray.get(i);
						managerInstance.setLSH_HashFunction(i, lshinf.pLSH, lshinf.pHash); 
					}
					
				 
		 }
	
		 if (!simulated) {
			 this.dataLoader = this.createDataLoader(jobconf);//createDataLoader(JobConf jobconf) 
			 this.idleCommunicator = this.createIdleCommunicator(reporter);
			 this.indexBuilder = this.createIndexBuilder();
		 }
		 
		 //independent of whether the search is simulated or not.
		 this.searchManager = this.createSearchManager();
		 
	 }
	
	 /**
	  *  to start the service inside the MapReduce job. 
	  *  
	  * @param checker the checker that is passed in to do sampe queries verification.
	  */
	 public void startServiceInMapReduceJob(IndexBuildingChecker checker) {
		 this.serviceStarted = false;
		 boolean result = false; 
		 
		 //NOTE: we can have the report progress to be put here later on.
		 //(1) start the idle communicator (from Mijung);
		 try {
		    Thread idleCommunicatorThread = new Thread((Runnable)this.idleCommunicator);
		    idleCommunicatorThread.start();
		    result = true;
		 }
		 catch(Exception ex) {
			 LOG.error("fails to launch idle communicator thread at partition number:" + this.partitionNumber, ex);
		 }
		 
		 //(2) to do the synchronization with the coordinator.
		 this.synchronizeWithCoordinator();
		 
		 //(3) start the data loader 
		 //only when the communicator is launched successfully, as otherwise, sonner or later, the Map process will be killed by the Job Tracker.
		 if (result)
		 {
			LOG.info("successfully launched the idle communicator thread (to job tracker) at partition number: " + this.partitionNumber);
			if (!simulated) {
				result= this.dataLoader.loadTimeSeriesData();
				if (!result) {
					LOG.error("hosting service at partition: " + this.partitionNumber + "fails to load time series data...abort service");
				}
				else{
					LOG.info("successfully load time series data at partition: " + this.partitionNumber);
				}
			}
			//NOTE: we are not going to load the LSH index related parameters, as they have been loaded before reaching the Map instance (the input 
			//parameters in the Map function is the created LSH parameters. 
		 }
		
		 
		 //(4) start the index building. Then set index building to be done. We may later control the exact sequence of the indexing of the data,
		 //so that we can do indexing in multiple round. 
		 if (!simulated)
		 {
			 if (result) {
				 
				 result = this.indexBuildingWithScheduleFromCoordinator();
				 if (result) {
					 LOG.info("partitioner: " + this.partitionNumber + " successfully built its index under the coordinator's scheduling");
				 }
				 else {
					 LOG.error ("partitioner: " + this.partitionNumber + " failed to build its index under the coordinator's scheduling");
				 }
				 
				 //NOTE: how should we expose that the entire index building is successful or not.
				 //so we have the checking on the inputed query files. we will have to do this step, before the thread get blocked in the
				 //query search waiting phase (next).
				 if (result) {
					 if (checker!=null) {
						 checker.checkWithSampleQueries();
					 }
				 }
				 
			 }
		 }
		 
		 //independent of whether it is simulated or not.
		 LOG.info("finish the index building for the partition: " + this.partitionNumber + " proceeds to launch the query processor.");
		 if (result) {
		    //(5) start the multi-search manager 
			 this.searchManager.startQueryProcessors();
		 }
		 
		 this.serviceStarted = true; 
		 LOG.info("successfully start the service at partition: " + this.partitionNumber + " to be ready to accept query search at partition");
		 
		 //(6) then finally start the thread that is to accept remote query and abandonment commands 
		 QueryCommandAcceptor.QueryCommandAcceptorThread queryAndCommandAcceptorThread =
				                      new  QueryCommandAcceptor.QueryCommandAcceptorThread (
				                      this.context, this.coordinatorMasterAddress, this.privateIPAddress, this.searchManager);
		 queryAndCommandAcceptorThread.start();
		 try {
		    queryAndCommandAcceptorThread.join(); 
		 }
		 catch (Exception ex) {
			 //do nothing;
		 }
	 }
	 
	 
	 /**
	  * This method is called in the non-map-reduce job environment
	  * the last join on the query/command acceptor will make this method to be blocked.
	  */
	 public void startService(boolean checkerEnabled, ArrayList<ArrayList<Float>> sampleQueryList) {
		 this.serviceStarted = false;
		 //(0) start the idle communicator (from Mijung); we do not need this step, for the non-map-reduce environment.
		 
		 //(1) to do the synchronization with the coordinator.
		 this.synchronizeWithCoordinator();
		 
		 //(2) start the data loader. this way, the partitioner can have some work to do before to check with the coordinator for the scheduling of 
		 //the index building.
		 boolean result = true;//initialize to be true. 
		 
		 //NOTE: we can have the report progress to be put here later on.
		
		 //(3) start the index building with the coordination from the coordinator. Then set index building to be done. We may later control the exact sequence of the indexing of the data,
		 //so that we can do indexing in multiple round. 
		 //NOTE: I choose even for Naive search, we will go ahead to do the dummy index building, as we will need the synchronization barrier before 
		 //moving to wait for the search (loading of large amount of partitioner data into memory on the last step still takes some time.
		 LOG.info("partitioner: "  + this.partitionNumber + " is ready to move into index building phase...");
		 if (!simulated) {
			 if (result) {
				 result = this.indexBuildingWithScheduleFromCoordinator();
				 if (result) {
					 LOG.info("partitioner: " + this.partitionNumber + " successfully built its index under the coordinator");
				 }
				 else {
					 LOG.error ("partitioner: " + this.partitionNumber + " failed to build its index under the coordinator");
				 }
				 
				 if (result) {
					 if (this.chosenSearch == SupportedSearchCategory.SupportedSearch.LSH_SEARCH) {
						 if (checkerEnabled) {
							 int R_num= this.configuration.getInt("RNumber", 6);
							 int L_num = this.configuration.getInt("LNumber", 200);
							 int pert_num = this.configuration.getInt("numberOfPerturbations",100);
							 int topk_no= this.configuration.getInt("topk_no", 5);
							 
							 //the LSH parameters has already been loaded earlier via "loadIndexBuildingParameters".
							 LSH_Hash lsh_Hash = this.dataLoader.getLSHFunctions();
							 IndexBuildingChecker checker = new IndexBuildingCheckerImpl(R_num, L_num, pert_num, topk_no, lsh_Hash, sampleQueryList);
							 checker.checkWithSampleQueries();
						 }
					 }
					 else if (this.chosenSearch == SupportedSearchCategory.SupportedSearch.NAIVE_SEARCH){
						 if (checkerEnabled) {
							 int topk_no= this.configuration.getInt("topk_no", 5);
							 IndexBuildingChecker checker = new SampleQueriesCheckerImpl(this.partitionNumber, topk_no,sampleQueryList);
						     checker.checkWithSampleQueries();
						 }
					 }
				 }
			 }
		 }
		 else {
			 result = true;
		 }
		 
		 
		 //independent of whether it is simulated or not.
		 LOG.info("finish the index building for the partition: " + this.partitionNumber + " proceeds to launch the query processor.");
		 if (result) {
		 
		    //(4) start the multi-search manager 
			 this.searchManager.startQueryProcessors();
		 }
		 
		 this.serviceStarted = true; 
		 LOG.info("successfully start the service at partition: " + this.partitionNumber + " to be ready to accept query search at partition");
		 
		 //(4) then finally start the thread that is to accept remote query and abandonment commands 
		 QueryCommandAcceptor.QueryCommandAcceptorThread queryAndCommandAcceptorThread =
				                      new  QueryCommandAcceptor.QueryCommandAcceptorThread (
				                      this.context, this.coordinatorMasterAddress, this.privateIPAddress, this.searchManager);
		 queryAndCommandAcceptorThread.start();
		 try {
		    queryAndCommandAcceptorThread.join(); 
		 }
		 catch (Exception ex) {
			 //do nothing;
		 }
	 }
	 
	 public void shutdownService() {
		 if (this.serviceStarted) {
			 this.searchManager.shutdownQueryProcessors();
			 //WARN: we also need to shutdown the idle communicator as well.
		 }
	 }
	 
	 @Override
	 public void synchronizeWithCoordinator() {
		    //before getting to the polling of the message, let's use REQ and REP to wait for the synchronization point. 
	        ZMQ.Socket synchronizationBarrier1 = context.socket(ZMQ.REQ);
	        int synchronizationPort1=CoordinatorPortConfiguration.QUERY_COMMAND_DISTRIBUTION_SYNCHRONIZATION_PORT_1;
	        String destinationSynchronizationAddress1 = "tcp://" + this.coordinatorMasterAddress
                                                        +":" + ( new Integer(synchronizationPort1)).toString();
	        synchronizationBarrier1.connect(destinationSynchronizationAddress1);
	        
	        //we need to subscribe it much earlier, so that we will not miss the first published message.
	        ZMQ.Socket synchronizationBarrier2 = context.socket(ZMQ.SUB);
	        int synchronizationPort2=CoordinatorPortConfiguration.QUERY_COMMAND_DISTRIBUTION_SYNCHRONIZATION_PORT_2;
	        String destinationSynchronizationAddress2 = "tcp://" + this.coordinatorMasterAddress
                                                       +":" + ( new Integer(synchronizationPort2)).toString();
	        synchronizationBarrier2.connect(destinationSynchronizationAddress2);
	        synchronizationBarrier2.subscribe(new byte[0]);
	        
	        //instead of sending hello message, we use this one to communicate to the coordinator the runtime information about this 
	        //partition. the response will be received when the number of 
	        //partitions have been reached. 
	        {
	            byte[] runtimeInfo = reportRuntimeInformation(); 
	            LOG.info("Sending runtime information as the hand-shaking information from partition: " + this.searchManager.getPartitionId() 
	            		 + " to the coordinator");
	            synchronizationBarrier1.send(runtimeInfo, 0);
	
	            byte[] reply = synchronizationBarrier1.recv(0);
	            LOG.info("Received for hand-shaking " + new String(reply));
           }
	        
	        LOG.info("the partitioner: " + this.searchManager.getPartitionId() + " is waiting for hand-shaking barrier to proceed...");
	        {
	              byte[] reply = synchronizationBarrier2.recv(0);
	              //get the initial command 
	              this.initialSchedulingCommand = new CoordinatedIndexBuilding.Command();
	              try {
		   	            
			   	        ByteArrayInputStream in = new ByteArrayInputStream(reply);
			   	        DataInputStream dataIn = new DataInputStream(in);
			   	        
			   	        this.initialSchedulingCommand.readFields(dataIn);
			   	        dataIn.close();
		   	        
		   	      }
		   	      catch (Exception ex) {
		   	        	LOG.error("fails to receive/reconstruct the initial scheduling command information from the aggregator", ex);
		   	      }
		   	       
	             LOG.info("the partitioner: " +  this.searchManager.getPartitionId() 
	            		  + " received for hand-shaking (the initial scheduling command) and now it finishes the synchronization.");
	             //this is not on the critical path of the search related computation. 
	             {
	            	 if (this.initialSchedulingCommand != null) {
		            	 //display the initial scheduling command information
		            	 List<CoordinatedIndexBuilding.SchedulingUnit> schedulingUnits = this.initialSchedulingCommand.getSchedulingUnits();
		            	 LOG.info("the initial scheduling command is the following");
		            	 int counter=0;
		            	 for (CoordinatedIndexBuilding.SchedulingUnit unit: schedulingUnits) {
		            		 LOG.info("the " + counter + "-th scheduling unit has private IP address: " 
		            	                + unit.getPrivateIPAddress() + " and process id:" + unit.getProcessId());
		            		 counter++;
		            	 }
	            	 }
	            	 else{
	            		 LOG.info("no initial scheduling command to be displayed....");
	            	 }
	            	 
	             }
	        }
		 
	 }
	 
	 private byte[] reportRuntimeInformation() {
		  
		 //get the public IP address by querying the machine
		 this.publicIPAddress = null;
		 try {
		    this.publicIPAddress = InetAddress.getLocalHost().getHostAddress();
		 }
		 catch(UnknownHostException ex) {
			 LOG.error("can not resolve the machine's public IP address", ex);
			 this.publicIPAddress = null;
		 }
		  
		 String processIdStr = ManagementFactory.getRuntimeMXBean().getName();
		 int indexOfSeperator = processIdStr.lastIndexOf("@");
		 String realProcessIdPart = processIdStr.substring(0, indexOfSeperator);
		 
		 this.processId = 0;
		 try {
	   	    this.processId = Integer.parseInt(realProcessIdPart);
		 }
		 catch (NumberFormatException ex) {
			 LOG.error("can not resolve the Map instance's process id on the machine of: " + this.privateIPAddress);
			 this.processId=0;
		 }
	
		 ProcessingUnitRuntimeInformation information = 
	    			new ProcessingUnitRuntimeInformation (this.processId, this.publicIPAddress, this.privateIPAddress,
	    			this.partitionNumber);
	    	
	     ByteArrayOutputStream out = new ByteArrayOutputStream();
	     DataOutputStream dataOut = new DataOutputStream (out);
	     try {
	    	  information.write(dataOut);
	    	  dataOut.close();
	     }
	     catch(IOException ex) {
	    		LOG.error ("fails to serialize the partition's runtime information", ex);
	     }
	    
	     byte[] message = out.toByteArray();
	     
	     return message;
	   
	 }
	 
	 
	 
	 @Override
	 public boolean indexBuildingWithScheduleFromCoordinator() {
		
		boolean result = false;
		boolean goAheadForIndexBuilding=false; 
		
		//(3.1) wait under receiving the commands
	    ZMQ.Socket indexingCoordinationReceiver = context.socket(ZMQ.SUB);
	    int indexingCoordinationPort = CoordinatorPortConfiguration.COORDINATED_INDEX_BUILDING_PORT_1;
	    String indexingCoordinationAddress = "tcp://" + this.coordinatorMasterAddress
	    		                                          +":" + ( new Integer(indexingCoordinationPort)).toString();
	    indexingCoordinationReceiver.connect(indexingCoordinationAddress);
	    ///subscription topic to be with zero length, that is, to subscribe to any message.
	    indexingCoordinationReceiver.subscribe(new byte[0]); 
        
        
        ZMQ.Socket indexBuildingNotifier = context.socket(ZMQ.REQ);
        int indexBuildingNotificationPort = CoordinatorPortConfiguration.COORDINATED_INDEX_BUILDING_PORT_2;
        String indexBuildingNotificationAddress = "tcp://" + this.coordinatorMasterAddress
                                                           +":" + ( new Integer(indexBuildingNotificationPort)).toString();
        indexBuildingNotifier.connect(indexBuildingNotificationAddress);
	  
 
        //then move to the next step, to wait for scheduler commands that match my process id  and my private IP address.
        if (this.initialSchedulingCommand == null) {
        	//bad, the protocol is broken;
        	return result;
        }
        else {
        	List<CoordinatedIndexBuilding.SchedulingUnit> initialSchedulingUnits = this.initialSchedulingCommand.getSchedulingUnits();
        	for (CoordinatedIndexBuilding.SchedulingUnit unit: initialSchedulingUnits) {
        		String ipAddress = unit.getPrivateIPAddress();
        	    int pId = unit.getProcessId();
        	    if ( (ipAddress.compareTo(this.privateIPAddress) == 0) && (pId == this.processId)) {
        	    	goAheadForIndexBuilding=true;
        	    	break;
        	    }
        	}
        }
        
        //if I am not in the initial scheduling unit, I will have to wait unit my turn comes. otherwise, I do not need to wait for my
        //turn to come--just go to the next block.
        if (!goAheadForIndexBuilding) {
	        //  Initialize poll set
	        ZMQ.Poller items = new ZMQ.Poller (1);
	        items.register(indexingCoordinationReceiver, ZMQ.Poller.POLLIN);
	
	        //  Process messages from both sockets
	        
	        while (!Thread.currentThread ().isInterrupted ()) {
	            byte[] message = null;
	            items.poll();
	            if (items.pollin(0)) {
	               message = indexingCoordinationReceiver.recv(0);
	               goAheadForIndexBuilding= this.processIndexBuildingCoordinationCommand(message);
	               if (goAheadForIndexBuilding) {
	            	   break; 
	               }
	            }
	             
	        }
        }
        
        if(goAheadForIndexBuilding) {
        	 LOG.info("partitioner: " + this.partitionNumber + " just received signal from the coordinator to go ahead to build index.");
        	 //move the loading of the index to just before the index building. This way, all partitioners will not conflict with each other
        	 //on loading the data from disk to the memory.
        	 if (!simulated)
    		 {
    			result= this.dataLoader.loadTimeSeriesData();
    			if (!result) {
    				LOG.error("hosting service at partition: " + this.partitionNumber + "fails to load time series data...abort service");
    			}
    			else {
    				LOG.info("finish loading time series data at partition: " + this.partitionNumber + " at the process: " + this.processId);
    				if (this.chosenSearch == SupportedSearchCategory.SupportedSearch.LSH_SEARCH) {
    					result = this.dataLoader.loadIndexBuildingParameters();
    					if (!result) {
    						LOG.error("hosting service at partition: " + this.partitionNumber + "fails to load LSH function parameters ...abort service");
    					}
    					else {
    						LOG.info("hosting service at partition: " + this.partitionNumber + " finish loading LSH function parameters.");
    					}
    				}
    			}
    		 }
    		 else {
    			 result = true;
    		 }
        	 
        	 if (this.chosenSearch == SupportedSearchCategory.SupportedSearch.LSH_SEARCH)
        	 {
				 //(3.2) then do the actual building;
				 //(3>) report back the results to the coordinator. 
				 List<PartitionOfLTablesAtRForIndexBuilding>  partitions = this.indexBuilder.getRangePartitionForIndexBuilding();
				 LOG.info("the partition for indexing is the following: ");
				 LOG.info("total number of the paritions is: " + partitions.size());
				 int counter=0;
				 for (PartitionOfLTablesAtRForIndexBuilding p: partitions) {
						LOG.info( counter + " -th parition has  R value: " + p.getRValue()
								+ " and low range: " + p.getRangeLowForLTables() + " and high range: " + p.getRangeHighForLTables());
						counter++;
				 }
				 //at this time, we do not have the status report on whether all of the index tables have been built correctly.
				 //so we assume that when this method gets finished, all of the index tables are built successfully.
				 this.indexBuilder.startIndexBuilding(partitions);
        	 }
        	 else{
        		 LOG.info("naive search is chosen. no need to build index..but sleep 100 ms before ack...");
        		 try {
        			 Thread.sleep(100);
        		 }
        		 catch(Exception ex) {
        			 //ignore.
        		 }
        	 }
        	 
			 //then now signal that this partitioner has done its index building now. 
			 {
				 //(1) prepare the status 
				 CoordinatedIndexBuilding.SchedulingUnit unit = new CoordinatedIndexBuilding.SchedulingUnit(this.privateIPAddress, this.processId);
				 boolean status = true; //at this, we always set it to be true.
				 CoordinatedIndexBuilding.Status cStatus = new CoordinatedIndexBuilding.Status (unit, status);
				 //turn the status to be bytes
				 ByteArrayOutputStream out = new ByteArrayOutputStream();
		   	     DataOutputStream dataOut = new DataOutputStream (out);
		   	     try {
		   	    	 cStatus.write(dataOut);
		   	    	 dataOut.close();
		   	     }
		   	     catch(IOException ex) {
		   	    		LOG.error ("fails to serialize the partition's scheduling unit's index building status information", ex);
		   	     }
		   	    
		   	    	
		   	    byte[] message = out.toByteArray();
		   	    //(2) Socket to send messages on REQ
		   	    //we choose this send result to be our final result for the synchronized index building.
		   	    result = indexBuildingNotifier.send(message, 0, message.length, 0); 
	   	        if (!result) {
	   	        	LOG.error("fails to send out the partition's scheduling unit's index building status information to the coordinator");
	   	        }
				
	   	        
				//(3) we have to wait for the dummy reply from "REQ/REP" in this scheduling protocol.
	   	        byte[] reply = indexBuildingNotifier.recv(0);
	            LOG.info("Received for hand-shaking for scheduling " + new String(reply));
			 }
        }
		 
		 return result;
	 }
 
	 /**
	  * the expected message is with the time of CoordinatedIndexBuilding.command.
	  * @param message
	  * @return
	  */
	 private boolean processIndexBuildingCoordinationCommand(byte[] message){
		 boolean result =false;
		  //we are using polling here. 
		 if (LOG.isDebugEnabled()) {
		    LOG.debug("partitioner: " + this.partitionNumber + " receives scheduling command with length of: " + message.length);
		 }
		 
		 CoordinatedIndexBuilding.Command  command = new CoordinatedIndexBuilding.Command();
	     try {
	            
	   	        ByteArrayInputStream in = new ByteArrayInputStream(message);
	   	        DataInputStream dataIn = new DataInputStream(in);
	   	        
	   	        command.readFields(dataIn);
	   	        dataIn.close();
	        
	      }
	      catch (Exception ex) {
	        	LOG.error("fails to receive/reconstruct the coordinated scheduling command", ex);
	      }
	      
		  List<CoordinatedIndexBuilding.SchedulingUnit> units= command.getSchedulingUnits();
		  for (CoordinatedIndexBuilding.SchedulingUnit su: units) {
			 if ( ( this.privateIPAddress.compareTo(su.getPrivateIPAddress()) == 0) 
			           && (this.processId == su.getProcessId())) {
				 //it is me: ip address and process id are identical.
				 result=true;
				 break;
			 }
		  }
		  
		  return result;
	 }
	 
	 
			 
	 /**
	  * NOTE: pay attention to the current query search related threads, when go to the  real search engine.
	  * 
	  */
	 private void loadLSHConfigurationParameters(){
		 //how to load it? 
		 this.lshConfigurationParameters = new LSHIndexConfigurationParameters();
		 
		 this.lshConfigurationParameters.setTopK(this.configuration.getInt("topk_no",5));
		 //this.lshConfigurationParameters..setTSdir(job.get("tsdir"));
		 this.lshConfigurationParameters.setRNumber(this.configuration.getInt("RNumber",6)); //default value, should be set by the time series configuration file
		 this.lshConfigurationParameters.setLNumber(this.configuration.getInt("LNumber",200)); ////default value, should be set by the time series configuration file
		 this.lshConfigurationParameters.setNumberOfPerturbations(this.configuration.getInt("numberOfPerturbations", 80)); //default value, should be set by the time series configuraiton file
		 this.lshConfigurationParameters.setQueryLength(this.configuration.getInt("queryLength",48));
		 this.lshConfigurationParameters.setNumberofWorkerThreadsForIndexing(this.configuration.getInt("numberOfWorkerThreadsForIndexing", 3));
		 this.lshConfigurationParameters.setNumberOfLTablesInAIndexingGroup(this.configuration.getInt("numberOfLTablesInAIndexingGroup", 50));
		 this.lshConfigurationParameters.setCapacityForEntriesInLTable(this.configuration.getInt("capacity", 700000)); //default for 256 MB
	     //only use for the MapReduce.
		 //this.lshConfigurationParameters.setTimeCommunicator(this.configuration.getInt("timeCommunicator", 120000)); //2 minutes by default.
			    
		 //for the communication buffer size. Currently, we are not using this parameter.  
		 this.lshConfigurationParameters.setCommunicatorBufferLimit(this.configuration.getInt("communicationBuffer", 300));  //300 ms
		 //the number of the concurrent queries allowed in each partition's incoming query processing queue.
		 this.lshConfigurationParameters.setNumberOfConcurrentQueriesAllowed(this.configuration.getInt("concurrentQueriesAllowed", 1000));  //1000
		 //the number of the active queries held in each worker thread, in the concurrent queries setting. 
		 this.lshConfigurationParameters.setNumberOfQueriesInWorkListForWorkerThread(this.configuration.getInt("workListInWorkerThread", 10));  //10
		 //this is to design for intra-query in which different threads devoted to processing different L tables and merged results.
		 this.lshConfigurationParameters.setNumberOfWorkerThreadsForSearchPerQuery(this.configuration.getInt("numberOfWorkerThreadsPerQuery", 1));  //1
		 this.lshConfigurationParameters.setNumberOfThreadsForConcurrentQuerySearch(this.configuration.getInt("numberOfWorkerThreadsForSearch", 1)); //worker threads to be 1, let's make it single thread first.
		 this.lshConfigurationParameters.setScheduledCommunicatorBufferSendingInterval(this.configuration.getInt("communicationBufferSendingInterval", 200));//200 ms
		 this.lshConfigurationParameters.setNumberOfQueriesQueuedForWorkerThread (this.configuration.getInt("numberOfQueriesQueuedForWorkerThread", 100)); //100 concurrent queries to be queued 
		
	 }
	 
	 //used for the cluster-based non-map-reduce environment.
	 protected DataLoader createDataLoader(String dataFile, String lshFile) {
		  DataLoader dataLoader = new DataLoaderImpl(dataFile, lshFile, 
				            this.partitionNumber, this.lshConfigurationParameters);
		  return dataLoader;
	 }
	 
	 //used for the MapReduce job
	 protected DataLoader createDataLoader(JobConf jobconf) {
		 TimeSeriesDataLoader ts_loader = new TimeSeriesDataLoader();
		 String tsdir = this.lshConfigurationParameters.getTSdir();
		 int querylength = this.lshConfigurationParameters.getQueryLength();
		 ts_loader.setParameters(this.partitionNumber, tsdir, jobconf, querylength);
		 return ts_loader;
	 }
	 
	 //used for the cluster-based non-map-reduce environment.
	 protected IdleCommunicator createIdleCommunicator() {
	      return null; //for the cluster non-map-reduce environment, we will not use this idle communicator. 
	 }
	 
	 //use for the MapReduce job
	 protected IdleCommunicator createIdleCommunicator(Reporter reporter) {
		 TimeSeriesSearchCommunicator communicator = new TimeSeriesSearchCommunicator();
		 int tmCommunicator = this.lshConfigurationParameters.getTimeCommunicator();
		 communicator.setIdleProgressPeriod(tmCommunicator);
		 communicator.setReporter(reporter);
		 return communicator; 
			
	 }
	 
	 protected IndexBuilder createIndexBuilder() {
		IndexBuilder builder = new IndexBuilderImpl(this.lshConfigurationParameters);
	
		return builder;
	 }
	
	 protected MultiSearchManager createSearchManager(){
		 
		 int bufferedCoomunicationPort = CoordinatorPortConfiguration.INTERMEDIATE_SEARCH_RESULT_REPORT_PORT;
		 SinkAdaptorForBufferedCommunicator sinkAdaptor = 
				 new SinkAdaptorForNoBufferedCommunicatorImpl.ZeroMQBasedAdaptorForNoBufferedCommunicatorImpl  (this.context,  
						                        this.coordinatorMasterAddress, bufferedCoomunicationPort);
		 this.partitionNumber = this.getPartitionNumber();
	 
		 MultiSearchManager  manager = 
				  new MultiSearchManagerImpl(this.lshConfigurationParameters, this.partitionNumber, sinkAdaptor, this.chosenSearch, this.simulated);
		 return manager;
	 }
	 
     /**
      * This will be done in a separated thread.
      */
	 protected  void startIdelCommunicator (){
		 
	 }

	 
	 private int getPartitionNumber() {
		 return this.partitionNumber;
	 }
	 
	
}
