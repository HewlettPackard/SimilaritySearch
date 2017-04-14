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

import java.util.List;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import com.hp.hpl.palette.similarity.datamodel.CoordinatedIndexBuilding;
import com.hp.hpl.palette.similarity.datamodel.ProcessingUnitRuntimeInformation;
import com.hp.hpl.palette.similarity.datamodel.TimeSeriesSearch.FinalQueryResult;
import com.hp.hpl.palette.similarity.datamodel.TimeSeriesSearch.SearchResult;

/**
 * For the coordinator to schedule the round-robin based index building for the partitioners that are assigned on the same NUMA node that share
 * the same private IP address 
 *
 */
public class IndexBuildingScheduler {
     private List<ProcessingUnitRuntimeInformation> allPartitionersInformation;
     //the string holds the private IP address representing the NUMA node.
     private HashMap<String, PartitionersWithStatusOnNumaNode> partitionerSchedulingTracker;
	 
     
     private static class PartitionerWithStatus {
    	 public int processId; 
    	 public boolean indexBuildingDone;
    	 
    	 public PartitionerWithStatus (int processId, boolean indexBuildingDone) {
    		 this.processId = processId;
    		 this.indexBuildingDone = indexBuildingDone;
    	 }
    	
     }
     
     
     private static class PartitionersWithStatusOnNumaNode    {
        private List<PartitionerWithStatus> partitionersWithStatus;
        private boolean allPartitionersScheduled; ///all of the partitioners are done with the index scheduling.
        
        public PartitionersWithStatusOnNumaNode (){
        	this.partitionersWithStatus = new ArrayList<PartitionerWithStatus>();
        	this.allPartitionersScheduled = false;
        }
  
        public void registerProcessId(int processId) {
        	boolean registered=false;
  
        	for (PartitionerWithStatus partitionerStatus: this.partitionersWithStatus) {
        		 if ( partitionerStatus.processId == processId) {
        			 registered=true;
        			 break;
        		 }
        	}
        	
        	if (!registered) {
        		PartitionerWithStatus newPartitioner= new PartitionerWithStatus(processId, false);
        		this.partitionersWithStatus.add(newPartitioner);
        	}
        }
        
        
 		public void updatePartitionerStatus(int processId, boolean status) {
 			for (PartitionerWithStatus partitionerStatus: this.partitionersWithStatus) {
	       		if ( partitionerStatus.processId == processId) {
	       			partitionerStatus.indexBuildingDone = status;
	       			 break;
	       		}
 			}
       	}
       	
 		/**
 		 * if the return is -1, that is because of the partitioners list is empty.
 		 * @return -1 is the partitioners in this NUMA is empty.
 		 */
 		public int getFirstPartitionerForWork () {
 			//(1) get the array that has all of the integers.
 			if ((this.partitionersWithStatus == null) || (this.partitionersWithStatus.size() ==0)) {
 				return -1;
 			}
 			else {
	 			int size = this.partitionersWithStatus.size();
	 			int processIds[] = new int[size];
	 			 
	 			for (int i=0; i<this.partitionersWithStatus.size(); i++) {
	 				processIds[i] = this.partitionersWithStatus.get(i).processId;
	 			}
	 			//(2) sort it.
	 			Arrays.sort(processIds);
	 			
	 			//(3) pick the smallest one.
	 			return processIds[0];
 			}
 		}
 		
 		/**
 		 * if the return is -1, that is because of all of the partitioners have been done with the scheduling.
 		 * 
 		 * @return
 		 */
 	    public int pickNextPartitionerForWork() {
 	   	    if ((this.partitionersWithStatus == null) || (this.partitionersWithStatus.size() ==0)) {
				return -1;
			}
 	   	    else {
	 	    	 //for the partitioners that have not been done, pick the largest process id, and then schedule it.
 	   	    	 List<PartitionerWithStatus> notScheduledYet = new ArrayList<PartitionerWithStatus>();
 	   	    	 for (PartitionerWithStatus partitionerWithStatus: this.partitionersWithStatus) {
 	   	    		 if (!partitionerWithStatus.indexBuildingDone) {
 	   	    			notScheduledYet.add(partitionerWithStatus);
 	   	    		 }
 	   	    	 }
 	   	    	 
	 	    	 //(1) get the array that has all of the integers that has the index not done.
 	   	    	 int size = notScheduledYet.size();
 	   	    	 if (size == 0) {
 	   	    		 this.allPartitionersScheduled = true;
 	   	    		 return -1; //nothing left for scheduling.
 	   	    	 }
 	   	    	 else {
		 	    	 //(2) sort it
 	   	    		 int processIds[] = new int[size];
 		 			 
 		 			 for (int i=0; i<notScheduledYet.size(); i++) {
 		 				processIds[i] = notScheduledYet.get(i).processId;
 		 			 }
 		 			//(2) sort it.
 		 			Arrays.sort(processIds);
 		 			
 		 			//(3) pick the smallest one.
 		 			return processIds[0];
 	   	    	 }
 	   	    	 
 	   	    }
 	    }
 	    
 	    /**
 	     * to inspect whether all of the partitioners on this NUMA has been scheduled.
 	     * @return
 	     */
 	    public boolean allPartitionersScheduled() {
 	    	if ((this.partitionersWithStatus == null) || (this.partitionersWithStatus.size() ==0)) {
 	    		return true;
 	    	}
 	    	else {
 	    		return this.allPartitionersScheduled;
 	    	}
 	    }
     }
     
     /**
      * The entire partitioner information is received from the hand-shaking information from all of the partitioners.
      * 
      * @param partitionersInfo received by the coordinator from all of the partitioners
      */
     public IndexBuildingScheduler (List<ProcessingUnitRuntimeInformation>  partitionersInfo) {
    	 this.allPartitionersInformation = partitionersInfo;
    	 this.partitionerSchedulingTracker  = new  HashMap<String, PartitionersWithStatusOnNumaNode> ();
    	 
    	 for (ProcessingUnitRuntimeInformation runtimeInfo: this.allPartitionersInformation) {
    		 //based on the private IP address
    		 String privateIPAddress = runtimeInfo.getPrivateIPAddress();
    		 PartitionersWithStatusOnNumaNode partitionersOnThisNumaNode =  this.partitionerSchedulingTracker.get(privateIPAddress);
    		 if (partitionersOnThisNumaNode == null) {
    			 //this is the first time the IP address builds this NUMA node tracker.
    			 PartitionersWithStatusOnNumaNode newPartitionersOnNumaNode = new PartitionersWithStatusOnNumaNode();
    			 this.partitionerSchedulingTracker.put(privateIPAddress, newPartitionersOnNumaNode);
    			 newPartitionersOnNumaNode.registerProcessId(runtimeInfo.getProcessId());
    		 }
    		 else {
    			 partitionersOnThisNumaNode.registerProcessId(runtimeInfo.getProcessId());
    		 }
    	 }
     }
     
     /**
      * Produce the first batch of the partitioners from all of the NUMA nodes that will start the indexing work immediately after the synchronization. 
      * each private IP address will pick one. 
      * @return the command that contains the scheduling unites to be broadcast to all of the partitioners. 
      */
     public CoordinatedIndexBuilding.Command getFirstPartitionersForWorkOnNumaNodes () {
    	 List<CoordinatedIndexBuilding.SchedulingUnit> schedulingUnits = new ArrayList<CoordinatedIndexBuilding.SchedulingUnit>();
    	 for (Map.Entry<String, PartitionersWithStatusOnNumaNode> partitionersOnNumaNode:
    		                                               this.partitionerSchedulingTracker.entrySet()) {
    		 PartitionersWithStatusOnNumaNode numaNode = partitionersOnNumaNode.getValue();
    		 int processId=numaNode.getFirstPartitionerForWork();
    		 if (processId != -1) {
    			 String privateIPAddress = partitionersOnNumaNode.getKey();
    			 CoordinatedIndexBuilding.SchedulingUnit unit = new CoordinatedIndexBuilding.SchedulingUnit (privateIPAddress, processId);
    			 schedulingUnits.add(unit);
    		 }
    	 }
    	 
    	 //finally construct a command 
    	 CoordinatedIndexBuilding.Command command = new  CoordinatedIndexBuilding.Command(schedulingUnits);
    	 
    	 return command;
     }
     
     /**
      * Given a NUMA node that has just finish the indexing work, pick the next one on the same NUMA node (that is, there is no work that can be 
      * scheduled for a partitioner that is bound a different NUMA node).
      * @param privatIPAddress the private IP address that represents the NUMA node 
      * @return the command that contains ONLY ONE scheduling unit that shares the same private IP address as the input; return NULL is nothing else to 
      * be scheduled on this NUMA node. 
      */
     public CoordinatedIndexBuilding.Command getNextPartitionerForWorkOnNumaNode (String privateIPAddress){
    	 CoordinatedIndexBuilding.Command command = null;
    	 PartitionersWithStatusOnNumaNode numaNode  = this.partitionerSchedulingTracker.get(privateIPAddress);
    	 if (numaNode!=null) {
    		 //usually we will come to this branch with numanode != null, unless something is wrong. 
	    	 int processId = numaNode.pickNextPartitionerForWork();
	    	 if (processId != -1) {
	    		 List<CoordinatedIndexBuilding.SchedulingUnit> schedulingUnits = new ArrayList<CoordinatedIndexBuilding.SchedulingUnit>();
	    		 CoordinatedIndexBuilding.SchedulingUnit unit = new CoordinatedIndexBuilding.SchedulingUnit (privateIPAddress, processId);
	    		 schedulingUnits.add(unit);
	    		 command = new  CoordinatedIndexBuilding.Command(schedulingUnits);
	    		 
	    	 }
    	 }
    	 return command;
     }
     
     /**
      * Given a NUMA node that has just finish the indexing work, update its done status. 
      * @param privateIPAddress the private IP address that represents the NUMA node 
      * @param status typically it is with true value.
      */
     public void updatePartitionerWorkingStatusOnNumaNode(CoordinatedIndexBuilding.SchedulingUnit schedulingUnit, boolean status){
    	 PartitionersWithStatusOnNumaNode numaNode  = this.partitionerSchedulingTracker.get(schedulingUnit.getPrivateIPAddress());
    	 if (numaNode!=null) {
    	    numaNode.updatePartitionerStatus(schedulingUnit.getProcessId(), true);
    	 }
     }
     
     /**
      * to check whether the entire system's index building has been fully scheduled, across all of the partitioners and across all of the NUMA nodes.
      * @return true if all partitioners in the entire system have been scheduled.otherwise, false.
      * 
      */
     public boolean allPartitionersOnNumaNodesScheduled() {
    	 boolean result=true;
    	 for (Map.Entry<String, PartitionersWithStatusOnNumaNode> partitionersOnNumaNode: this.partitionerSchedulingTracker.entrySet()) {
    		 PartitionersWithStatusOnNumaNode numaNode = partitionersOnNumaNode.getValue();
    		 if (numaNode!=null) {
	    		 if (!numaNode.allPartitionersScheduled()){
	    			 result=false;
	    			 break;
	    		 }
    		 }
    	 }
    	 
    	 return result;
     }
}
