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

package com.hp.hpl.palette.similarity.datamodel;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.AbstractMap.SimpleEntry;

import org.apache.hadoop.io.Writable;

import com.hp.hpl.palette.similarity.datamodel.TimeSeriesSearch.SearchResult;

/**
 * to define the data model that is required to coordinate the index building for all of the partitiners that share the same NUMA node (that is,
 * the private IP address)
 * 
 *
 */
public class CoordinatedIndexBuilding {

  public static class SchedulingUnit  implements Writable{
	  private String privateIPAddress;
	  private int processId; 
	  
	  public SchedulingUnit () {
		  //default, required for the un-marshalling.
	  }
	  
	  public SchedulingUnit(String ipAddress, int processId) {
		  this.privateIPAddress = ipAddress;
		  this.processId = processId;
	  }
	  
	  public String getPrivateIPAddress(){
		  return this.privateIPAddress;
	  }
	  
	  public int getProcessId(){
		  return this.processId;
	  }

	@Override
	public void readFields(DataInput in) throws IOException {
		//machineIPAddress
		int privateIPAddressLength = in.readInt();
		if (privateIPAddressLength == 0) {
			 this.privateIPAddress = null;
		}
		else{
			 byte[] bytes = new byte[privateIPAddressLength];
			 in.readFully(bytes, 0, privateIPAddressLength);
			 this.privateIPAddress = new String(bytes);
	    }
		
		//process id
		this.processId = in.readInt();
		
	}

	@Override
	public void write(DataOutput out) throws IOException {
		//write the string
		if (this.privateIPAddress == null) {
			out.writeInt(0);
		}
		else {
			 byte[] privateIPAddressInBytes = this.privateIPAddress.getBytes();
			 out.writeInt(privateIPAddressInBytes.length);
			 out.write(privateIPAddressInBytes, 0, privateIPAddressInBytes.length);
		}
		
		//write the process id.
		out.writeInt(this.processId);
	}
  }
  
  
   /**
    * the command sent from the coordinator to the partitioners.
    *
    */
   public static class Command  implements Writable {
	    //the scheduling units that will do the indexing next.
        private  List<SchedulingUnit> unitsToBeScheduled;
        
        public Command () {
        	//default, for un-marshalling.
        }
        
        public Command (List<SchedulingUnit> units) {
        	this.unitsToBeScheduled=units;
        }
        
        public  List<SchedulingUnit>  getSchedulingUnits(){
        	return this.unitsToBeScheduled;
        }
        
		@Override
		public void readFields(DataInput in) throws IOException {
			 
			 int lengthOfSchedulingUnits = in.readInt();
			 if (lengthOfSchedulingUnits == 0) {
				 this.unitsToBeScheduled = null;
			 }
			 else{
				 this.unitsToBeScheduled= new ArrayList<SchedulingUnit> ();
				 for (int i=0; i<lengthOfSchedulingUnits; i++) {
					  
					 SchedulingUnit su = new SchedulingUnit();
					 su.readFields(in);
					 this.unitsToBeScheduled.add(su);
				 }
				 
			 }
		}
	
		@Override
		public void write(DataOutput out) throws IOException {
			//queryPattern
			if ((this.unitsToBeScheduled == null) || (this.unitsToBeScheduled.size() ==0)){
				out.writeInt(0);
			}
			else{
				int size = this.unitsToBeScheduled.size();
			    out.writeInt(size);
			    for (SchedulingUnit unit:this.unitsToBeScheduled) {
			    	unit.write(out);
			    }
			}
		}
	   
   }
   
   /**
    * the status information sent from the partitioners to the coordinator.
    *
    */
   public static class Status implements Writable{
	    private  SchedulingUnit  unit;
	    private  boolean status;
	    
	    public Status () {
	    	//default, for un-marshalling purpose.
	    	this.unit = null;
	    	this.status = false;
	    }
	    
	    public Status (SchedulingUnit unit, boolean status) {
	    	this.unit = unit;
	    	this.status=status;
	    }
	    
	    public SchedulingUnit getSchedulingUnit() {
	    	return this.unit;
	    }
	    
	    public boolean getStatus() {
	    	return this.status;
	    }
	    
		@Override
		public void readFields(DataInput in) throws IOException {
			 this.unit = new SchedulingUnit();
			 this.unit.readFields(in);
			 this.status = in.readBoolean();
			
		}
	
		@Override
		public void write(DataOutput out) throws IOException {
			 //here we assume that the unit is always not null.
			 this.unit.write(out);
			 out.writeBoolean(this.status);	
		}
	   
   }
}
