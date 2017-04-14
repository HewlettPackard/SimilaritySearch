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

import org.apache.hadoop.io.Writable;

/**
 * To communicate from the partition-based processing unit 
 * @author Jun Li
 *
 */
public class ProcessingUnitRuntimeInformation  implements Writable {
    private  int processId; //the process id for the 
    private  String machineIPAddress; //this is the public IP address registered for the machine;
    private  String privateIPAddress; //this is the private IP address assigned to this process unit;
    private  int partitionNumber; // the assigned partition number. 
    
    public ProcessingUnitRuntimeInformation  () {
    	
    }
    
    public ProcessingUnitRuntimeInformation  (int processId, String machineIPAddress, String privateIPAddress, int partitionNumber) {
    	this.processId = processId; 
    	this.machineIPAddress = machineIPAddress;
    	this.privateIPAddress = privateIPAddress;
    	this.partitionNumber = partitionNumber; 
    }
    
    public int getProcessId() {
    	return this.processId; 
    }
    
    public String getMachineIPAddress() {
    	return this.machineIPAddress;
    }
    
    public String getPrivateIPAddress() {
    	return this.privateIPAddress;
    }
    
    public int getPartitionNumber() {
    	return this.partitionNumber;
    }
    
    
	@Override
	public void readFields(DataInput in ) throws IOException {
		this.processId = in.readInt();
		
		//machineIPAddress
		int machineIPAddressLength = in.readInt();
		if (machineIPAddressLength == 0) {
			this.machineIPAddress = null;
		}
		else{
			byte[] bytes = new byte[machineIPAddressLength];
			in.readFully(bytes, 0, machineIPAddressLength);
			this.machineIPAddress = new String(bytes);
		}
		//privateIPAddress
		int privateIPAddressLength = in.readInt();
		if (privateIPAddressLength == 0) {
			this.privateIPAddress = null;
		}
		else{
			byte[] bytes= new byte[privateIPAddressLength];
			in.readFully(bytes, 0, privateIPAddressLength);
			this.privateIPAddress  = new String(bytes);
		}
		//partition number 
		this.partitionNumber = in.readInt();
		 
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeInt(processId);
		//for machine IP address
		if (machineIPAddress == null) {
			out.writeInt(0);
		}
		else {
		  byte[] machineIPAddressInBytes = machineIPAddress.getBytes();
		  out.writeInt(machineIPAddressInBytes.length);
		  out.write(machineIPAddressInBytes, 0, machineIPAddressInBytes.length);
		}
		//for privateIPAddress
		if (privateIPAddress == null) {
			out.writeInt(0);
		}
		else{
			byte[] privateIPAddressInBytes=privateIPAddress.getBytes();
			out.writeInt(privateIPAddressInBytes.length);
			out.write(privateIPAddressInBytes, 0, privateIPAddressInBytes.length);
		}
		
		out.writeInt(this.partitionNumber);
		 
	}

}
