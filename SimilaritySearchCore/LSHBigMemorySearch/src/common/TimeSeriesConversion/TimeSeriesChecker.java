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
package common.TimeSeriesConversion;

import java.io.DataInputStream;
import java.io.FileInputStream;


import org.apache.hadoop.io.DoubleWritable;

import com.hp.hpl.palette.similarity.bridge.TimeSeriesBuilderProxy;

import common.TimeSeries;
import common.Utils;

/**
 * Class to implement check on the binary partitions
 * @author gomariat -Tere Gonzalez
 *
 */
public class TimeSeriesChecker {
	
	
		/** to check whether a particular key does exist in a particular partition
		 * 
		 * @param fileName the partition file name
		 * @param tsId the time series id to check the existence
		 * @return
		 */
		public boolean checkIdExistence(String fileName, int tsId) {
			boolean result=false;
			long  recordCount = 0 ;
		    int key = 0;
		
			TimeSeries data = new TimeSeries(); 
									
			DataInputStream fileTS;
			try {
				fileTS = new DataInputStream(new FileInputStream(fileName));
			
				
				System.out.println("Checking Partition: "+ fileName);
				System.out.println("time series id to check: "+ tsId);
				while (fileTS.available() != 0) {
					 recordCount++;	
					 
					 if (recordCount%100000==0) {
						 System.out.println("currently checking record count: " + recordCount);
					 }
					 
					 data.readFields(fileTS); // copied from HDFS
						
					 	key = (int)data.id.get();
					 
						if (key == tsId)  //check if the id %10 belongs to the partitionid
						{
							result=true;
							System.out.println("Identified id: "+  tsId  +" on partition: "+ fileName);
							break;
						}					
				}
			
			    System.out.println("Check Done...........");
			    System.out.println("Total records read:"+recordCount);
			    if (!result){
			    	System.out.println("can not identify id: "+  tsId  +" on partition: "+ fileName);
			    }
							
		    } catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			
			return result;						
		}
		
		/**
		 * Method to check if the ids in the module of 10 parition  id
		 *  
		 * @param fileName
		 * @param idx
		 * @return
		 */
		public boolean checkId(String fileName, int idx) {
			
			long  recordCount = 0 ;
			long  errorCount = 0 ;
			int key = 0;
			int mod = 0;
			
			
			TimeSeries data = new TimeSeries(); 
									
			DataInputStream fileTS;
			try {
				fileTS = new DataInputStream(new FileInputStream(fileName));
			
				
				System.out.println("Checking Partition:"+ idx);
				while (fileTS.available() != 0) {
					 recordCount++;	
					 data.readFields(fileTS); // copied from HDFS
						
					 	key = (int)data.id.get();
						mod = key%10;
						if (mod != idx)  //check if the id %10 belongs to the partitionid
						{
							System.out.println("Invalid TSID:"+ key +" on partition: "+ idx );
							errorCount++;
						}					
				}
			
			    System.out.println("Check Done...........");
			    System.out.println("Total records read:"+recordCount);
			    System.out.println("Total error found:"+errorCount);
							
		    } catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			
			return true;						
		}
}
