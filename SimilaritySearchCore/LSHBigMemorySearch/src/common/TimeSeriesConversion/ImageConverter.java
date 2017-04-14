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
/**
 * 
 */
package common.TimeSeriesConversion;

import java.io.BufferedReader;
import java.io.DataOutputStream;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.util.StringTokenizer;

import org.apache.commons.logging.Log;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;

import common.TimeSeries;

/**
 * Class that map data from raw sources
 * into Timeseries data structure
 * 
 * This implementation is a local partition transformation
 * @author gomariat -Tere Gonzalez
 *
 */
public class ImageConverter {
	
	public ImageConverter(){
		
	}
	
	/**
	 * This class formats data from Image features to time series object
	 * where the features represents points of the vectors
	 * Image features does not have notion of time 
	 * but we assume that the image are for time 0 and interval 9
	 * @param inputFileName: input file name: text file , comma separted values for each image 
	 * it single line represents a "timeseries" points like:. 0.1 0.2 0.4..
	 * @param outFileName: output file name: binary file, time series objet for each image
	 * @param initialtsId: initial id for the timeseries, the algorihtm will generate 
	 * a consecutive sequence of unique ids as consecutive numbers.
	 */
	public void FromImageFeatures(String inputFileName, String outFileName,long initialTSIdentifier) {
							
			BufferedReader   in  = null;
			DataOutputStream out = null;			
			int        i = 0;
			long   tsID  = initialTSIdentifier;
			String line  = "";
			
			System.out.println("Open input file Name : "+ inputFileName);
			System.out.println("Open output file Name: "+ outFileName);
			
			
			if (initialTSIdentifier<1){
				System.out.println("The initial identifier should be > 1, enter a valid id number.");
				return;				
			}
			try{
				
				in  = new BufferedReader(new FileReader(inputFileName));		 		 				 				 
				out = new DataOutputStream(new FileOutputStream(outFileName));
				 
				 System.out.println("converting...");
				 
				 while ((line = in.readLine()) != null) {
					
					 //split the different values in the image feature
					 StringTokenizer     itr = new StringTokenizer(line.toString(),",");					 
					 DoubleWritable[] values = new DoubleWritable[itr.countTokens()];
					 
					 i=0; 
					 while(itr.hasMoreTokens()){
						 DoubleWritable val = new DoubleWritable(Double.parseDouble(itr.nextToken())); 
						 values[i]=val;
						 i++;
					 }
					 
					 TimeSeries ts = new TimeSeries();	 
										 
					 ts.id         = new LongWritable(tsID);					 					 				
					 ts.interval   = new LongWritable(0);  //single interval=0,- no time dimension
					 ts.start_time = new LongWritable(0);  //startime =0 , no time dimension
					 ts.vals       = new ArrayWritable(DoubleWritable.class);
					 ts.vals.set(values);
							 	
					 ts.write(out);
					 
					 tsID ++; //Get next id as a consecutive number
				 }
				    	
			    			 
				 System.out.println("closing the stream"); 
				 System.out.println("total timeseries transformed: "+(tsID-initialTSIdentifier));
				 System.out.println("Timeseries IDs generated from "+initialTSIdentifier +" to " +(tsID-1));
				 			 
			}catch(Exception e) {
				e.printStackTrace();
			}finally{
					
				if (in!=null)		
					try{
					 in.close();
					}catch(Exception e){
						//ignore
					}
				 if (out!=null)
					 try{
					 out.close();
					 }catch(Exception e){
						 //ignore
					 }				 
				}		
		}
	
	/**
	 * Method to start the timeseries id=0
	 * @param inputFileName: 
	 * @param outFileName
	 */
	public void FromImageFeatures(String inputFileName,String outFileName){
		
		FromImageFeatures(inputFileName,outFileName,1);
	}
}
