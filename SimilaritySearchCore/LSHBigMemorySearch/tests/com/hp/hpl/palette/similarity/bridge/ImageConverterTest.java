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
package com.hp.hpl.palette.similarity.bridge;

import java.io.IOException;

import common.TimeSeriesConversion.ImageConverter;


/**
 * Test class to execute TimeSeries Formatter 
 * from Image to timeseries data structure
 * This version assumes a local copy of the files.
 * @author gomariat
 *
 */
public class ImageConverterTest {
	
	/**
     * Calls the Formatter class that 
     * transform the image features into a timeseries objects
     * @param args 0: input file 1: output file name, 2: the id for the timeseries
     */
	public void ConvertFromImageFeature(String[] args){
			
		try{
		ImageConverter a = new ImageConverter();
		
	
		if (args.length<1){
			System.out.println("ImageConverterTest <inputFileName> <outputFileNaive> <initial id =1 for the timeseries list>");
			return;
		}
		
		System.out.println("Starting........."); 
		if (args.length<3){
			a.FromImageFeatures(args[0],args[1]);	//no time series id provided.		
		}
		else {
			a.FromImageFeatures(args[0],args[1], Long.parseLong(args[2]));	
		}
												
		System.out.println("finishing.........");
		}
		catch(Exception e){
			System.out.println("Error occurred runnning conversion."+e.getStackTrace());
		}
	}

	/**
	 * Main program
	 * @param args: 0 input file 1: output file name 2: initial number of the id 
	 * @throws NumberFormatException
	 * @throws IOException
	 */
	public static void main(String[] args) {
							
		ImageConverterTest imageConversion = new ImageConverterTest();
		imageConversion.ConvertFromImageFeature(args);
				
	}

}