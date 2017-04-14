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

import java.io.DataOutputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Random;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.junit.Assert;
import org.junit.Test;

import com.hp.hpl.palette.similarity.datamodel.TimeSeriesSearch;

import junit.framework.TestCase;


 
/**
 * to use Math.Sqrt function in the loop to simulate full CPU 30 ms. 
 *
 *
 */
public class TestOutputWithCorrectString extends TestCase {

	 private static final Log LOG = LogFactory.getLog(TestOutputWithCorrectString.class.getName());

	 
	 @Override
	 protected void setUp() throws Exception{ 
		  
		 super.setUp();
		 
	 }
	 
	 
	 /**
	  * To test one a particular CPU platform, how long it will take to produce 20 ms latency. 
	  */
	 @Test
	 public void testOutputWithString () throws IOException {
	 
		 DataOutputStream theKthResults = new DataOutputStream(new FileOutputStream("theKthResults.txt")); 
		
		 for (int i=0; i<5; i++){
		 
			 int partitionId = 100;
			 int q1 = 3;
			 int id=10000;
			 int offset=0;
			 float distance=(float) 0.102;
			 
			 String output =  partitionId + " " + q1+ " " + id + " " +   offset + " " +  distance + "\n";
			 
			 theKthResults.writeBytes(output); //the k-th result.
		 }
		 
		 
		 theKthResults.flush();
		 theKthResults.close();
	 }
	 
	 
	 @Override
	 protected void tearDown() throws Exception{ 
		 //do something first;
		 super.tearDown();
	 }
	 
	 public static void main(String[] args) {
		  
	      junit.textui.TestRunner.run(TestOutputWithCorrectString.class);
	 }
}



