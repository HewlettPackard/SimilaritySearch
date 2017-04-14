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
 
import java.util.ArrayList;
import java.util.Random;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.junit.Assert;
import org.junit.Test;
import junit.framework.TestCase;


 
/**
 * to use Math.Sqrt function in the loop to simulate full CPU 30 ms. 
 *
 *
 */
public class SimpleEvaluationOn20MSFullCPUTest extends TestCase {

	 private static final Log LOG = LogFactory.getLog(SimpleArrayListInitialCapacityTest.class.getName());
	 int counter = 1250000;
	 
	 @Override
	 protected void setUp() throws Exception{ 
		  
		 super.setUp();
		 
	 }
	 
	 
	 /**
	  * To test one a particular CPU platform, how long it will take to produce 20 ms latency. 
	  */
	 @Test
	 public void testTimeSpentOn20Ms () {
	 
		 int loopCounter=20; 
		 for (int loop=0; loop<loopCounter; loop++)  {
			 Random rand = new Random (System.currentTimeMillis());
			 
			 long startTime = System.currentTimeMillis();
			 for (int i=0; i<counter;i++) {
				 double val = rand.nextDouble();
				 double result = Math.sqrt(val);
			 }
			 
			 long endTime = System.currentTimeMillis();
			 long timeDifference = endTime -startTime;
			 
			 LOG.info("on loop: " + loop + "after computing sqrt for : " + counter + " times" + " the total time spent is: " + timeDifference + " (ms) ");
		 }
		
	 }
	 
	 
	 @Override
	 protected void tearDown() throws Exception{ 
		 //do something first;
		 super.tearDown();
	 }
	 
	 public static void main(String[] args) {
		  
	      junit.textui.TestRunner.run(SimpleEvaluationOn20MSFullCPUTest.class);
	 }
}


