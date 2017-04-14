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
package com.hp.hpl.palette.similarity.job;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

import com.hp.hpl.palette.similarity.worker.IdleCommunicator;

// thread #1
// The class is check timer thread (jdk). context object is passed and increment the counter of the context
public class TimeSeriesSearchCommunicator implements IdleCommunicator, Runnable {

	private Reporter reporter;
	private static enum TimeSeriesSearchCommunicatorCounters { COMMUNICATOR_COUNTER };
	private long timeMilliseconds;
	
	private static final Log LOG = LogFactory.getLog(TimeSeriesSearchCommunicator.class);
	
	public void setReporter(Reporter r) {
		reporter = r;
	}
	@Override
	public void setIdleProgressPeriod(long time) {
		
		timeMilliseconds = time;
		
	}

	@Override
	public void run() {

		while(true) {
			reporter.incrCounter(TimeSeriesSearchCommunicatorCounters.COMMUNICATOR_COUNTER, 1); 
		    try {
				Thread.sleep(timeMilliseconds);
			} catch (InterruptedException ex) {
				LOG.error("Time Series Search communicator to send progress report to Job Tracker fails...", ex);
				//we will ignore the error.
				continue;
			}
        }
		
	}

}
