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
*/package common;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class TopKSearchReducer extends MapReduceBase
    implements Reducer<LongWritable, MaxHeap, MaxHeap, NullWritable> {

	Log log = LogFactory.getLog(TopKSearchReducer.class);
	
  MaxHeap topk = new MaxHeap();
  int k;
	@Override
	public void configure(JobConf job) {
      // Get the cached archives/files
      try {
          k = Integer.parseInt(job.get("topk", "5"));
			
		} catch (Exception e) {
			e.printStackTrace();
		}
  }
  public void reduce(LongWritable key, Iterator values,
      OutputCollector output, Reporter reporter) throws IOException {

	    while (values.hasNext()) {
	    	MaxHeap t = (MaxHeap)values.next();
	    	//log.info("reduce t=" + t);

    		Measurements mt;
	    	while((mt = t.poll()) != null) {
	    		if(topk.size() < k) {
	    			topk.add(mt);
	    		} else {
	    			Measurements maxsofar = topk.peek();
	    		
	    			//log.info("reduce maxsofar=" + maxsofar);
	    			//log.info("reduce mt=" + mt);
		    		
		    		if(maxsofar.distance < mt.distance) {
		    			continue;
		    		} else {
		    			topk.poll();
				    	topk.add(mt);
		    		}
	    		}
	    	}

	    	//log.info("reduce topk=" + topk);
	    }
	    //log.info("reduce topk=" + topk);
	    key = new LongWritable(1);
	    output.collect(topk, null);
  	}
}