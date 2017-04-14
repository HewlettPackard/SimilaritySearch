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
package common;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Writable;


public class TimeSeries implements Writable {

	public LongWritable id;
	public LongWritable start_time;
	public LongWritable interval;
	public ArrayWritable vals;
	Log log = LogFactory.getLog(TimeSeries.class);
	
	public TimeSeries() {
		
	}

	public TimeSeries(TimeSeries ts) {
		id = new LongWritable(ts.id.get());
		start_time = new LongWritable(ts.start_time.get());
		interval = new LongWritable(ts.interval.get());
		vals = new ArrayWritable(DoubleWritable.class);
		vals.set(ts.vals.get());
	}
	
	public void set(TimeSeries ts) {
		id = new LongWritable(ts.id.get());
		start_time = new LongWritable(ts.start_time.get());
		interval = new LongWritable(ts.interval.get());
		vals = new ArrayWritable(DoubleWritable.class);
		vals.set(ts.vals.get());		
	}
	
	@Override
	public void readFields(DataInput arg0) throws IOException {
		id = new LongWritable();
		start_time = new LongWritable();
		interval = new LongWritable();
		vals = new ArrayWritable(DoubleWritable.class);
		id.readFields(arg0);
		//log.info("id=" + id);
		start_time.readFields(arg0);
		//log.info("start_time=" + start_time);
		interval.readFields(arg0);
		//log.info("interval=" + interval);
		//log.info("size=" + arg0.readInt());
		vals.readFields(arg0);
		//log.info("vals=" + vals);
	}

	@Override
	public void write(DataOutput arg0) throws IOException {
		id.write(arg0);
		start_time.write(arg0);
		interval.write(arg0);
		vals.write(arg0);
	}
	@Override
	public String toString() {
		//Writable[] dvals = vals.get();
		//String str = "start_time=" + start_time + ",interval=" + interval + ",vals size=" + dvals.length;
		
		String str = "";
		DoubleWritable[] dvals = Utils.getDoubleArray(vals);
		//log.info("dvals=" + dvals.length);
		for(int i=0;i<dvals.length;i++) {
			//log.info("dvals[" + i + "]=" + dvals[i]);
			str += dvals[i].get() + " ";
		}
		
		
		return str;
	}

}
