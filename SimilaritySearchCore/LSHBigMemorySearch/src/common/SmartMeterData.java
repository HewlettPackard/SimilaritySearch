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

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.WritableComparable;


public class SmartMeterData implements WritableComparable<SmartMeterData> {

	public LongWritable time;
	public DoubleWritable val;

	public SmartMeterData() {
	}
	public SmartMeterData(SmartMeterData data) {
		time = new LongWritable(data.time.get());
		val = new DoubleWritable(data.val.get());
	}
	@Override
	public void readFields(DataInput arg0) throws IOException {
		time = new LongWritable();
		val = new DoubleWritable();
		
		time.readFields(arg0);
		val.readFields(arg0);
	}

	@Override
	public void write(DataOutput arg0) throws IOException {
		time.write(arg0);
		val.write(arg0);
	}

	@Override
	public int compareTo(SmartMeterData arg0) {
		long t1 = time.get();
		long t2 = arg0.time.get();
 		if(t1 > t2)
 			return 1;
 		else if(t1 < t2)
 			return -1;
 		else
 			return 0;
	}
	public String toString() {
		return "time=" + time + ",val=" + val;
	}

}
