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
import java.util.PriorityQueue;
import java.util.Iterator;

import org.apache.hadoop.io.Writable;


public class MaxHeap extends PriorityQueue<Measurements> implements Writable {

	public void readFields(DataInput arg0) throws IOException {
		int heapSize = arg0.readInt();
		for(int i=0;i<heapSize;i++) {
			Measurements ms = new Measurements();
			ms.readFields(arg0);
			add(ms);
		}
	}

	@Override
	public void write(DataOutput arg0) throws IOException {
		arg0.writeInt(this.size());
		Iterator<Measurements> it = this.iterator();
		while(it.hasNext()) {
			Measurements ms = it.next();
			ms.write(arg0);
		}
	}
	public String toString() {
		int n = this.size();
		StringBuilder builder = new StringBuilder();
		for(int i=0;i<n;i++) {
			Measurements ms = this.poll();
			builder.append(ms.toString());
		}
		return builder.toString();
	}
}
