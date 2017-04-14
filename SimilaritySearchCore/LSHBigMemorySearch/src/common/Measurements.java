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
import java.util.ArrayList;

import org.apache.commons.lang.builder.HashCodeBuilder;
import org.apache.hadoop.io.Writable;


public class Measurements implements Writable,  Comparable<Measurements> {
	public long user_idx;
	public int start_idx;
	public int length;
	public ArrayList<Double> data = new ArrayList<Double>();
	public double distance;
	public double R;
	public int R_idx;
	
	public Measurements() {
		
	}
	public Measurements(Measurements m) {
		user_idx = m.user_idx;
		start_idx = m.start_idx;
		length = m.length;
		for(int i=0;i<m.data.size();i++) {
			data.add(m.data.get(i));
		}
		distance = m.distance;
		R = m.R;
		R_idx = m.R_idx;
	}
	public void readFields(DataInput arg0) throws IOException {
		user_idx = arg0.readLong();
		start_idx = arg0.readInt();
		length = arg0.readInt();
		for(int i=0;i<length;i++) {
			data.add(arg0.readDouble());
		}
		distance = arg0.readDouble();
		R = arg0.readDouble();
		R_idx = arg0.readInt();
	}

	@Override
	public void write(DataOutput arg0) throws IOException {
		arg0.writeLong(user_idx);
		arg0.writeInt(start_idx);
		arg0.writeInt(length);
		for(int i=0;i<length;i++) {
			arg0.writeDouble(data.get(i));
		}
		arg0.writeDouble(distance);
		arg0.writeDouble(R);
		arg0.writeInt(R_idx);
	}
	
	public String toString() {
		StringBuilder builder = new StringBuilder();
		builder.append(user_idx + "," + start_idx + ",");
		//for(int i=0;i<data.size();i++) {
		//	builder.append(data.get(i) + " ");
		//}
		builder.append(data.toString() + ",");
		builder.append(distance);
		//builder.append(distance + "," + R_idx + "," + R);
		
		return builder.toString() + "\n";
	}
	public int compareTo(Measurements ms) {
		
		if(this.distance > ms.distance)
			return -1;
		else if(this.distance < ms.distance)
			return 1;
		else return 0;
	}
	@Override
	public boolean equals(Object obj) {
		
		if (!( obj instanceof Measurements )) {
			return false;
		}	

		Measurements record = (Measurements) obj;
		
		return (record.user_idx == user_idx && record.start_idx == start_idx);
		
	} 
	
	// Added by Mijung
	@Override
    public int hashCode() {
		HashCodeBuilder hash = new HashCodeBuilder(17, 31);
		hash.append(user_idx);
		hash.append(start_idx);
		return hash.toHashCode();
        /*
		int prime = 31;
        int hash = 1;
        hash = prime * hash + this.id;
        hash = prime * hash + this.offset;
        return hash;
        */
    }
}
