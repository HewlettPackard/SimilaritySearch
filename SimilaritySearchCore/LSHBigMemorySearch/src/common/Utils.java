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

import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Writable;

public class Utils {
	public static DoubleWritable[] getDoubleArray(ArrayWritable vals) {
		int l = vals.get().length;
	  	Writable[] arr = vals.get();
	  	DoubleWritable[] arrVals = new DoubleWritable[l];
	  	for(int i=0;i<l;i++) {
	  		arrVals[i] = (DoubleWritable)arr[i];
	  	}
	  	return arrVals;
	}
	public static ArrayWritable[] getArrayArray(ArrayWritable vals) {
		int l = vals.get().length;
	  	Writable[] arr = vals.get();
	  	ArrayWritable[] arrVals = new ArrayWritable[l];
	  	for(int i=0;i<l;i++) {
	  		arrVals[i] = (ArrayWritable)arr[i];
	  	}
	  	return arrVals;
	}
	public static IntWritable[] getIntArray(ArrayWritable vals) {
		int l = vals.get().length;
	  	Writable[] arr = vals.get();
	  	IntWritable[] arrVals = new IntWritable[l];
	  	for(int i=0;i<l;i++) {
	  		arrVals[i] = (IntWritable)arr[i];
	  	}
	  	return arrVals;
	}

}
