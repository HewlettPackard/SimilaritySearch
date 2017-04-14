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

import com.hp.hpl.palette.similarity.worker.LSHIndexConfigurationParameters;
/**
 * It is a singleton object that has the C++ counter part. This is the only object pointer that we will need to hold at Java side, in order to 
 * get to the other data objects constructed in the C++. 
 * 
 * This is also the place that we will need to statically load the C/C++ native shared libraries. 
 * 
 * 
 * @author Jun Li
 *
 */
public class LSHManagerProxy {

	
	static 
	{
		System.loadLibrary("stdc++");
		System.loadLibrary( "LSH" );
		System.loadLibrary( "LSHSearchInf" );
	}
	
	private long  pointerToNativeManager; 
	
	public final static LSHManagerProxy INSTANCE= new LSHManagerProxy();
	
	//the pointer to the C++ counterpart.
	private long pointer; 
	
	private LSHManagerProxy() {
		//TODO: to get the LSHIndexConfigurationParameters from the concrete class of the LSHConfigurationManager 
		//then invoke the native method of "startLSHManager". 
		//pointer = 0; //need to be set from the actual invocation.
	}
	
	/**
	 * Start the C++ side LSH manager, and then returns the pointer that is then hold by the LSH Manager. Hopefully this is the only object pointer
	 * that needs to be passed between the Java and C++ boundaries. 
	 * 
	 * @return the pointer 
	 */
	private native long startLSHManager(int R_num, int L_num, int capacity);
	private native void setLSHHashStruct(int R_num, long pLSH, long pHash);
	private native void changeRval(int R_num, long pLSH, float Rval);
	private native void constructCompactTs();
	
	public long getPointer() {
		 return this.pointer; 
	}
	public long start(int R_num, int L_num, int capacity) 
	{
		pointer = startLSHManager(R_num,L_num,capacity);
		return pointer;
	}
	public void setLSH_HashFunction(int R_num, long pLSH, long pHash) {
		setLSHHashStruct(R_num, pLSH, pHash);
	}
	public void changeRvalue(int R_num, long pLSH, float Rval) {
		changeRval(R_num, pLSH, Rval);
	}	
	public void constructCompactTimeSeries() {
		constructCompactTs();
	}
	
}
