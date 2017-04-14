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

public class IndexBuilderProxy {

	/**
	 * To populate the entire Hash table that corresponds to the specified R and specified L, for the time series data that has been loaded via
	 * the time series builder. the time series data can be retrieved via the pointer to the LSH Manager.
	 * 
	 * This can be potentially a long and CPU intensive operation, and will be devoted by a designated thread via the Index builder scheduler.
	 * 
	 * @param managserInstance
	 * @param Rindex
	 * @param L_low: low number of L
	 * @param L_high: high number of L
	 * @param querylength
	 * @return
	 */
	public boolean buildIndex(int Rindex, int L_low, int L_high, int querylength) {
		LSHManagerProxy managserInstance = LSHManagerProxy.INSTANCE;
		return this.buildIndex(managserInstance.getPointer(), Rindex, L_low, L_high, querylength);
	}
	
	private native boolean buildIndex(long lshManagerPointer, int RIndex, int L_low, int L_high, int querylength); 
	
	
	// Added by Mijung
	// set hash table type
	// TWO_ARRAYS_BASED: 0, UNORDERED_MAP: 1 (default: TWO_ARRAYS_BASED)
	public static int TWO_ARRAYS_BASED = 0;
	public static int UNORDERED_MAP = 1;
	public void setHashTableType(int hashtable_type) {
		LSHManagerProxy managserInstance = LSHManagerProxy.INSTANCE;
		this.setHashTableType(managserInstance.getPointer(), hashtable_type);
	}
	private native void setHashTableType(long lshManagerPointer, int hashtable_type); 
	
	
	/** to serialize/deserialize the index that has been built in memory to/from the persistent file system. 
	 * 
	 * @param managserInstance
	 */
	public void serialize() {
		LSHManagerProxy managserInstance = LSHManagerProxy.INSTANCE;
		serialize(managserInstance.getPointer());
	}
	
	private native void serialize(long lshManagerPointer);
	
	public void deserialize() {
		LSHManagerProxy managserInstance = LSHManagerProxy.INSTANCE;
		deserialize(managserInstance.getPointer());
	}
	
	private native void deserialize(long lshManagerPointer);
}
