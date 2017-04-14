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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
/**
 * The counterpart for the C++ TimeSeriesBuilder 
 * 
 * @author junli
 *
 */
public class TimeSeriesBuilderProxy {
	private static final Log LOG = LogFactory.getLog(TimeSeriesBuilderProxy.class.getName());
	
	/**
	 * Note that each method we will need to pass the manager instance, so that at the JNI implementation, from the pointer of the manager instance
	 * @param managerInstance the holder that points to the native LSH manager instance (pointer)
	 * 
	 * @param data the time series data that needs to be retrieved  from the HDFS and then stored to the in-memory partition.
	 */
	public boolean buildTimeSeries (LSHManagerProxy managerInstance, int id, float[] vals) {
		// check if vals are NaN
		for(int i=0;i<vals.length;i++) {
                        if(Float.isNaN(vals[i])) {
                                LOG.warn("NaN found id=[" + id + "], val=[" +  vals[i] + "]");
                                return false;
                        }
                }
		return this.buildTimeSeries(managerInstance.getPointer(), id, vals);
	}
	
	private native boolean buildTimeSeries(long managerPointer, int id, float[] arr);

	
}
