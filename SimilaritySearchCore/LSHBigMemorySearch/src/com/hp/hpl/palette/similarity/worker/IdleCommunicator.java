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
package com.hp.hpl.palette.similarity.worker;

/**
 * When the Map instance (process) is not doing the work of index building, or searching, there is no actual progress to be sent 
 * to the Job Tracker for MapReduce. So we will rely on this Idel Communicator to make sure even the system is not doing actual work, 
 * there is still some "idle" progress to  be sent to the Job Tracker, so that the Job Tracker will determine that the Map instance is
 * still alive. 
 * 
 * Note that we will need to get from MapReduce the "context" instance, so that the idle communicator can send the progress periodically to
 * the Job Tracker.
 * 
 * 
 * @author Jun Li
 *
 */
public interface IdleCommunicator {

	/**
	 * we can set it say, 2 minutes.
	 * TODO: for the implementation, we will need to have the Hadoop "context" to be passed in, so that we can issue progress report periodically to
	 * the Hadoop Job Tracker from the Map instance. 
	 * 
	 * @param timeMilliseconds
	 */
	void setIdleProgressPeriod (long timeMilliseconds);
}
