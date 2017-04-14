/** Similarity Search
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
/*
 * LSHManager.h
 *
 *  Created on: Jan 3, 2014
 *      Author: kimmij
 */

#ifndef LSH_H_
#define LSH_H_




class LSHGlobalConstants {
public:
        //static const unsigned int TOTAL_NUM_TIMESERIES_POINTS = 172944;
	static const unsigned int TOTAL_NUM_TIMESERIES_POINTS = 4000000;
	static const unsigned int NUM_TIMESERIES_POINTS = 1; //=17250/48
	static const unsigned int POS_TIMESERIES = 384;

	enum {TWO_ARRAYS_BASED, UNORDERED_MAP};

};


class LSHPerformanceCounter {

private:
	int totalPointsInFinalNaiveSearch;
	int totalTimeSpentInPertubation;  //milliseconds;
	int totalTimeSpentInNaiveSearch;  //milliseconds

public:
	inline void setTotalPointsInFinalNaiveSearch(int val) {
		totalTimeSpentInNaiveSearch = val;
	};

	inline int getTotalTimeSpentInFinalNaiveSearch() const {
		return totalTimeSpentInNaiveSearch;
	};

	void reset() {
		totalTimeSpentInNaiveSearch = 0;
		totalTimeSpentInPertubation = 0;  //milliseconds;
	};
};



#endif /* LSH_H_ */
