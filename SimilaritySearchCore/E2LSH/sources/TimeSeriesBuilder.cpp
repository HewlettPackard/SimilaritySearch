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
 * TimeSeriesBuilder.cpp
 *
 *  Created on: Dec 17, 2013
 *      Author: kimmij
 */

#include "TimeSeriesBuilder.h"
#include <fstream>

TimeSeriesBuilder::TimeSeriesBuilder() {


}

TimeSeriesBuilder::~TimeSeriesBuilder() {
	// TODO Auto-generated destructor stub
}

//map<int, TimeSeries>& TimeSeriesBuilder::getAllTimeSeries() {
vector<TimeSeries>& TimeSeriesBuilder::getAllTimeSeries() {

	return timeseries_map;
}

void TimeSeriesBuilder::print() {
	/*
	for(map<int, TimeSeries>::iterator iterator = timeseries_map.begin(); iterator != timeseries_map.end(); iterator++) {
		TimeSeries &ts = iterator->second;
		int id = ts.getId();
		printf("Timeseries id[%d],vals[0][%f]\n", id, ts.getValues().at(0));

	}
	fflush(stdout);
*/
}

TimeSeries const& TimeSeriesBuilder::get(int id) {

	return timeseries_map[id];
}

bool TimeSeriesBuilder::find(int id) {
	/*
	if ( timeseries_map.find(id) == timeseries_map.end() )
		return false;
	else
		return true;
		*/
	return true;
}

void TimeSeriesBuilder::put(int id, TimeSeries const& timeseries)  {
	//timeseries_map[id] = timeseries;

	timeseries_map.push_back(timeseries);
}


// construct 2-d pointer array of candidates
void TimeSeriesBuilder::constructCompactTs() {

// this is only used by serializatin the input data, which is used by LSHtest
#ifdef SERIALIZE_INPUT
        cout << "start serializing" << endl;
        serialize("ts_file4M_part1");
        cout << "finish serializing" << endl;
#endif

  this->ts_cnt = timeseries_map.size();
cout << "start constructCompactTs, ts_cnt=" << ts_cnt << endl;
  this->compact_timeseries= (RealT**)malloc(this->ts_cnt*sizeof(RealT*));
  this->ts_length = (size_t*)malloc(this->ts_cnt*sizeof(size_t)); 
  for (int i=0; i<this->ts_cnt; i++) {
    TimeSeries &ts=timeseries_map[i];
    vector<RealT> const &vals = ts.getValues();
    int columnSize = vals.size();
    this->compact_timeseries[i]=(RealT*)malloc(columnSize*sizeof(RealT));
    for (int j=0; j<columnSize; j++) {
      this->compact_timeseries[i][j] = vals.at(j);
    }
    this->ts_length[i] = columnSize;
  }
// free values (Note that only values are freed, 'id' is still used)
cout << "start freeTsVector" << endl;
  freeTsVector();
cout << "end freeTsVector" << endl;
}
