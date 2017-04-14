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
 * TimeSeriesBuilder.h
 *
 *  Created on: Dec 17, 2013
 *      Author: kimmij
 */
#ifndef TIMESERIESBUILDER_H_
#define TIMESERIESBUILDER_H_
#include <map>
#include "TimeSeries.h"
#include <google/malloc_extension.h>

using namespace std;

class TimeSeriesBuilder {
private:
       //map<int, TimeSeries> timeseries_map;
       vector<TimeSeries> timeseries_map;
       //pointer based compact representation 
       RealT **compact_timeseries;
       size_t ts_cnt;
       size_t* ts_length;
public:
       TimeSeriesBuilder();
       virtual ~TimeSeriesBuilder();

       bool find(int id);
       TimeSeries const& get(int id);
       void put(int id, TimeSeries const& timeseries);
       //map<int, TimeSeries>& getAllTimeSeries();
       vector<TimeSeries>& getAllTimeSeries(); 
       void print();

public: 
       void constructCompactTs(); 
       RealT *getCompactTs(int id) {
    	     return compact_timeseries[id];
       }
       size_t getCountAllTimeSeries(){
	     return ts_cnt;
       }
       size_t getTsLength(int i) {
             return ts_length[i];
       }
       void freeTsVector() {
                for(int i=0;i<timeseries_map.size();i++) {
                        TimeSeries &ts = timeseries_map[i];
            		ts.cleanup();
                }
		MallocExtension::instance()->ReleaseFreeMemory();
       };

       void serialize(string filename) {
                std::ofstream outfile(filename.c_str());
                for(int i=0;i<timeseries_map.size();i++) {
                        TimeSeries &ts = timeseries_map[i];
                        ts.writeFields(outfile);
                }
       };
       void deserialize(string filename) {
            std::ifstream infile(filename.c_str());
            std::string line;
            while (std::getline(infile, line)) {
                        TimeSeries ts;
                        bool res = ts.readFields(line);
                        timeseries_map.push_back(ts);
                        if(!res)
                                break;
                }
	    infile.close();

       };
       
};


#endif /* TIMESERIESBUILDER_H_ */
