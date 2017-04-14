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
 * TimeSeries.cpp
 *
 *  Created on: Dec 17, 2013
 *      Author: kimmij
 */

#include "TimeSeries.h"
#include <stdio.h>
#include <string.h>
#include <fstream>

TimeSeries::TimeSeries() {

}

TimeSeries::TimeSeries(int id, vector<RealT> vals) : m_id(id), m_vals(vals) {

}

TimeSeries::~TimeSeries() {
	// TODO Auto-generated destructor stub
}

bool TimeSeries::writeFields(std::ofstream &outfile) {

        outfile << m_id << "," << m_vals[0];
        for(int i=1;i<m_vals.size();i++) {
            outfile <<  "," << m_vals[i];
        }
        outfile << endl;
}

bool TimeSeries::readFields(string line) {

	char* str = (char*)line.c_str();
	char* pch = strtok (str,",");
	m_id = atoi(pch);
	//printf ("id=%s\n",pch);
	pch = strtok (NULL, ",");
	int i=0;
	while (pch != NULL)
	{
		m_vals.push_back(atof(pch));
		pch = strtok (NULL, ",");
	}
/*
	int result;
	result = fread (&(id) , sizeof(long) , 1 , pFile);
	if(result != sizeof(long))
		return false;
	result = fread (&(start_time) , 1, sizeof(long) , pFile);
	if(result != sizeof(long))
		return false;
	result = fread (&(interval) , 1, sizeof(long) , pFile);
	if(result != sizeof(long))
		return false;
	int sz;
	result = fread (&(sz) , 1, sizeof(int) , pFile);
	if(result != sizeof(int))
			return false;

	for(int i=0;i<sz;i++) {
		result = fread (&(vals[i]) , 1, sizeof(RealT) , pFile);
		if(result != sizeof(RealT))
			return false;

	}
	*/
	return true;
}


