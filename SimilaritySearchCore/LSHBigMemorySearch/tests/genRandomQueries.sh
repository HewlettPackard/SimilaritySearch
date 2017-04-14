#!/bin/bash
#/** 
#“© Copyright 2017  Hewlett Packard Enterprise Development LP
#Redistribution and use in source and binary forms, with or without modification, are permitted provided that the following #conditions are met:
#1. Redistributions of source code must retain the above copyright notice, this list of conditions and the following #disclaimer.
#2. Redistributions in binary form must reproduce the above copyright notice, this list of conditions and the following #disclaimer in the documentation and/or other materials provided with the distribution.
#3. Neither the name of the copyright holder nor the names of its contributors may be used to endorse or promote products #derived from this software without specific prior written permission.
#THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, #INCLUDING, BUT NOT LIMITED TO,
#WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER #OR CONTRIBUTORS BE LIABLE FOR ANY
#DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF #SUBSTITUTE GOODS OR SERVICES; LOSS OF
#USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, #STRICT LIABILITY, OR TORT (INCLUDING
#NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH #DAMAGE.”
#*/

# Script that generates random queries and computes the top-k distance by Naive Search  
# Created On  June 9, 2014 by Mijung Kim 



numPartitions=3 # number of partitions
inputSet=data/datasets/ts_sample_55G
outputfile=randomquery
querycnt=20
querylength=40
top_k=5
naive_result=naiveresult
for (( i = 1; i<=$numPartitions; i+=1 ))
do
	echo java -Xmx1024m -cp ./randomquery.jar:./extlibs/hadoop-core-1.1.2.jar:./extlibs/log4j-1.2.12.jar:./extlibs/commons-lang-2.4.jar:./extlibs/commons-logging-1.0.4.jar:./extlibs/commons-logging-api-1.0.4.jar com.hp.hpl.palette.similarity.bridge.RandomQuery ${inputSet}_$i ${outputfile}_$i $querycnt $querylength
	java -Xmx1024m -cp ./randomquery.jar:./extlibs/hadoop-core-1.1.2.jar:./extlibs/log4j-1.2.12.jar:./extlibs/commons-lang-2.4.jar:./extlibs/commons-logging-1.0.4.jar:./extlibs/commons-logging-api-1.0.4.jar com.hp.hpl.palette.similarity.bridge.RandomQuery ${inputSet}_$i ${outputfile}_$i $querycnt $querylength
	if [ "$i" -ge 1 ]; then
		echo cat ${outputfile}_$(( i - 1 )) '>>' ${outputfile}_$i
		cat ${outputfile}_$(( i - 1 )) >> ${outputfile}_$i
	fi
done

export LD_LIBRARY_PATH=/opt/gcc/lib64
for (( j = 1; j<=$numPartitions; j+=1 ))
do
	echo java -Djava.library.path=./lshLibs_prev:/opt/gcc/lib64 -Xmx1024m -cp ./LSHBigMemorySearch_test.jar:./extlibs/hadoop-core-1.1.2.jar:./extlibs/log4j-1.2.12.jar:./extlibs/commons-lang-2.4.jar:./extlibs/commons-logging-1.0.4.jar:./extlibs/commons-logging-api-1.0.4.jar com.hp.hpl.palette.similarity.bridge.NaiveSearchTest_OutputFile ${inputSet}_$j ${outputfile}_$numPartitions $top_k ${naive_result}_$j 
	java -Djava.library.path=./lshLibs_prev:/opt/gcc/lib64 -Xmx1024m -cp ./LSHBigMemorySearch.jar:./extlibs/hadoop-core-1.1.2.jar:./extlibs/log4j-1.2.12.jar:./extlibs/commons-lang-2.4.jar:./extlibs/commons-logging-1.0.4.jar:./extlibs/commons-logging-api-1.0.4.jar com.hp.hpl.palette.similarity.bridge.NaiveSearchTest_OutputFile ${inputSet}_$j ${outputfile}_$numPartitions $top_k ${naive_result}_$j 
	if [ "$j" -ge 1 ]; then
		echo cat ${naive_result}_$(( j - 1 )) '>>' ${naive_result}_$j
		cat ${naive_result}_$(( j - 1 )) >> ${naive_result}_$j
	fi
done
