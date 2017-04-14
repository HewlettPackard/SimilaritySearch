#!/bin/bash
# Script that executes LSH Search given a dataset, query and parameter configuration
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
# parameters of LSH Code
# 1. input dataset= ts_sample_55G #: path to the binary data set
# 2. input LSHparameters =lSH_Hash_part0_K19_L200_skip7R_55G #: xml computed LSH Parameters, R,L,K..
# 3. query file: path to the qeury file
# 4. output file: path to the results
# 5. L,R,p values to modify number of iterations of the LSH 
#	6.  size_R=4  : number of Rs read from parameter 2.
# 	7.  size_L=10 : maximum number of L
#       8.  top_k=5 : Number of Candiates
#	9.  start_L=1 : initial L
# 	10. end_L=10   : end L to search
# 	11. inc_L=1   : loop every inc_l
# 	12. start_p=10 : perturbations start
# 	13. end_p=30   : perturbations end
# 	14. inc_p=20   : loop every inc_p
#       15. use_serial=0: serialiation. ignored by now.
# Created On  Feb 20, 2014 by Tere Gonzalez

inputDataSet=./data/datasets/ts_sample_55G
hashFile=./data/hashes/LSH_HASH_K19_L104_55G_NEWR6_A
#hashFile=./data/hashes/LSH_Hash_part0_K19_L200_skip7R_55G
outputFile=./results/res1
#queryFile=./data/queries/queryfile7BA
#queryFile=./data/queries/queryfile_10_1bad
queryFile=./data/queries/experiment1_50/queryfile
#queryFile=./data/queries/query1
topK=5
sizeR=3
sizeL=100
startL=100
endL=100
incL=100
startP=50
endP=50
incP=50

export LD_LIBRARY_PATH=/opt/gcc/lib64

#NOTE: LSHBigMemorySearch.jar should be put at the first place, to allow its log4j to control the log output
java -Djava.library.path=/home/junli/LSH_CPLUS/Code/lshLibs/:/opt/gcc/lib64 -Xmx1024m -cp ./LSHBigMemorySearch.jar:./extlibs/hadoop-core-1.1.2.jar:./extlibs/log4j-1.2.12.jar:./extlibs/commons-lang-2.4.jar:./extlibs/commons-logging-1.0.4.jar:./extlibs/commons-logging-api-1.0.4.jar com.hp.hpl.palette.similarity.bridge.TestMultiThreadedLSHBuilder $inputDataSet $hashFile $queryFile $outputFile $sizeR $sizeL $topK $startL $endL $incL $startP $endP $incP 0




