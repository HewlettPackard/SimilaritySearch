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

package lsh;

import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.JobConf;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;


// LSH java native interface
public class LSHinf {
	
	static 
	{
		System.loadLibrary("stdc++");
		System.loadLibrary( "LSH" );
		System.loadLibrary( "LSHSearchInf" );
	}
	
	// create LSH struct and return the pointer of the struct 
	public native long initLSH();  

	// create Hash indices and return the pointer of the Hash index
	public native long initHash(int bucketcnt, long lptrLSHstruct);  

	// deserialize LSH struct and return the pointer of the struct 
	public native long deserializeLSH(byte[] data);  

	// deserialize Hash indices and return the pointer of the Hash index
	public native long deserializeHash(int bucketcnt, long lptrLSHstruct, byte[] data);  
	
	// find h1 and h2 indices for Hash index 
	public native void getHashIndex(long lptrLSHstruct, long lptrHashIndex, double[] points, int[] h1, int[] h2, int printout);
	
	// return npert_max choose npert
	public native long getPerturbationCnt(long lptrLSHstruct, long lptrHashIndex, int npert);
		
	// generate perturbation vectors and find h1 and h2 indices 
	public native void getPerturbationIndices(long lptrLSHstruct, long lptrHashIndex, double[] points, int[] h1, int[] h2, int printout, int npert);
		
	// return size of LSH
	public native int getSizeSerializedLSH(long lptrLSHstruct);
	
	// return size of Hash
	public native int getSizeSerializedHash(long lptrHashIndex);
	
	// serialize LSH struct
	public native void serializeLSH(long lptrLSHstruct, String filename);

	// serialize Hash indices
	public native void serializeHash(long lptrHashIndex, String filename);
	
	// clean up the Hash index
	public native void cleanup(long lptrLSHstruct, long lptrHashIndex);
	
	// estimate parameters
	//public native long estimate(double R, double successProbability, int nPoints, int dimension, int arrlen, double[][] ts);


	public LSHinf(double paramR) {
		parameterR = paramR;
		parameterR2 = Math.pow(parameterR,2);
	}
	public LSHinf() {
	}
	public LSHinf(JobConf conf) {
		parameterR = Double.parseDouble(conf.get("lsh.parameterR","0.9"));
		parameterR2 = Math.pow(parameterR,2);
		useUfunctions = Integer.parseInt(conf.get("lsh.useUfunctions","1"));
		parameterK = Integer.parseInt(conf.get("lsh.parameterK","16"));
		parameterL = Integer.parseInt(conf.get("lsh.parameterL","231"));
		parameterM = Integer.parseInt(conf.get("lsh.parameterM","22"));
		parameterT = Integer.parseInt(conf.get("lsh.parameterT","17481"));
		parameterW = Double.parseDouble(conf.get("lsh.parameterW","4.000000000"));
		nPoints = Integer.parseInt(conf.get("lsh.tablesize","17481"));
		
	}
	public LSHinf(double paramR, JobConf conf) {
		parameterR = paramR;
		parameterR2 = Math.pow(parameterR,2);
		useUfunctions = Integer.parseInt(conf.get("lsh.useUfunctions","1"));
		parameterK = Integer.parseInt(conf.get("lsh.parameterK","16"));
		parameterL = Integer.parseInt(conf.get("lsh.parameterL","231"));
		parameterM = Integer.parseInt(conf.get("lsh.parameterM","22"));
		parameterT = Integer.parseInt(conf.get("lsh.parameterT","17481"));
		parameterW = Double.parseDouble(conf.get("lsh.parameterW","4.000000000"));
		nPoints = Integer.parseInt(conf.get("lsh.tablesize","17481"));
		
	}
	public LSHinf(Configuration conf) {
		parameterR = Double.parseDouble(conf.get("lsh.parameterR","0.9"));
		parameterR2 = Math.pow(parameterR,2);
		useUfunctions = Integer.parseInt(conf.get("lsh.useUfunctions","1"));
		parameterK = Integer.parseInt(conf.get("lsh.parameterK","16"));
		parameterL = Integer.parseInt(conf.get("lsh.parameterL","231"));
		parameterM = Integer.parseInt(conf.get("lsh.parameterM","22"));
		parameterT = Integer.parseInt(conf.get("lsh.parameterT","17481"));
		parameterW = Double.parseDouble(conf.get("lsh.parameterW","4.000000000"));
		nPoints = Integer.parseInt(conf.get("lsh.tablesize","17481"));
		
	}
	public long pLSH;
	public long pHash;
	public double parameterR = 0.9;
	public double parameterR2 = 0.81;
	public int useUfunctions = 1;
	public int parameterK = 16;
	public int parameterL = 1;
	public int parameterM = 5;
	public int parameterT = 17481;
	public int dimension = 40;
	public double parameterW = 4.000000000;
	public int nPoints = 17481;
	public int nperturbation = 1;
	  
	public static void main(String[] args) throws IOException {

		LSHinf lsh = new LSHinf();
		//double[] points = new double[lsh.dimension];
		/*
		for(int i=0;i<lsh.dimension;i++) {
			 points[i] = Math.random();
			 System.out.println("points[" + i + "]=" + points[i]);
		}
		*/
		
		double query [] = {0.1050000,	0.1010000,	0.5880000,	0.2510000,	0.3380000,	0.1170000,	0.2120000,	0.1190000,	0.1800000,	0.1860000,	0.0326000,	0.0322000,	0.1110000,	0.1020000,	0.4280000,	0.3900000,	0.2390000,	0.2030000,	0.0589000,	0.0613000,	0.0582000,	0.1050000,	0.0150000,	0.0570000,	0.0888000,	0.0781000,	0.1100000,	0.0731000,	0.1310000,	0.0936000,	0.1070000,	0.1000000,	0.1840000,	0.7590000,	0.8550000,	0.7680000,	0.4720000,	0.2340000,	0.2480000,	0.7790000};
		//double points [] = {0.151,0.205,0.177,0.165,0.0792,0.0521,0.117,0.178,0.272,0.0107,0.103,0.1,0.0663,0.0216,0.0197,0.024,0.0245,0.0368,0.291,0.25,0.339,0.213,0.129,0.0802,0.0943,0.0752,0.00885,0.0815,0.11,0.0847,0.346,0.375,0.248,0.265,0.278,0.114,0.402,0.444,0.21,0.676};
		double points [] = {0.101,0.0587,0.158,0.218,0.212,0.186,0.122,0.13,0.32,0.17,0.246,0.114,0.207,0.0662,0.0781,0.0669,0.0754,0.219,0.162,0.209,0.155,0.119,0.12,0.189,0.115,0.115,0.127,0.0668,0.0419,0.107,0.104,0.072,0.193,0.837,0.888,0.667,0.341,0.447,0.385,0.639};

		/*
		BufferedReader br = new BufferedReader(new FileReader("/home/kimmij/E2LSH-0.1/points.txt"));
		String sCurrentLine;
		int idx=0;
		while ((sCurrentLine = br.readLine()) != null) {
			points[idx] = Double.parseDouble(sCurrentLine);
			//System.out.println("points[" + idx + "]=" + points[idx]);
			idx++;
		}
		*/

		long pLSH = lsh.initLSH();
		//System.out.println("ptr=" + ptr);
		long pHash = lsh.initHash(lsh.nPoints, pLSH);
		
		lsh.serializeLSH(pLSH, "LSH.bin");
		lsh.serializeHash(pHash, "Hash.bin");
		
		int[] hIndex = new int[lsh.parameterL];
		int[] control1 = new int[lsh.parameterL];
		lsh.getHashIndex(pLSH, pHash, points, hIndex, control1, 1);

		for(int i=0;i<lsh.parameterL;i++) {
			 System.out.println("h1[" + i + "]=" + hIndex[i]);
			 System.out.println("h2[" + i + "]=" + control1[i]);
		}
		lsh.cleanup(pLSH, pHash);
		System.out.println("cleanup done");
		
		//for(int i=0;i<lsh.parameterL;i++) {
		//	hIndex[i]=0;
		//	control1[i]=0;
		//}
		int[] qhIndex = new int[lsh.parameterL];
		int[] qcontrol1 = new int[lsh.parameterL];
		
        // create new data input stream
		DataInputStream lshin = new DataInputStream(new FileInputStream("LSH.bin"));
        // available stream to be read
        int length = lshin.available();
        // create buffer
        byte[] lshbuf = new byte[length];
        // read the full data into the buffer
        lshin.readFully(lshbuf);
        long pNewLSH = lsh.deserializeLSH(lshbuf);
        System.out.println("deserializeLSH done");
        // create new data input stream
		DataInputStream hashin = new DataInputStream(new FileInputStream("Hash.bin"));
        // available stream to be read
        length = hashin.available();
        // create buffer
        byte[] hashbuf = new byte[length];
        // read the full data into the buffer
        hashin.readFully(hashbuf);
        long pNewHash = lsh.deserializeHash(lsh.nPoints, pNewLSH, hashbuf);
        System.out.println("deserializeHash done");
		
        lsh.getHashIndex(pNewLSH, pNewHash, query, qhIndex, qcontrol1, 1);
        for(int i=0;i<lsh.parameterL;i++) {
			 System.out.println("2 h1[" + i + "]=" + qhIndex[i]);
			 System.out.println("2 h2[" + i + "]=" + qcontrol1[i]);
		}
    	for(int i=0;i<lsh.parameterL;i++) {
    		if(hIndex[i] == qhIndex[i] && control1[i] == qcontrol1[i]) {
    			System.out.println("MATCH [" + i + "]" + hIndex[i] + "," + control1[i]);
    		}
    	}
    	
		lsh.cleanup(pNewLSH, pNewHash);
	}
}
