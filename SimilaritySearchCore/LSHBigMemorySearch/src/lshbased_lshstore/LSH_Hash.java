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
package lshbased_lshstore;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;

import lsh.LSHinf;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Writable;

import common.Measurements;
import common.Utils;


public class LSH_Hash implements Writable {

	//public LongWritable id = new LongWritable(); // partition id
	public ArrayList<LSHinf> rarray = new ArrayList<LSHinf>(); // R (radius) list that contains LSH
	int nPoints = 25728; // for test purpose
	int rcount = 10;     // for test purpose
	Log log = LogFactory.getLog(LSH_Hash.class);
	
	public LSH_Hash(int p, int r) {
		nPoints = p;
		rcount = r;
	}
	public LSH_Hash() {
	}

	public void set(LSH_Hash lsh_hash) {
		//id = lsh_hash.id;
		rarray = lsh_hash.rarray;
	}
	
	
	@Override
	public void readFields(DataInput arg0) throws IOException {
		nPoints = arg0.readInt();
		rcount = arg0.readInt();
		for(int i=0;i<rcount;i++) {
			LSHinf lsh = new LSHinf();	
			
			log.info("r=" + i + ",readFieldsLSH");
			int lenLSH = arg0.readInt();
			//int lenLSH = 1120264;
			log.info("lenLSH=" + lenLSH);
			byte[] dataLSH = new byte[lenLSH];
			arg0.readFully(dataLSH);
			lsh.pLSH = lsh.deserializeLSH(dataLSH);
			
			log.info("r=" + i + ",start readFieldsHash");
			int lenHash = arg0.readInt();
			//int lenHash = 64;
			log.info("lenHash=" + lenHash);
			byte[] dataHash = new byte[lenHash];
			arg0.readFully(dataHash);
			lsh.pHash = lsh.deserializeHash(nPoints, lsh.pLSH, dataHash);
			
			rarray.add(lsh);
			log.info("r=" + i + ",end readFieldsHash");
		}
	}

	@Override
	public void write(DataOutput arg0) throws IOException {
		// not used
	}
	
	
	
}
