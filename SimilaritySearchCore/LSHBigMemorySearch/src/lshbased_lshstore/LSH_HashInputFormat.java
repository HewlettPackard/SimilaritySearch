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

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;


public class LSH_HashInputFormat extends FileInputFormat<LongWritable, LSH_Hash> {


	Log log = LogFactory.getLog(LSH_HashInputFormat.class);
	@Override
    public RecordReader<LongWritable, LSH_Hash> getRecordReader(InputSplit inputSplit, JobConf job, Reporter reporter) throws IOException {
    	FileSplit split = (FileSplit) inputSplit;
        Path file = split.getPath();
        log.info("filename=" + file.getName());
        String[] str = file.getName().split("-");
        int part = Integer.parseInt(str[1]);
        FileSystem fs = file.getFileSystem(job);
        
        return new LSH_HashRecordReader(file, fs, part);
    }

    protected boolean isSplitable(FileSystem fs, Path filename) {
        return false;
    }

    class LSH_HashRecordReader implements RecordReader<LongWritable, LSH_Hash> {

    	private Path file;
        private FileSystem fs;
        private FSDataInputStream fileIn;
        private int part;

        public LSH_HashRecordReader(Path file, FileSystem fs, int part) {
            this.file = file;
            this.fs = fs;
            this.part = part;
            log.info("file=" + file + ",part=" + part);
            try {
				fileIn = fs.open(file);
            } catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
            //log.info("TimeSeriesRecordReader");
        }

        @Override
        public void close() throws IOException {
        	fileIn.close();
        }

        @Override
        public LongWritable createKey() {
            return new LongWritable();
        }

        @Override
        public LSH_Hash createValue() {
        	LSH_Hash lsh = new LSH_Hash();
	        return lsh;
        }

        @Override
        public long getPos() throws IOException {
            return fileIn.getPos();
        }

        @Override
        public boolean next(LongWritable key, LSH_Hash value) throws IOException
        {
        	try {
        		log.info("next=" + key);
        		if(fileIn.available() == 0)
        			return false;
        		LSH_Hash lsh = new LSH_Hash();
        		lsh.readFields(fileIn);
        		key.set(part);
        		value.set(lsh);
        		
        	} catch (Exception e) {
        		e.printStackTrace();
        		return false;
        	}
        	return true;
        }

		@Override
		public float getProgress() throws IOException {

			return 0;
		}
    }
}
