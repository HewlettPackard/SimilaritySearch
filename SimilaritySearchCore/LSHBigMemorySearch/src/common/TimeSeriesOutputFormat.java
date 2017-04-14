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
package common;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordWriter;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.util.Progressable;


public class TimeSeriesOutputFormat extends FileOutputFormat<LongWritable, TimeSeries> {

	Log log = LogFactory.getLog(TimeSeriesInputFormat.class);
	
	@Override
	public RecordWriter<LongWritable, TimeSeries> getRecordWriter(FileSystem fs, JobConf conf,
			String arg2, Progressable arg3) throws IOException {
    	Path path = FileOutputFormat.getOutputPath(conf);
    	Path subpath = new Path(path, "ts");
    	Path subpath1 = new Path(subpath, FileOutputFormat.getUniqueName(conf, "ts"));
    	//Path subpath1 = FileOutputFormat.getTaskOutputPath(conf, FileOutputFormat.getUniqueName(conf, "ts"));

    	//Path path = FileOutputFormat.getPathForCustomFile(conf, name)getUniqueName(conf, name)getWorkOutputPath(conf);

    	return new TimeSeriesRecordWriter(fs, subpath1);
	}
	
	class TimeSeriesRecordWriter implements RecordWriter<LongWritable, TimeSeries> {
        
        private FSDataOutputStream fileOut;
        FileSystem fs;
        Path path;
       	Log log = LogFactory.getLog(TimeSeriesRecordWriter.class);        
        
       	public TimeSeriesRecordWriter(FileSystem fs, Path path) {
            this.fs = fs;
            this.path = path;
       		try {
            	fileOut = fs.create(path);
            		
            	
            } catch (IOException e) {
				e.printStackTrace();
			}
            log.info("TimeSeriesRecordWriter");
        }
        
		@Override
		public void close(Reporter arg0) throws IOException {
			fileOut.close();
			
		}

		@Override
		public void write(LongWritable key, TimeSeries data)
				throws IOException {

			data.write(fileOut);
		}
	
	}

}
