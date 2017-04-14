/*
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
package com.hp.hpl.ssh;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;


import com.jcraft.jsch.Channel;
import com.jcraft.jsch.ChannelExec;
import com.jcraft.jsch.JSch;
import com.jcraft.jsch.Session;

/**
 * This class contains methods to connect to remote machines via SSH and execute commands, among others.
 * @author Janneth Rivera
 *
 */
public class SSHConnector {
	
	/** The user name */
	private String user;
	
	/** The user's private key */
	public String privateKey;
	
	/** The ssh properties file */
	private String SSHPROPERTIES_FILE = "ssh.properties";
	
	/**
	 * Initializes variables from configuration file
	 * @param path Represents path for configuration file
	 */
	public void init(String path){
		String filePath = path + "/" + SSHPROPERTIES_FILE;
		
		Properties prop = new Properties();
		try {
			prop.load(new FileInputStream(filePath));
    		user = prop.getProperty("user"); 
    		privateKey = path + "/" + prop.getProperty("privateKey");
    	} catch (IOException ex) {
    		System.out.println(ex);
        }			
	}
	
	
	/**
	 * SSH Connects password-less
	 * @param	host	the IP Address to connect
	 * @return	the session from connection
	 */
	public Session connectPwdless(String host){
		Session session = null;
		try{            
            java.util.Properties config = new java.util.Properties(); 
            config.put("StrictHostKeyChecking", "no");
            JSch jsch = new JSch();
            
            
            jsch.addIdentity(privateKey);
            System.out.println("identity added for user: " + user);
            
            session = jsch.getSession(user, host, 22);            
            session.setConfig(config);
            session.connect();
            System.out.println("Connected");            
            
		}catch(Exception e){
            e.printStackTrace();
        }
		
		return session;
	}
	
	/**
	 * Closes SSH connection
	 * @param session
	 */
	public void disconnect(Session session){
		session.disconnect();
		System.out.println("Disconnected");
	}
	
	/**
	 * Executes a command in SSH shell
	 * @param	session	the session from connection
	 * @param	cmd	the command to execute
	 * @return	the results from executed command
	 */
	public List<String> executeCommand(Session session, String cmd){
		List<String> results = new ArrayList<String>();
		
		try{
			Channel channel=session.openChannel("exec");
            ((ChannelExec)channel).setCommand(cmd);
            
            BufferedReader in=new BufferedReader(new InputStreamReader(channel.getInputStream()));
            channel.connect();
           
            while(true){
            	String res = null;
            	while((res = in.readLine())!=null)  results.add(res);
            		   
            	if(channel.isClosed()){
            		//System.out.println("exit-status: "+channel.getExitStatus());
                break;
              }
              try{Thread.sleep(1000);}catch(Exception ee){}
            }
            channel.disconnect();
			
		}catch(Exception e){
			e.printStackTrace();
		}
		
		System.out.println(results);
		return results;
	}
	
	
	/**
	 * Transfers file from local to remote machine via SSH shell connection
	 * @param	session	the session from connection
	 * @param	filename	the	file name to transfer
	 * @param	fromPath	the path to file in local machine
	 * @param	toPath	the path to transfer file in remote machine
	 */
	public void fileTransfer(Session session, String filename, String fromPath, String toPath){
		FileInputStream fis=null;
		try{ 
			
			boolean ptimestamp = true;
		 
			// exec 'scp -t rfile' remotely
			String command = "sudo scp " + (ptimestamp ? "-p" :"") +" -t "+ toPath+filename;
			Channel channel = session.openChannel("exec");
			((ChannelExec)channel).setCommand(command);
			
			 
			// get I/O streams for remote scp
			OutputStream out = channel.getOutputStream();
			InputStream in = channel.getInputStream();
			
			channel.connect();
			
			if(checkAck(in)!=0){
				System.exit(0);
			}
	
			File _lfile = new File(fromPath + "/" + filename);
			 
			if(ptimestamp){
				command="T "+(_lfile.lastModified()/1000)+" 0";
				// The access time should be sent here,
				// but it is not accessible with JavaAPI ;-<
				command+=(" "+(_lfile.lastModified()/1000)+" 0\n"); 
				out.write(command.getBytes()); out.flush();
				if(checkAck(in)!=0){
					System.exit(0);
				}
			}
			 
			// send "C0644 filesize filename", where filename should not include '/'
			long filesize=_lfile.length();
			command="C0644 "+filesize+" ";
			if(filename.lastIndexOf('/')>0){
				command+=filename.substring(filename.lastIndexOf('/')+1);
			}
			else{
				command+=filename;
			}
			command+="\n";
			out.write(command.getBytes()); out.flush();
			
			if(checkAck(in)!=0){
				System.exit(0);
			}
			 
			// send a content of filename
			fis=new FileInputStream(fromPath + "/" + filename);
			byte[] buf=new byte[1024];
			while(true){
				int len=fis.read(buf, 0, buf.length);
				if(len<=0) break;
					out.write(buf, 0, len); //out.flush();
			}
			fis.close();
			fis=null;
			// send '\0'
			buf[0]=0; out.write(buf, 0, 1); out.flush();
			if(checkAck(in)!=0){
				System.exit(0);
			}
			out.close();
			 
			channel.disconnect();
			System.out.println("File: " + filename + " was succesfully transfered.");
			
		}
	    catch(Exception e){
	      System.out.println(e);
	      try{if(fis!=null)fis.close();}catch(Exception ee){}
	    }		
	}
	
	
	public int checkAck(InputStream in) throws IOException{
	    int b=in.read();
	    // b may be 0 for success,
	    //          1 for error,
	    //          2 for fatal error,
	    //          -1
	    if(b==0) return b;
	    if(b==-1) return b;
	 
	    if(b==1 || b==2){
	    	StringBuffer sb=new StringBuffer();
	    	int c;
	    	do {
	    		c=in.read();
	    		sb.append((char)c);
	    	}
	    	while(c!='\n');
    		if(b==1){ // error
    			System.out.print(sb.toString());
    		}
    		if(b==2){ // fatal error
    			System.out.print(sb.toString());
    		}
	    }
	    return b;
	  }
	

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		SSHConnector ssh = new SSHConnector();
		
		String path = "src/main/webapp/WEB-INF";
			
			ssh.init(path);
			Session session = ssh.connectPwdless("mercado-1.hpl.hp.com");
			//ssh.fileTransfer(session,"servlet-context.xml", path, "/usr/local/hadoop-1.1.2/");
			
			//ssh.executeCommand(session, "cd MemoryCollectApp2; ./start.sh;");
			ssh.executeCommand(session, "cd CPUCollectApp2; ./start.sh;");
			ssh.disconnect(session);
			
		
	}

}

