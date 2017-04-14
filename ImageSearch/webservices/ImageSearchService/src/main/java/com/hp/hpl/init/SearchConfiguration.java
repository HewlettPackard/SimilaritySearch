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
package com.hp.hpl.init;


import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

import javax.servlet.ServletContext;

import org.apache.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;


/**
 * This class sets configuration properties for the search service
 * @author Tere Gonzalez
 * 
 */
public class SearchConfiguration {
	
	/** The application context */
	@Autowired
    private ServletContext context;
	
	private static final Logger logger = Logger.getLogger(SearchConfiguration.class);
	
	/** The configuration file */
	private String CONFIGURATIONFILE = "searchConfiguration.properties";
	
	/** The coordinator IP Address for LSH */
	private static String coordinatorIPAddressLsh;
	
	/** The coordinator IP Address for Naive */
	private static String coordinatorIPAddressNaive;
	
	/** The path to extracted features files folder */
    private static String extractedFeaturesPath;
    
    /** The path to general search configuration file */
    private static String generalSearchConfigurationFileStr;
    
    /** The path to data library folder */
    private static String dataLibraryPath;
    
    /** The path to feature extraction program folder */
    private static String featureExtractionPath;
    
    /** The path to Hadoop script */
    private static String hadoopScript;
    
    /** The Hadoop IP Address */ 
    private static String hadoopIPAddress;
    
    /** The path to Hadoop cancel job script */
    private static String hadoopCancelJobScript;
    
    /** The path to Hadoop cancel job script */
    private static String hadoopJobResultsScript;
    
    
    /**
     * Reads configuration file
     * @see "searchConfiguration.properties"
     */
    public void readConfigurationFile(){    	
    	logger.info("Initializing Search Configuration properties");
		
    	String contextPath = context.getRealPath("/WEB-INF");		
		System.out.println(contextPath);
		Properties prop = new Properties();
		InputStream input = null;
	 
		try {
	 
			input = new FileInputStream(contextPath+"/"+CONFIGURATIONFILE);
	 
			// load a properties file
			prop.load(input);
	 
			// get the property value and print it out
			coordinatorIPAddressLsh=prop.getProperty("coordinatorIPLsh");
			coordinatorIPAddressNaive=prop.getProperty("coordinatorIPNaive");
			extractedFeaturesPath=prop.getProperty("extractedFeaturesPath");
		    generalSearchConfigurationFileStr =prop.getProperty("searchConfigurationFile");
		    dataLibraryPath=prop.getProperty("dataLibraryPath");	    
		    featureExtractionPath = prop.getProperty("featureExtractionPath");
		    hadoopScript = prop.getProperty("hadoopScript");
		    hadoopIPAddress = prop.getProperty("hadoopIP");
		    hadoopCancelJobScript = prop.getProperty("hadoopCancelJobScript");
		    hadoopJobResultsScript = prop.getProperty("hadoopJobResultsScript");
	 
		    //printSearchConfiguration();
		} catch (IOException ex) {
			logger.error("Error while initializing SearchConfiguration properties. " + ex.getMessage(), ex);
		} finally {
			if (input != null) {
				try {
					input.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}
		
    	
    }
    
    /**
     * Prints all configurations found in configuration file
     */
    public static void printSearchConfiguration(){    	
    	System.out.println("coordinatorIPAddressLsh="+ coordinatorIPAddressLsh);
    	System.out.println("coordinatorIPAddressNaive="+ coordinatorIPAddressNaive);
    	System.out.println("extractedFeaturesPath="+ extractedFeaturesPath); 
    	System.out.println("generalSearchConfigurationFileStr="+ generalSearchConfigurationFileStr); 
    	System.out.println("dataLibraryPath="+ dataLibraryPath); 
    	System.out.println("featureExtractionPath="+ featureExtractionPath);    
    	System.out.println("hadoopIP="+ hadoopIPAddress);    
    	System.out.println("hadoopScript="+ hadoopScript); 
    	System.out.println("hadoopCancelJobScript="+ hadoopCancelJobScript); 
    	System.out.println("hadoopJobResultsScript="+ hadoopJobResultsScript); 
    }
    
    /**
     * Returns the coordinator IP Address for LSH
     * @return	the IP Address
     */
    public static String getCoordinatorIPAddressLsh(){
    	return coordinatorIPAddressLsh;
    }
    
    /**
     * Returns the coordinator IP Address for Naive
     * @return	the IP Address
     */
    public static String getCoordinatorIPAddressNaive(){
    	return coordinatorIPAddressNaive;
    }
    
    /**
     * Returns the path to extracted features files folder
     * @return	the path
     */
    public static String getExtractedFeaturesPath(){
    	return extractedFeaturesPath;    	
    }
    
    /**
     * Returns the path to general search configuration file
     * @return	the path
     */
    public static String getGeneralSearchConfigurationFileStr(){
    	return generalSearchConfigurationFileStr;
    }
    
    /**
     * Returns the path to data library folder
     * @return	the path
     */
	public static String getDataLibraryPath() {
		return dataLibraryPath;
	}

	/**
     * Returns the path to feature extraction program folder
     * @return	the path
     */
	public static String getFeatureExtractionPath() {
		return featureExtractionPath;
	}
	
	/**
     * Returns the Hadoop IP Address
     * @return	the IP Address
     */
	public static String getHadoopIPAddress() {
		return hadoopIPAddress;
	}
	
	/**
     * Returns the path to Hadoop script
     * @return	the path
     */
	public static String getHadoopScript() {
		return hadoopScript;
	}
    
	/**
     * Returns the path to Hadoop cancel job script
     * @return	the path
     */
	public static String getHadoopCancelJobScript() {
		return hadoopCancelJobScript;
	}
	
	/**
     * Returns the path to Hadoop job results script
     * @return	the path
     */
	public static String getHadoopJobResultsScript() {
		return hadoopJobResultsScript;
	}

}

