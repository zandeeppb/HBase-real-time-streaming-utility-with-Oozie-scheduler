package com.hbase.read.dto;

import java.util.List;

public class Input {
	
	private String filePath;
	
	private List<FileContent> fileContents;

	/**
	 * @return the filePath
	 */
	public String getFilePath() {
		return filePath;
	}

	/**
	 * @param filePath the filePath to set
	 */
	public void setFilePath(String filePath) {
		this.filePath = filePath;
	}

	/**
	 * @return the fileContents
	 */
	public List<FileContent> getFileContents() {
		return fileContents;
	}

	/**
	 * @param fileContents the fileContents to set
	 */
	public void setFileContents(List<FileContent> fileContents) {
		this.fileContents = fileContents;
	}
		
}
