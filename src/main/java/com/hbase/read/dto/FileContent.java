package com.hbase.read.dto;

public class FileContent {
	
	private String rowKey;
	
	private long projectId;
	
	private long customerId;

	/**
	 * @return the rowKey
	 */
	public String getRowKey() {
		return rowKey;
	}

	/**
	 * @param rowKey the rowKey to set
	 */
	public void setRowKey(String rowKey) {
		this.rowKey = rowKey;
	}

	/**
	 * @return the projectId
	 */
	public long getProjectId() {
		return projectId;
	}

	/**
	 * @param projectId the projectId to set
	 */
	public void setProjectId(long projectId) {
		this.projectId = projectId;
	}

	/**
	 * @return the customerId
	 */
	public long getCustomerId() {
		return customerId;
	}

	/**
	 * @param customerId the customerId to set
	 */
	public void setCustomerId(long customerId) {
		this.customerId = customerId;
	}
	
}
