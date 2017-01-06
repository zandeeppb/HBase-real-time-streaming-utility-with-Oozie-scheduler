package com.hbase.read.dto;

public class TrackingTable {
	
	private String rowKey;
	
	private String hbaseReadTblRowKey;
	
	private String status;
	
	private long projectId;
	
	private long customerId;
	
	private long succLastUpdatedTimeinMilli;
	
	private long errLastUdataedTimeinMilli;

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
	 * @return the hbaseReadTblRowKey
	 */
	public String getHbaseReadTblRowKey() {
		return hbaseReadTblRowKey;
	}

	/**
	 * @param hbaseReadTblRowKey the hbaseReadTblRowKey to set
	 */
	public void setHbaseReadTblRowKey(String hbaseReadTblRowKey) {
		this.hbaseReadTblRowKey = hbaseReadTblRowKey;
	}

	/**
	 * @return the status
	 */
	public String getStatus() {
		return status;
	}

	/**
	 * @param status the status to set
	 */
	public void setStatus(String status) {
		this.status = status;
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

	/**
	 * @return the succLastUpdatedTimeinMilli
	 */
	public long getSuccLastUpdatedTimeinMilli() {
		return succLastUpdatedTimeinMilli;
	}

	/**
	 * @param succLastUpdatedTimeinMilli the succLastUpdatedTimeinMilli to set
	 */
	public void setSuccLastUpdatedTimeinMilli(long succLastUpdatedTimeinMilli) {
		this.succLastUpdatedTimeinMilli = succLastUpdatedTimeinMilli;
	}

	/**
	 * @return the errLastUdataedTimeinMilli
	 */
	public long getErrLastUdataedTimeinMilli() {
		return errLastUdataedTimeinMilli;
	}

	/**
	 * @param errLastUdataedTimeinMilli the errLastUdataedTimeinMilli to set
	 */
	public void setErrLastUdataedTimeinMilli(long errLastUdataedTimeinMilli) {
		this.errLastUdataedTimeinMilli = errLastUdataedTimeinMilli;
	}
	
}
