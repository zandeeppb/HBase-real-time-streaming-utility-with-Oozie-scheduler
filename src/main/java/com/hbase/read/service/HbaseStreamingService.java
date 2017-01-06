package com.hbase.read.service;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.StringWriter;
import java.net.URI;
import java.net.URISyntaxException;
import java.security.PrivilegedExceptionAction;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.ws.rs.core.MediaType;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.log4j.Logger;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;

import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.hbase.read.constant.Constants;
import com.hbase.read.dto.Input;
import com.hbase.read.dto.FileContent;
import com.hbase.read.dto.PlinkStatus;
import com.hbase.read.dto.SjsMonitor;
import com.hbase.read.dto.TrackingTable;
import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.ClientResponse;
import com.sun.jersey.api.client.WebResource;
import com.sun.jersey.core.util.MultivaluedMapImpl;

public class HbaseStreamingService {

	private static org.apache.hadoop.conf.Configuration conf = null;
	private static Logger log = null;
	private static Client client = null;
	private static String hdfsUrl;
	private static String hdfsUser;
	private static FileSystem hdfs = null;

	/**
	 * 
	 * @param hbaseQuorum
	 * @param nameNode
	 * @param user
	 * @param hbaseSite
	 * @param coreSite
	 * @throws IOException
	 */
	public static void setHbaseConfig(String hbaseQuorum, String nameNode,
			String user, String hbaseSite, String coreSite) throws IOException {
		client = new Client();
		conf = HBaseConfiguration.create();
		conf.set(Constants.HBAE_QUORUM, hbaseQuorum);
		conf.addResource(new Path(hbaseSite));
		conf.addResource(new Path(coreSite));
		hdfsUrl = nameNode;
		hdfsUser = user;
		log = Logger.getLogger(HbaseStreamingService.class);
	}

	public static void main(String[] args) throws IOException {

		try {

			String rowKeyFilePath = null;
			String hbaseQuorum = null;
			String nameNode = null;
			String user = null;
			int timeRangeInSecs = 0;
			int numOfRecords = 0;
			String hbaseSite = null;
			String coreSite = null;
			List<Input> inputs = null;

			if (null != args) {
				hbaseQuorum = assignValue(args, 0);
				hbaseSite = assignValue(args, 1);
				coreSite = assignValue(args, 2);
				nameNode = assignValue(args, 3);
				user = assignValue(args, 4);
				timeRangeInSecs = Integer.parseInt(assignValue(args, 5));
				numOfRecords = Integer.parseInt(assignValue(args, 6));
				rowKeyFilePath = assignValue(args, 7);// Sample given below

				/**
				 * File 1 ----------- rowkey1, project Id , customer Id , etc
				 * 
				 * File 2 ----------- rowkey2, project Id , customer Id , etc
				 * 
				 * File 3 ----------- rowkey3, project Id , customer Id , etc
				 * 
				 */

			}

			setHbaseConfig(hbaseQuorum, nameNode, user, hbaseSite, coreSite);

			// Create track table if not exists
			String[] familys = new String[] { Constants.ATTRIBUTES };
			creatTable(Constants.track_tablename, familys);

			// Read DMLE input from HDFS and update track table
			if (null != rowKeyFilePath) {
				try {
					inputs = updateTrackTable(rowKeyFilePath);
				} catch (Exception e) {
					log.error("HBASE Update track table error" + e.toString());
				}
			}
			// Once successfully updated HBase track table, delete files from
			// source HDFS path
			if (null != inputs && !inputs.isEmpty()) {
				for (Input input : inputs) {
					deleteFile(input.getFilePath());
				}
			}

			// Get all records with "in-progress" status from HBase track table
			List<TrackingTable> listOfTrackingTable = getAllInProgressRecords(Constants.track_tablename);

			// Processing request parallel
			List<Thread> threads = new ArrayList<Thread>();
			if (null != listOfTrackingTable)
				System.out.println("Number of threads "	+ listOfTrackingTable.size());
			for (TrackingTable table : listOfTrackingTable) {
				System.out.println("Processing rowkey "	+ table.getHbaseReadTblRowKey());
				PlinkDmleIncrementalProcessingThread thread = new PlinkDmleIncrementalProcessingThread(
						table, timeRangeInSecs, numOfRecords, conf, client);
				Thread threadInstance = new Thread(thread);
				threadInstance.start();
				threads.add(threadInstance);
			}
			for (Thread thread : threads) {
				thread.join();
			}
		} catch (Exception e) {
			e.printStackTrace();
			log.error(e.toString());
		}
	}

	/**
	 * 
	 * @param tableName
	 * @param familys
	 * @throws Exception
	 */
	public static void creatTable(String tableName, String[] familys)
			throws Exception {
		HBaseAdmin admin = new HBaseAdmin(conf);
		if (!admin.tableExists(tableName)) {
			HTableDescriptor tableDesc = new HTableDescriptor(tableName);
			for (int i = 0; i < familys.length; i++) {
				tableDesc.addFamily(new HColumnDescriptor(familys[i]));
			}
			admin.createTable(tableDesc);
		}
	}

	/**
	 * @param pathStr
	 * @return
	 */
	public static Configuration getConfig(String pathStr) {
		Configuration configuration = new Configuration();
		configuration.set("fs.defaultFS", hdfsUrl);
		configuration.set("hadoop.job.ugi", hdfsUser);
		configuration.addResource(new Path(pathStr));
		return configuration;
	}

	/**
	 * 
	 * @param rowKeyFilePath
	 * @return
	 * @throws IOException
	 * @throws URISyntaxException
	 */
	public static List<Input> updateTrackTable(String rowKeyFilePath)
			throws IOException, URISyntaxException {
		Gson gson = new Gson();
		List<Input> inputs = new ArrayList<Input>();
		Input input = null;
		List<FileContent> fileContents = new ArrayList<FileContent>();
		FileContent fileContent = null;
		List<String> files = new ArrayList<String>();
		BufferedReader br = null;
		FileSystem fs = null;
		String rowKey = Constants.EMPTY_STRING;
		StringBuilder stringUri = new StringBuilder(hdfsUrl)
				.append(rowKeyFilePath);
		try {

			URI uri = new URI(stringUri.toString());
			fs = FileSystem.get(uri, getConfig(rowKeyFilePath));
			FileStatus[] fileStatus = fs.listStatus(new Path(uri));
			for (FileStatus status : fileStatus) {

				br = new BufferedReader(new InputStreamReader(fs.open(status
						.getPath())));
				files.add(status.getPath().toString());
				input = new Input();
				input.setFilePath(status.getPath().toString());
				String line;
				line = br.readLine();
				while (line != null) {
					fileContent = gson.fromJson(line, FileContent.class);
					rowKey = new StringBuilder(fileContent.getRowKey())
							.append(Constants.UNDER_SCORE)
							.append(String.valueOf(fileContent.getProjectId()))
							.append(Constants.UNDER_SCORE)
							.append(String.valueOf(fileContent.getCustomerId()))
							.toString();

					fileContents.add(fileContent);

					addRecord(Constants.track_tablename, rowKey,
							Constants.ATTRIBUTES, Constants.ROW_KEY,
							fileContent.getRowKey());

					addRecord(Constants.track_tablename, rowKey,
							Constants.ATTRIBUTES, Constants.PROJECT_ID,
							String.valueOf(fileContent.getProjectId()));

					addRecord(Constants.track_tablename, rowKey,
							Constants.ATTRIBUTES, Constants.CUSTOMER_ID,
							String.valueOf(fileContent.getCustomerId()));

					addRecord(Constants.track_tablename, rowKey,
							Constants.ATTRIBUTES,
							Constants.SUCC_LAST_REC_TIMESTAMP, Constants.ZERO);

					addRecord(Constants.track_tablename, rowKey,
							Constants.ATTRIBUTES,
							Constants.ERR_LAST_REC_TIMESTAMP, Constants.ZERO);

					line = br.readLine();
				}
				input.setFileContents(fileContents);
				inputs.add(input);
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		return inputs;
	}

	/**
	 * 
	 * @param tableName
	 * @param rowKey
	 * @param family
	 * @param qualifier
	 * @param value
	 * @throws Exception
	 */
	public static void addRecord(String tableName, String rowKey,
			String family, String qualifier, String value) throws Exception {
		try {
			HTable table = new HTable(conf, tableName);
			Put put = new Put(Bytes.toBytes(rowKey));
			put.add(Bytes.toBytes(family), Bytes.toBytes(qualifier),
					Bytes.toBytes(value));
			table.put(put);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	/**
	 * 
	 * @param tableName
	 * @return
	 */
	public static List<TrackingTable> getAllInProgressRecords(String tableName) {

		Map<String, String> inProgressRowKeyMap = new HashMap<String, String>();
		TrackingTable trackingTable = new TrackingTable();
		List<TrackingTable> listOfTrackingTable = new ArrayList<TrackingTable>();
		try {
			HTable table = new HTable(conf, tableName);
			SingleColumnValueFilter filter = new SingleColumnValueFilter(
					Bytes.toBytes(Constants.ATTRIBUTES),
					Bytes.toBytes(Constants.STATUS), CompareOp.EQUAL,
					Bytes.toBytes(Constants.IN_PROGRESS));
			Scan s = new Scan();
			s.setFilter(filter);
			ResultScanner ss = table.getScanner(s);
			for (Result r : ss) {
				for (KeyValue kv : r.raw()) {
					inProgressRowKeyMap.put(Constants.TRACK_TABLE_ROWKEY,
							new String(kv.getRow()));

					if (Constants.ROW_KEY.equals(new String(kv.getQualifier())))
						inProgressRowKeyMap.put(Constants.ROW_KEY, new String(
								kv.getValue()));

					if (Constants.STATUS.equals(new String(kv.getQualifier())))
						inProgressRowKeyMap.put(Constants.STATUS,
								new String(kv.getValue()));

					if (Constants.CUSTOMER_ID.equals(new String(kv
							.getQualifier())))
						inProgressRowKeyMap.put(Constants.CUSTOMER_ID,
								new String(kv.getValue()));

					if (Constants.PROJECT_ID.equals(new String(kv
							.getQualifier())))
						inProgressRowKeyMap.put(Constants.PROJECT_ID,
								new String(kv.getValue()));

					if (Constants.SUCC_LAST_REC_TIMESTAMP.equals(new String(kv
							.getQualifier())))
						inProgressRowKeyMap.put(
								Constants.SUCC_LAST_REC_TIMESTAMP, new String(
										kv.getValue()));
					if (Constants.ERR_LAST_REC_TIMESTAMP.equals(new String(kv
							.getQualifier())))
						inProgressRowKeyMap.put(
								Constants.ERR_LAST_REC_TIMESTAMP,
								new String(kv.getValue()));

				}
				trackingTable = new TrackingTable();
				trackingTable.setRowKey(inProgressRowKeyMap
						.get(Constants.TRACK_TABLE_ROWKEY));
				trackingTable.setHbaseReadTblRowKey(inProgressRowKeyMap
						.get(Constants.ROW_KEY));
				trackingTable.setStatus(inProgressRowKeyMap
						.get(Constants.STATUS));
				trackingTable.setProjectId(Long.valueOf(inProgressRowKeyMap
						.get(Constants.PROJECT_ID)));
				trackingTable.setCustomerId(Long.valueOf(inProgressRowKeyMap
						.get(Constants.CUSTOMER_ID)));
				trackingTable.setErrLastUdataedTimeinMilli(Long
						.valueOf(inProgressRowKeyMap
								.get(Constants.ERR_LAST_REC_TIMESTAMP)));
				trackingTable.setSuccLastUpdatedTimeinMilli(Long
						.valueOf(inProgressRowKeyMap
								.get(Constants.SUCC_LAST_REC_TIMESTAMP)));
				listOfTrackingTable.add(trackingTable);
			}

		} catch (IOException e) {
			e.printStackTrace();
		}

		return listOfTrackingTable;
	}

	/**
	 * 
	 * @param hdfsPathStr
	 * @return
	 * @throws IOException
	 * @throws InterruptedException
	 */
	public static Boolean deleteFile(final String path) throws IOException,
			InterruptedException {
		Boolean hdfsDeleteStatus = false;
		try {
			UserGroupInformation ugi = UserGroupInformation
					.createRemoteUser(hdfsUser);
			hdfsDeleteStatus = ugi
					.doAs(new PrivilegedExceptionAction<Boolean>() {
						public Boolean run() throws Exception {
							try {
								URI uri = new URI(hdfsUrl);
								hdfs = FileSystem.get(uri, getConfig(path));
								hdfs.delete(new Path(path), true);
							} catch (Exception e) {

							} finally {
								closeHDFS();
							}
							return true;
						}
					});
		} catch (Exception e) {
			log.error("deleteFile error" + e.toString());
		} finally {
			closeHDFS();
		}
		return hdfsDeleteStatus;
	}

	/**
	 * Responsible for closing resources within HDFS after utilization
	 */
	public static void closeHDFS() {
		try {
			if (null != hdfs)
				hdfs.close();
		} catch (IOException e) {

		}
	}

	public static void createOrOverwrite(Admin admin, HTableDescriptor table)
			throws IOException {
		if (admin.tableExists(table.getTableName())) {
			admin.disableTable(table.getTableName());
			admin.deleteTable(table.getTableName());
		}
		admin.createTable(table);
	}

	/**
	 * 
	 * @param args
	 * @param index
	 * @return
	 */
	private static String assignValue(String[] args, int index) {
		if (args.length > index)
			return args[index];
		return null;
	}

}

class PlinkDmleIncrementalProcessingThread implements Runnable {

	private long startTimeInMilliSec = 0;
	private long endTimeInMilliSec = 0;
	private List<String> records = null;
	private TrackingTable trackingTbl = new TrackingTable();	
	private int timeRangeInSecs = 0;
	private Configuration conf = null;
	private Client client = null;
	private int numOfRecords = 0;
	private String callBackServiceUrl = null;

	/**
	 * 
	 * @param trackingTbl
	 * @param timeRangeInSecs
	 * @param callBackServiceUrl
	 * @param dmlsJobStatusUrl
	 * @param conf
	 * @param client
	 */
	PlinkDmleIncrementalProcessingThread(TrackingTable trackingTbl,
			int timeRangeInSecs, int numOfRecords, Configuration conf,
			Client client) {
		this.trackingTbl = trackingTbl;
		this.timeRangeInSecs = timeRangeInSecs;
		this.conf = conf;
		this.client = client;
		this.numOfRecords = numOfRecords;
	}

	@Override
	public void run() {
		callBackServiceUrl = "http://hostaddress:port/path";
		long currentTimeinMilliSec = Calendar.getInstance().getTimeInMillis();
		// DMLE output HBase call
		if (trackingTbl.getSuccLastUpdatedTimeinMilli() == 0) {

			startTimeInMilliSec = 0;
			endTimeInMilliSec = currentTimeinMilliSec;
			try {
				records = getRangeOfRecordsFromHbaseTbl(Constants.tablename,
						trackingTbl.getHbaseReadTblRowKey(),
						trackingTbl.getRowKey(), startTimeInMilliSec,
						endTimeInMilliSec, numOfRecords);
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		} else {
			startTimeInMilliSec = trackingTbl.getSuccLastUpdatedTimeinMilli() + 1;
			endTimeInMilliSec = startTimeInMilliSec + (timeRangeInSecs * 1000);
			try {
				records = getRangeOfRecordsFromHbaseTbl(Constants.tablename,
						trackingTbl.getHbaseReadTblRowKey(),
						trackingTbl.getRowKey(), startTimeInMilliSec,
						endTimeInMilliSec, numOfRecords);
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}

		if (null != records && !records.isEmpty()) {
			// Call back service
			String resposne;
			try {
				resposne = callBackService(callBackServiceUrl, records,
						trackingTbl.getProjectId(), trackingTbl.getCustomerId());
				if (Constants.FAILURE.equals(resposne)) {
					addRecord(Constants.track_tablename,
							trackingTbl.getRowKey(), Constants.ATTRIBUTES,
							Constants.SUCC_LAST_REC_TIMESTAMP,
							String.valueOf(startTimeInMilliSec));
				}
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}

		} else {

			try {
				addRecord(Constants.track_tablename, trackingTbl.getRowKey(),
						Constants.ATTRIBUTES,
						Constants.SUCC_LAST_REC_TIMESTAMP,
						String.valueOf(startTimeInMilliSec));
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		records = null;

	}

	/**
	 * 
	 * @param tableName
	 * @param dmleRowKey
	 * @param trackTableRowKey
	 * @param minTime
	 * @param maxTime
	 * @param numOfRecords
	 * @return
	 * @throws Exception
	 */
	public List<String> getRangeOfRecordsFromHbaseTbl(String tableName,
			String dmleRowKey, String trackTableRowKey, long minTime,
			long maxTime, int numOfRecords) throws Exception {
		long lastReadRecordTimeinMillSec = 0;
		List<String> list = null;
		HTable table = new HTable(conf, tableName);
		Get get = new Get(dmleRowKey.getBytes());
		get.setTimeRange(minTime, maxTime);
		get.setMaxResultsPerColumnFamily(numOfRecords);
		Result rs = table.get(get);
		for (KeyValue kv : rs.raw()) {
			if (null == list)
				list = new ArrayList<String>();
			if (kv.getTimestamp() > lastReadRecordTimeinMillSec) {
				lastReadRecordTimeinMillSec = kv.getTimestamp();
				addRecord(Constants.track_tablename, trackTableRowKey,
						Constants.ATTRIBUTES,
						Constants.SUCC_LAST_REC_TIMESTAMP,
						String.valueOf(lastReadRecordTimeinMillSec));
			}
			list.add(new String(kv.getValue()));
		}
		return list;
	}

	/**
	 * Sample Web service call to real world application
	 * 
	 * @param url
	 * @param linkId
	 * @param currentUserId
	 * @param currentUserOrgId
	 * @param recommendations
	 * @return String
	 * @throws ClientProtocolException
	 * @throws IOException
	 * @throws JSONException
	 */
	private String callBackService(String url, List<String> datas,
			long projectId, long customerId) throws ClientProtocolException,
			IOException {
		String callbackServiceResponse = null;
		Gson json = new Gson();
		StringBuilder data = new StringBuilder();
		for (String value : datas) {
			data.append(value);
		}
		try {
			WebResource contentResource = client.resource(url);
			MultivaluedMapImpl params = new MultivaluedMapImpl();
			params.add(Constants.PROJECT_ID, String.valueOf(projectId));
			params.add(Constants.CUSTOMER_ID, String.valueOf(customerId));
			ClientResponse response = contentResource
					.queryParams(params)
					.header(Constants.CONTENT_TYPE,
							Constants.CONTENT_TYPE_VALUE_JSON)
					.type(MediaType.APPLICATION_FORM_URLENCODED_TYPE)
					.type(MediaType.APPLICATION_OCTET_STREAM)
					.type(MediaType.APPLICATION_JSON)
					.post(ClientResponse.class, data.toString()); // data could be in any format like csv , json etc

			InputStream ioStream = response.getEntityInputStream();
			StringWriter writer = new StringWriter();
			IOUtils.copy(ioStream, writer, Constants.UTF_8);
			if (null != writer) {
				callbackServiceResponse = new JSONObject(writer.toString())
						.get(Constants.STATUS).toString();
			}
		} catch (Exception e) {
			e.printStackTrace();
			callbackServiceResponse = Constants.FAILURE;
		}
		return callbackServiceResponse;
	}

	/**
	 * 
	 * @param tableName
	 * @param rowKey
	 * @param family
	 * @param qualifier
	 * @param value
	 * @throws Exception
	 */
	public void addRecord(String tableName, String rowKey, String family,
			String qualifier, String value) throws Exception {
		try {
			HTable table = new HTable(conf, tableName);
			Put put = new Put(Bytes.toBytes(rowKey));
			put.add(Bytes.toBytes(family), Bytes.toBytes(qualifier),
					Bytes.toBytes(value));
			table.put(put);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

}
