package com.cdata.test;

import java.io.FileWriter;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

import com.opencsv.CSVWriter;

public class GBigQueryTesting {

	private static final String FILE_PATH = "./result.csv";

	public static Connection conn() throws SQLException {
		Properties prop = new Properties();
		prop.setProperty("InitiateOAuth", "REFRESH");
		prop.setProperty("OAuthClientID", "9198385943-lu19pjsr05tosjehamgjbektltu2k4km.apps.googleusercontent.com");
		prop.setProperty("OAuthClientSecret", "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");
		prop.setProperty("OAuthAccessToken",
				"ya29.a0AfB_byDW9FkBikLjbWZ-aaV9FvxxfI86dqafaba3VDDp3npMCziSV3tM9wEI2hR06O_wCxhSTN8J1b_e6gnQD-JZnN3TPAo8q7CMjXyfGdvlhYYPEmpGF0DVDlLoxy5u1uNB3Cn2PbgwfBJLbQbvCJd2jHGm2UTTXqZ9lgaCgYKAd4SARASFQHsvYls_AAUjkYdMuCslQo5EGC4RQ0173");
		prop.setProperty("OAuthRefreshToken",
				"1//04k1U9DTdyjAaCgYIARAAGAQSNwF-L9IrkEfwOwmg98k1Vzma2fSKqLZ0TlZLFLAprKEE-pEXwOlNAGBzT3BW5vsiq95b4b99DME");
		prop.setProperty("OAuthSettingsLocation", "%APPDATA%\\CData\\GoogleBigQuery Data Provider\\OAuthSettings.txt");
		prop.setProperty("RTK", "4442524A5641535552425641454E545033544D3234343532000000000000000000000000000000003131313131313131000052564E583030324336414A460000");

		Connection conn = DriverManager.getConnection("jdbc:googlebigquery:", prop);
		return conn;
	}

	public static void selectAllData() throws SQLException {
		Connection conn = conn();
		Statement stat = conn.createStatement();
		boolean ret = stat.execute("SELECT * FROM `cdata-for-bigquery-396907.bigquery.SalesRecord`");
		if (ret) {
			ResultSet rs = stat.getResultSet();
			while (rs.next()) {
				for (int i = 1; i <= rs.getMetaData().getColumnCount(); i++) {
					System.out.println(rs.getMetaData().getColumnLabel(i) + "=" + rs.getString(i));
				}
			}
		}
	}

	public static void insert() throws SQLException {
		Connection conn = conn();
		Statement stat = conn.createStatement();
		int count = stat.executeUpdate(
				"INSERT INTO [cdata-for-bigquery-396907].[bigquery].SalesRecord (Region, Country) VALUES('BoTaHtaung','Myanmar')",
				Statement.RETURN_GENERATED_KEYS);
		System.out.println(count);
		ResultSet rs = stat.getGeneratedKeys();
		while (rs.next()) {
			for (int i = 1; i <= rs.getMetaData().getColumnCount(); i++) {
				System.out.println(rs.getMetaData().getColumnLabel(i) + "=" + rs.getString(i));
			}
		}
	}

	public static void update() throws SQLException {
		Connection conn = conn();
		Statement stat = conn.createStatement();
		stat.execute(
				"UPDATE [cdata-for-bigquery-396907].[bigquery].SalesRecord SET region = 'XXX' , country = 'YYY' WHERE Row = '1'");
		int count = stat.getUpdateCount();
		System.out.println(count);
	}

	public static void delete() throws SQLException {
		Connection conn = conn();
		Statement stat = conn.createStatement();
		stat.execute("DELETE FROM [cdata-for-bigquery-396907].[bigquery].SalesRecord WHERE Row = '1'");
		int count = stat.getUpdateCount();
		System.out.println(count);
	}

	public static void batchInsert() throws SQLException {
		String query = "INSERT INTO [cdata-for-bigquery-396907].[bigquery].SalesRecord (country) VALUES (?)";
		Connection conn = conn();
		PreparedStatement pstmt = conn.prepareStatement(query, Statement.RETURN_GENERATED_KEYS);

		pstmt.setString(1, "EntityFramework");
		pstmt.addBatch();

		pstmt.setString(1, "CoreCLR");
		pstmt.addBatch();

		int[] r = pstmt.executeBatch();
		for (int i : r)
			System.out.println(i);

		ResultSet rs = pstmt.getGeneratedKeys();
		while (rs.next()) {
			System.out.println(rs.getString("Row"));
		}
	}

	public static void partitionSelectByHour() throws SQLException {
		Connection conn = conn();
		Statement stat = conn.createStatement();
		boolean ret = stat.execute(
				"SELECT Region, Country FROM `cdata-for-bigquery-396907.bigquery.partition` WHERE TIMESTAMP_TRUNC(_PARTITIONTIME, HOUR) = TIMESTAMP('2023-08-25T13:00:00') LIMIT 100");
		if (ret) {
			System.out.println("true");
			ResultSet rs = stat.getResultSet();
			while (rs.next()) {
				for (int i = 1; i <= rs.getMetaData().getColumnCount(); i++) {
					System.out.println(rs.getMetaData().getColumnLabel(i) + "=" + rs.getString(i));
				}
			}
		}
	}

	public static void partitionSelectByDay() throws SQLException {
		Connection conn = conn();
		Statement stat = conn.createStatement();
		boolean ret = stat.execute(
				"SELECT * FROM `cdata-for-bigquery-396907.bigquery.partitionByDay` WHERE TIMESTAMP_TRUNC(_PARTITIONTIME, DAY) = TIMESTAMP('2023-08-25') LIMIT 100");
		if (ret) {
			System.out.println("true");
			ResultSet rs = stat.getResultSet();
			while (rs.next()) {
				for (int i = 1; i <= rs.getMetaData().getColumnCount(); i++) {
					System.out.println(rs.getMetaData().getColumnLabel(i) + "=" + rs.getString(i));
				}
			}
		}
	}

	public static void partitionSelectByMonth() throws SQLException {
		Connection conn = conn();
		Statement stat = conn.createStatement();
		boolean ret = stat.execute(
				"SELECT Region, Country FROM `cdata-for-bigquery-396907.bigquery.PartitionByMonth` WHERE TIMESTAMP_TRUNC(_PARTITIONTIME, MONTH) = TIMESTAMP('2023-08-25') LIMIT 100");
		if (ret) {
			System.out.println("true");
			ResultSet rs = stat.getResultSet();
			while (rs.next()) {
				for (int i = 1; i <= rs.getMetaData().getColumnCount(); i++) {
					System.out.println(rs.getMetaData().getColumnLabel(i) + "=" + rs.getString(i));
				}
			}
		}
	}

	public static void partitionSelectByYear() throws SQLException {
		Connection conn = conn();
		Statement stat = conn.createStatement();
		boolean ret = stat.execute(
				"SELECT * FROM `cdata-for-bigquery-396907.bigquery.PartitionByYear` WHERE TIMESTAMP_TRUNC(_PARTITIONTIME, YEAR) = TIMESTAMP('2023-08-25') LIMIT 100");
		if (ret) {
			System.out.println("true");
			ResultSet rs = stat.getResultSet();
			while (rs.next()) {
				for (int i = 1; i <= rs.getMetaData().getColumnCount(); i++) {
					System.out.println(rs.getMetaData().getColumnLabel(i) + "=" + rs.getString(i));
				}
			}
		}
	}

	public static List<String> getAllTables() throws SQLException {
		List<String> defaultTableNames = Arrays.asList("Datasets", "PartitionsList", "PartitionsValues", "Projects");
		List<String> tableList = new ArrayList<String>();
		Connection conn = conn();
		DatabaseMetaData table_meta = conn.getMetaData();
		ResultSet rs = table_meta.getTables(null, null, "%", null);
		while (rs.next()) {
			String tableName = rs.getString("TABLE_NAME");
			if (!defaultTableNames.contains(tableName)) {
				tableList.add(tableName);
			}
		}
		return tableList;
	}

	public static List<String> getAllColumns() throws SQLException {
		List<String> columnList = new ArrayList<String>();
		Connection conn = conn();
		DatabaseMetaData table_meta = conn.getMetaData();
		ResultSet rs = table_meta.getColumns("cdata-for-bigquery-396907", "bigquery", "partition", null);
		while (rs.next()) {
			String columnName = rs.getString("COLUMN_NAME");
			if (!columnName.equals("_PARTITIONDATE"))
				columnList.add(columnName);
		}
		return columnList;
	}

	public static void csvWriter(String filePath) throws SQLException {
		List<String> tableList = getAllTables();
		List<String> columnList = getAllColumns();
		List<String> header = new ArrayList<>();
		header.add("No.");
		header.add("Report Name");
		header.add("Column Name");
		// create a List which contains String List
		List<List<String>> data = new ArrayList<List<String>>();
		data.add(header);

		int currentNo = 1;
		for (String reportName : tableList) {
			String previousReport = null;
			for (String columnName : columnList) {
				List<String> record = new ArrayList<>();
				if (previousReport == null) {
					record.add(String.valueOf(currentNo));
					record.add(reportName);
					currentNo++;
				} else {
					record.add("");
					record.add("");
				}
				record.add(columnName);
				data.add(record);
				previousReport = reportName;
			}
		}
		try (CSVWriter writer = new CSVWriter(new FileWriter(filePath))) {
			for (List<String> rowData : data) {
				String[] rowArray = rowData.toArray(new String[0]);
				writer.writeNext(rowArray);
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public static void main(String[] args) throws SQLException {
//		selectAllData();
//		insert();
//		update();
//		delete();
//		batchInsert();
//		partitionSelectByHour();
//		partitionSelectByMonth();
//		getAllTables();
//		getAllColumns();
		csvWriter(FILE_PATH);
	}
}
