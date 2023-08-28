package com.cdata.test;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Scanner;

public class GSheetTesting {
	public static final int LIMITED_COUNT = 30;
	
	public static Connection conn() throws SQLException {
		Properties prop = new Properties();
		
//		These properties are needed in "REFRESH"
		prop.setProperty("InitiateOAuth", "REFRESH");
		prop.setProperty("OAuthVerifier", retreiveVerifier("verifier_code"));
		prop.setProperty("OAuthClientID", "9198385943-lu19pjsr05tosjehamgjbektltu2k4km.apps.googleusercontent.com");
		prop.setProperty("OAuthClientSecret", "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");
		prop.setProperty("OAuthSettingsLocation", "%APPDATA%\\CData\\GoogleSheets Data Provider\\OAuthSettings.txt");
		
//		These properties are needed in "GETANDREFRESH"
//		prop.setProperty("InitiateOAuth","GETANDREFRESH");
//		prop.setProperty("OAuthClientID", "9198385943-lu19pjsr05tosjehamgjbektltu2k4km.apps.googleusercontent.com");
//		prop.setProperty("OAuthClientSecret", "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");
//		prop.setProperty("CallbackURL", "http://localhost:62360");
//		prop.setProperty("scope", "https://www.googleapis.com/auth/drive https://www.googleapis.com/auth/spreadsheets");
		
		Connection conn = DriverManager.getConnection("jdbc:googlesheets:",prop);		 
		return conn;
	}
	
	public static String getOAuthAccessToken() throws SQLException {
		Connection conn = conn();
		CallableStatement cstmt = conn.prepareCall("GetOAuthAccessToken");
		cstmt.setString("Verifier", getUrl());
		cstmt.setString("CallbackURL", "http://localhost:62360");
		boolean ret = cstmt.execute(); 
		String token = null;
		if (!ret) {
            int affectedRows = cstmt.getUpdateCount();
            System.out.println("Affected Rows: " + affectedRows);
        } else {
            ResultSet resultSet = cstmt.getResultSet();
            while (resultSet.next()) {
                String accessToken = resultSet.getString("OAuthAccessToken");
                String refreshToken = resultSet.getString("OAuthRefreshToken");
                String expiresIn = resultSet.getString("ExpiresIn");

                System.out.println("Access Token: " + accessToken);
                System.out.println("Refresh Token: " + refreshToken);
                System.out.println("Expires In: " + expiresIn);
            }
        }
		if(ret) {
			ResultSet rs = cstmt.getResultSet();
			token = rs.getString("OAuthAccessToken");
		}
//		cstmt.close();
		System.out.println("Access token " + token);
		return token;
	}
	
	public static List<String> getAllTables() throws SQLException {
		List<String> defaultTableNames = Arrays.asList(
				"Folders", "Sheets", "Spreadsheets");
		ArrayList<String> tableList = new ArrayList<String>();
		Connection conn = conn();		
		DatabaseMetaData table_meta = conn.getMetaData();
		//Getting all tables 
		ResultSet rs=table_meta.getTables(null, null, "%", null); 
		while(rs.next()) {
			String tableName = rs.getString("TABLE_NAME");
			if(!defaultTableNames.contains(tableName)) {
				tableList.add(tableName);
			}
		}
		return tableList;
	}
	
	public static List<String> getColumnsForEachTable(String tableName) throws SQLException {
		ArrayList<String> columnList = new ArrayList<String>();
		List<String> defaultColumnNames = Arrays.asList(
			    "Id", "Name", "DriveId", "Description", "CreatedTime",
			    "ModifiedTime", "Size", "OwnerName", "OwnerEmail",
			    "Starred", "Trashed", "Viewed", "ParentIds",
			    "ChildIds", "ChildLinks", "id"
			);			
			Connection conn = conn();		
			DatabaseMetaData table_meta = conn.getMetaData();
			ResultSet allColumns = table_meta.getColumns(null,null, tableName, null);
			while(allColumns.next()){
				String columnName = allColumns.getString("COLUMN_NAME");
				if(!defaultColumnNames.contains(columnName)) {
					columnList.add("`"+columnName+"`");
				}
			}
			return columnList;
	}
	
	public static void selectAllData(boolean fetchSample) throws SQLException {
		Connection conn = conn();
		Statement stat = conn.createStatement();
		List<String> tableList = getAllTables();
		for(String tableName : tableList) {
			System.out.println(tableName + ":");
			int tableRowCount = 0;
			List<String> columnList = getColumnsForEachTable(tableName);
			String selectedColumns = String.join(", ", columnList);
			StringBuilder query = new StringBuilder("SELECT "+ selectedColumns +" FROM `" + tableName+"`");
			if(fetchSample) {
				query.append(" LIMIT " + LIMITED_COUNT);
			}
			System.out.println(query);
			boolean result = stat.execute(query.toString());
			if (result) {
			  ResultSet dataRs=stat.getResultSet();
			  System.out.println(selectedColumns);
			  List<String> recordList = new ArrayList<>();
			  while(dataRs.next()) {
			    for(int i=1;i<=dataRs.getMetaData().getColumnCount();i++) {
			    	recordList.add(dataRs.getString(i));
			      //System.out.println(dataRs.getMetaData().getColumnLabel(i) +"="+dataRs.getString(i));
			    	System.out.print(dataRs.getString(i)+", ");
			    }
			    tableRowCount++;
			    System.out.println();
			  }
			}
			System.out.println("Actual resulted records: " + tableRowCount);
			System.out.println("Total Records: " + getRowCount(tableName));
		}
	}
	
	public static int getRowCount(String tableName) throws SQLException {
		int rowCount = 0;
		Connection conn = conn();
		Statement stat = conn.createStatement();
		String query = "SELECT COUNT(*) AS row_count FROM `" + tableName+"`";
		ResultSet resultSet = stat.executeQuery(query);
		if(resultSet.next()) {
			rowCount = resultSet.getInt("row_count");
		}
		return rowCount + 1;
	}
	
	public static void insert(String tableName) throws SQLException {
		Connection conn = conn();
		Statement stat = conn.createStatement();
		List<String> columnList = getColumnsForEachTable(tableName);
		String selectedColumns = String.join(", ", columnList);
		int count = stat.executeUpdate("INSERT INTO `"+tableName+"` ("+selectedColumns+") VALUES ('Kyi Sin','Fried Pork', '2F','Kyi Sin','Fried Pork', '2F','Kyi Sin','Fried Pork', '2F','Kyi Sin','Fried Pork', '2F')",Statement.RETURN_GENERATED_KEYS );
		System.out.println(count);
	}
	
	public static void batchInsert(String tableName) throws SQLException {
		Connection conn = conn();
		String query = "INSERT INTO " + tableName + " (B,C) VALUES (?,?)";
		PreparedStatement pstmt = conn.prepareStatement(query,Statement.RETURN_GENERATED_KEYS);
		 
		pstmt.setString(1, "Jon Doe");
		pstmt.setString(2, "korean food");
		pstmt.addBatch();
		 
		pstmt.setString(1, "John");
		pstmt.setString(2, "Chinese food");
		pstmt.addBatch();
		
		pstmt.setString(1, "Kaung");
		pstmt.setString(2, "Asian food");
		pstmt.addBatch();
		 
		int[] r = pstmt.executeBatch();
		for(int i: r)
		  System.out.println(i);
		 
		ResultSet rs = pstmt.getGeneratedKeys();
		while(rs.next()){   
		  System.out.println(rs.getString("Id"));
		}
	}
	
	public static void batchUpdate(String tableName) throws SQLException {
		Connection conn = conn();
		String query = "UPDATE " + tableName + " SET B = ? WHERE Id = ?";
		PreparedStatement pstmt = conn.prepareStatement(query);
		 
		pstmt.setString(1,"Kaung");
		pstmt.setString(2,"54");
		pstmt.addBatch();      
		 
		pstmt.setString(1,"Shoon");
		pstmt.setString(2,"55");
		pstmt.addBatch();      
		 
		int[] r = pstmt.executeBatch();
		for(int i: r)
		  System.out.println(i);
	}
	
	public static void batchDelete(String tableName) throws SQLException {
		Connection conn = conn();
		String query = "DELETE FROM " + tableName + " WHERE Id = ?";
		PreparedStatement pstmt = conn.prepareStatement(query);
		 
		pstmt.setString(1,"59");
		pstmt.addBatch();      
		 
		pstmt.setString(1,"58");
		pstmt.addBatch();      
		 
		int[] r = pstmt.executeBatch();
		for(int i: r)
		  System.out.println(i);
	}
	
	public static String getUrl() throws SQLException {
		Properties prop = new Properties();
		prop.setProperty("InitiateOAuth","OFF");
		prop.setProperty("OAuthClientID", "9198385943-lu19pjsr05tosjehamgjbektltu2k4km.apps.googleusercontent.com");
		prop.setProperty("OAuthClientSecret", "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");
		prop.setProperty("CallbackURL", "http://localhost:62360");
		Connection conn = DriverManager.getConnection("jdbc:googlesheets:",prop);
		CallableStatement cstmt = conn.prepareCall("GetOAuthAuthorizationURL");
		boolean ret = cstmt.execute();
		String url = null;
		if(ret) {
			ResultSet rs = cstmt.getResultSet();
			while (rs.next()) {
			System.out.println("rs" + rs);
			url = rs.getString("URL");
			}
		}		
		return url;
	}
	
	public static String getVerifier(String url) {
		String verifierCode = null;
		try {
            URI uri = new URI(url);
            String query = uri.getQuery();
            Map<String, String> params = new HashMap<>();
            String[] keyValuePairs = query.split("&");
            for (String pair : keyValuePairs) {
                String[] keyValue = pair.split("=");
                if (keyValue.length == 2) {
                    params.put(keyValue[0], keyValue[1]);
                }
            }
            verifierCode = params.get("code");
            System.out.println("Extracted code value: " + verifierCode);

        } catch (URISyntaxException e) {
            e.printStackTrace();
        }
		return verifierCode;
	}
	
	public static void storeVerifier(String key, String value) {
		try (OutputStream outputStream = new FileOutputStream("local_storage.properties")) {
            Properties properties = new Properties();
            properties.setProperty(key, value);
            properties.store(outputStream, null);
        } catch (IOException e) {
            e.printStackTrace();
        }
	}
	
	public static String retreiveVerifier(String key) {
		try (InputStream inputStream = new FileInputStream("local_storage.properties")) {
            Properties properties = new Properties();
            properties.load(inputStream);
            return properties.getProperty(key);
        } catch (IOException e) {
            e.printStackTrace();
            return null;
        }
	}
	
	public static void main(String[] args) throws SQLException {
		Boolean fetchSample = true;
		
//		We can test directly with "GETANDFRESH"
//		selectAllData(fetchSample);
//		insert("LunchTimeSheet_sheet1");
//		batchInsert("LunchTimeSheet_sheet1");
//		batchUpdate("LunchTimeSheet_sheet1");
//		batchDelete("LunchTimeSheet_sheet1");
		
//		We need this part when we use "REFRESH"
		String authorizationURL = getUrl(); // Get the authorization URL
        System.out.println("Authorization URL: " + authorizationURL);
        System.out.println("Please log in and grant permissions. After redirect, paste the callback URL here:");
        
        // Wait for the user to paste the callback URL
        @SuppressWarnings("resource")
		Scanner scanner = new Scanner(System.in);
        String callbackURL = scanner.nextLine();
        String verifier = getVerifier(callbackURL);
        storeVerifier("verifier_code", verifier);
		selectAllData(fetchSample);
	}
}
