package com.cdata.test;

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

import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryOptions;

public class BigqueryWithCusAccout {
	
	public static void main(String[] args) throws SQLException {
		getDataType("users1", "password");
//		compareTables("users", "users1");
//		getAllColumns("users1");
//		getAllTables();
//		addColumn("users", "password", "STRING");
//		addColumn("users1", "password", "INTEGER");
	}
	
	public static Connection conn() throws SQLException {
		Properties prop = new Properties();
		prop.setProperty("InitiateOAuth", "REFRESH");
		prop.setProperty("OAuthClientID", "435847296861-36copdi9mltguojlpotq118lpdfo2u2o.apps.googleusercontent.com");
		prop.setProperty("OAuthClientSecret", "KA9mIz2DXGbhzV3-psYYgsHE");
		prop.setProperty("OAuthAccessToken",
				"ya29.a0AfB_byAG_cWkhLuAltSToJgfQjX9-wUWpt4e07ClFUxdcgXebKLZr6VJcK-SararGsoyuva-Ek61uS__3AQfPOEAXQH8xf1-Fm8X3Ho93yds0p8IXDVrA5OZbioWpG0_HQXQmzMlxbOrG_7mFIz0VxSplJlW_8MRrHoemwaCgYKAWESARASFQHsvYlsP9XV6urmSNRO1QkFFaEaZg0173");
		prop.setProperty("OAuthRefreshToken",
				"1//04o1fOXlyrfQ6CgYIARAAGAQSNwF-L9IrZuhMjTHziNDixowxYI8uybTVfptuFBKWiBRk1W-Lzz0tWKyLUCt2Wff6t0TotknU4EQ");
		prop.setProperty("OAuthSettingsLocation", "%APPDATA%\\CData\\GoogleBigQuery Data Provider\\OAuthSettings.txt");
		prop.setProperty("ProjectId", "bdash2-bd73f");
		prop.setProperty("DatasetId", "java_local_testing");
		prop.setProperty("RTK", "4442524A5641535552425641454E545033544D3234343532000000000000000000000000000000003131313131313131000052564E583030324336414A460000");
		prop.setProperty("InsertMode", "Upload");
		Connection conn = DriverManager.getConnection("jdbc:googlebigquery:", prop);
		return conn;
	}
	
	public static void createTable(String tableName){
		try {
			Connection conn = conn();
			Statement stat = conn.createStatement();
			stat.executeUpdate("CREATE TABLE `"+tableName+"` (id INT PRIMARY KEY, name VARCHAR(50), email VARCHAR(50))");
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}
	
	public static List<String> getAllTables() throws SQLException {
		Connection conn = conn();
		DatabaseMetaData table_meta = conn.getMetaData();
		ResultSet rs = table_meta.getTables(null, null, "%", null);
		List<String> tableList = new ArrayList<>();
		while (rs.next()) {
			String tableName = rs.getString("TABLE_NAME");
			tableList.add(tableName);
			System.out.println(tableName);
		}
		return tableList;
	}	
	
	public static List<String> getAllColumns(String tableName) {
		try {
			List<String> columnList = new ArrayList<>();
			Connection conn = conn();
			DatabaseMetaData table_meta = conn.getMetaData();
			ResultSet rs = table_meta.getColumns("bdash2-bd73f", "java_local_testing", tableName, null);
			while(rs.next()) {
				System.out.println(rs.getString("COLUMN_NAME"));
				String column = rs.getString("COLUMN_NAME");
				columnList.add(column);
			}
			return columnList;
		} catch(SQLException e) {
			e.printStackTrace();
		}
		return null;
	}
	
	public static String getDataType(String tableName, String columnName) {
		String newColumnType = null;
		try {
			Connection conn = conn();
			DatabaseMetaData table_meta = conn.getMetaData();
			ResultSet rs = table_meta.getColumns("bdash2-bd73f", "java_local_testing", tableName, columnName);
			while(rs.next()) {
				String dataType = rs.getString("DATA_TYPE");
				System.out.print(columnName + dataType);
			    if (dataType.equals("-5")) {
			        newColumnType = "INTEGER";  
			    } else if (dataType.equals("12")) {
			        newColumnType = "STRING";  
			    }
				System.out.println("type" + newColumnType);
			}
			return newColumnType;
		} catch(SQLException e) {
			e.printStackTrace();
		}
		return newColumnType;
	}
	
	public static void insert() {
		try {
		Connection conn = conn();
		Statement stat = conn.createStatement();
		int count = stat.executeUpdate("INSERT INTO users VALUES (2, `test`, `test@gmail.com`)", Statement.RETURN_GENERATED_KEYS);
		System.out.println(count);
		} catch(SQLException e) {
			e.printStackTrace();
		}		
	}
	
	public static void update() {
		try {
			Connection conn = conn();
			String query = "UPDATE users SET name = ? WHERE id = ?";
			PreparedStatement pstmt = conn.prepareStatement(query);
			pstmt.setString(1, "Shoon Lae Linn");
			pstmt.setString(2, "2");
			int r = pstmt.executeUpdate();
			System.out.println(r);
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}
	
	public static void delete() {
		try {
			Connection conn = conn();
			String query = "DELETE FROM users WHERE id = ?";
			PreparedStatement pstmt = conn.prepareStatement(query);
			pstmt.setString(1, "4");
			int r = pstmt.executeUpdate();
			System.out.println(r);
		} catch(SQLException e) {
			e.printStackTrace();
		}
	}
	
	public static void addColumn(String tableName, String columnName, String dataType) {
		try {
			Connection conn = conn();
			Statement stat = conn.createStatement();
			System.out.print("tb"  + tableName + columnName + dataType);
			stat.executeUpdate("ALTER TABLE "+tableName+" ADD COLUMN "+columnName+" `"+dataType+"`");
		} catch(SQLException e) {
			e.printStackTrace();
		}
	}
	
	public static void batchInsert() {
		try {
		Connection conn = conn();
		String query = "INSERT INTO users (id, name, email) VALUES (?, ?, ?)";
		PreparedStatement pstmt = conn.prepareStatement(query,Statement.RETURN_GENERATED_KEYS);
		 
		pstmt.setString(1, "3");
		pstmt.setString(2, "Kyi");
		pstmt.setString(3, "k@gmail.com");
		pstmt.addBatch();
		 
		pstmt.setString(1, "4");
		pstmt.setString(2, "Sin");
		pstmt.setString(3, "s@gmail.com");
		pstmt.addBatch();
		
		pstmt.setString(1, "5");
		pstmt.setString(2, "Shoon");
		pstmt.setString(3, "shoon@gmail.com");
		pstmt.addBatch();
		 
		int[] r = pstmt.executeBatch();
		for(int i: r)
		  System.out.println(i);
		 
		ResultSet rs = pstmt.getGeneratedKeys();
		while(rs.next()){   
		  System.out.println(rs.getString("name"));
		}
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}
	
	public static void batchUpdate() {
		try {
			Connection conn = conn();
			String query = "UPDATE users SET email = ? WHERE id = ?";
			PreparedStatement pstmt = conn.prepareStatement(query);
			 
			pstmt.setString(1,"shoonlaelin@gmail.com");
			pstmt.setString(2,"2");
			pstmt.addBatch();      
			 
			pstmt.setString(1,"kyi@gmail.com");
			pstmt.setString(2,"3");
			pstmt.addBatch();      
			 
			int[] r = pstmt.executeBatch();
			for(int i: r)
			  System.out.println(i);
		} catch(SQLException e) {
			e.printStackTrace();
		}
	}
	
	public static void batchDelete() {
		try {
			Connection conn = conn();
			String query = "DELETE FROM users WHERE id = ?";
			PreparedStatement pstmt = conn.prepareStatement(query);
			 
			pstmt.setString(1,"3");
			pstmt.addBatch();      
			 
			pstmt.setString(1,"5");
			pstmt.addBatch();      
			 
			int[] r = pstmt.executeBatch();
			for(int i: r)
			  System.out.println(i);
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}
	
	public static void compareTables(String incomingTable, String existingTable) {
		try {
			List<String> tableList = getAllTables();
			Connection conn = conn();
			if(!tableList.contains(incomingTable)) {
				createTable(incomingTable);
			} else {
				List<String> newColumnList = getAllColumns(incomingTable);
				List<String> oldColumnList = getAllColumns(existingTable);
				for(String columnName : newColumnList) {
					if(!oldColumnList.contains(columnName)) {
						addColumn(existingTable, columnName, getDataType(incomingTable, columnName));
					}
				}
				for(String columnName: newColumnList) {
					for(String oldColumnName: oldColumnList) {
						if(columnName.equals(oldColumnName)) {
							if(!getDataType(incomingTable, columnName).equals(getDataType(existingTable, oldColumnName))) {
								Statement stat = conn.createStatement();
			                    stat.executeUpdate("ALTER TABLE `"+existingTable+"` DROP COLUMN `"+oldColumnName+"`");
			                    addColumn(existingTable, columnName, getDataType(incomingTable, columnName));
			                    break;
							}
						}
					}
				}							            
			}
		} catch(SQLException e) {
			e.printStackTrace();
		}
	}
	
	public static void selectALLData() {
		try {
			Connection conn = conn();
			Statement stat = conn.createStatement();
			boolean ret = stat.execute("SELECT * FROM users");
			if(ret) {
				ResultSet rs = stat.getResultSet();
				while(rs.next()) {
					for (int i = 1; i <= rs.getMetaData().getColumnCount(); i++) {
						System.out.print(rs.getString(i) + ",");
					}
					System.out.println();
				}
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}
}
