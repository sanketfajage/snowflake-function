package com.snowflake.functions;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

public class SnowflakeJDBCExample {

	static Connection connection = ConnectionFactory.getSnowflakeConnection();

	private SnowflakeJDBCExample() {
	}

	public static void executeSFCopyInto(String fileUrl) throws SQLException {
		try (Statement statement = connection.createStatement()) {
			fileUrl = fileUrl.replace("https", "azure");

			String stageName = "AZURE_BLOB_STORAGE_STAGE";
			createSFStage(stageName, fileUrl);
			copyFromSFStage(stageName);
			copyWithoutSFStage(fileUrl);
		}
	}

	private static void createSFStage(String stageName, String fileUrl) throws SQLException {
		// create an external stage
		try (Statement statement = connection.createStatement()) {
			String createStageStmt = "create or replace stage " + stageName + " url=\'" + fileUrl + "\'"
					+ " credentials=(azure_sas_token=" + AzureConstants.SAS_TOKEN + ")";
			statement.executeUpdate(createStageStmt);
		}
	}

	private static void copyFromSFStage(String stageName) throws SQLException {
		try (Statement statement = connection.createStatement()) {
			String copyIntoStmt = "COPY INTO table_name FROM @" + stageName;
			statement.executeUpdate(copyIntoStmt);
		}
	}

	private static void copyWithoutSFStage(String fileUrl) throws SQLException {
		try (Statement statement = connection.createStatement()) {
			String copyIntoStmt = "COPY INTO table_name FROM \'" + fileUrl + "\'" + " credentials=(azure_sas_token="
					+ AzureConstants.SAS_TOKEN + ")";
			statement.executeUpdate(copyIntoStmt);
		}
	}
}
