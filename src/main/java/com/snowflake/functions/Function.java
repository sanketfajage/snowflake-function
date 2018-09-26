package com.snowflake.functions;

import java.sql.SQLException;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import com.microsoft.azure.eventgrid.models.EventGridEvent;
import com.microsoft.azure.functions.ExecutionContext;
import com.microsoft.azure.functions.annotation.EventGridTrigger;
import com.microsoft.azure.functions.annotation.FunctionName;

/**
 * Azure Functions with HTTP Trigger.
 */
public class Function {
	/**
	 * This function listens at endpoint "/api/HttpTrigger-Java". Two ways to
	 * invoke it using "curl" command in bash: 1. curl -d "HTTP Body" {your
	 * host}/api/HttpTrigger-Java 2. curl {your
	 * host}/api/HttpTrigger-Java?name=HTTP%20Query
	 */
	@FunctionName("eventGridTrigger")
	public void httpTriggerJava(@EventGridTrigger(name = "eventGridEvent") EventGridEvent eventGridEvent,
			final ExecutionContext context) {
		context.getLogger().info("Azure Event Grid trigger started...");

		JSONArray jsonArray = new JSONArray(eventGridEvent);

		for (int i = 0; i < jsonArray.length(); i++) {
			JSONObject jsonObject = jsonArray.getJSONObject(i);
			String eventType = jsonObject.getString("eventType");

			if (eventType.equals("Microsoft.Storage.BlobCreated")) {
				JSONObject eventDataJson = jsonObject.getJSONObject("data");

				if (!eventDataJson.getString("contentType").equals("application/octet-stream")) {
					try {
						SnowflakeJDBCExample.executeSFCopyInto(eventDataJson.getString("url"));
					} catch (JSONException | SQLException e) {
						e.printStackTrace();
					}
				}
			}
		}
	}
}
