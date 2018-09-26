package com.snowflake.functions;

import org.joda.time.DateTime;

import com.microsoft.azure.eventgrid.models.EventGridEvent;

public class Event extends EventGridEvent{

	public Event(String id, String subject, Object data, String eventType, DateTime eventTime, String dataVersion) {
		super(id, subject, data, eventType, eventTime, dataVersion);
	}
}
