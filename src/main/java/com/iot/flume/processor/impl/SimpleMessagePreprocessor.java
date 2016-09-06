package com.iot.flume.processor.impl;

import com.iot.flume.processor.MessagePreprocessor;
import com.iot.flume.sink.KafkaSinkConstants;
import org.apache.flume.Context;
import org.apache.flume.Event;

import java.io.UnsupportedEncodingException;
import java.util.Map;

/**
 * Created by hjl on 2016/8/26.
 */
public class SimpleMessagePreprocessor implements MessagePreprocessor {


    public String extractKey(Event event, Context context) throws UnsupportedEncodingException {
        // get timestamp header if it's present.
//        String timestampStr = event.getHeaders().get("timestamp");
//        if(timestampStr != null){
//            // parse it and get the hour
//            Long timestamp = Long.parseLong(timestampStr);
//            Calendar cal = Calendar.getInstance();
//            cal.setTimeZone(TimeZone.getTimeZone("UTC"));
//            cal.setTimeInMillis(timestamp);
//            return Integer.toString(cal.get(Calendar.HOUR_OF_DAY));
//        }
//        return null;    // return null otherwise

        return new String(event.getBody(),"UTF-8");

    }


    public String extractTopic(Event event, Context context) throws UnsupportedEncodingException {
        String eventTopic;
        String default_type = context.getString(KafkaSinkConstants.TYPE_KEY, KafkaSinkConstants.DEFAULT_TYPE);
        String default_topic = context.getString(KafkaSinkConstants.TOPIC, KafkaSinkConstants.DEFAULT_TOPIC);
        Map<String,String> headers = event.getHeaders();
        String type = headers.get("type");
        String app = headers.get("app");
        if (app == null && type == null) {
            eventTopic = default_topic;
        } else {
            if (null == type) {
                eventTopic = app + "." + default_type;
            } else {
                eventTopic = app + "." + type;
            }
        }
        return eventTopic;
    }

    public String transformMessage(Event event, Context context) throws UnsupportedEncodingException {
//        String messageBody = new String(event.getBody());
//        String timestampStr = event.getHeaders().get("timestamp");
//        if(timestampStr != null){
//            messageBody = timestampStr.concat(": " + messageBody);
//        }
//        return messageBody;

        return new String(event.getBody(),"UTF-8");

    }
}
