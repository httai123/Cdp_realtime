package com.viettel.cdp.utils;

import org.apache.log4j.Logger;

import java.util.ResourceBundle;

public class Resource {

    private static final String CONFIG_FILE = "config";
    private static final Logger logger = Logger.getLogger(Resource.class);
    private static ResourceBundle SCORE_RESOURCE = getCdpResource(CONFIG_FILE);

    private static ResourceBundle getCdpResource(String resourceFile) {
        ResourceBundle resourceBundle = null;

        try {
            resourceBundle = ResourceBundle.getBundle(resourceFile);
        } catch (Exception e) {
            logger.error("Error loading resource bundle", e);
        }
        return resourceBundle;
    }

    public static String getParamValue(String key) {
        try  {
            SCORE_RESOURCE.getString(key);
            return SCORE_RESOURCE.getString(key);
        }
        catch (Exception e){
            logger.warn("MISSING CONFIG FOR KEY: " + key + ", default null ");
            return null;
        }
    }

    public static class KAFKA_SOURCE {
        public static final String BOOTSTRAP_SERVERS = getParamValue("kafka_source.bootstrap.servers");
        public static final String TOPIC_EVENTS = getParamValue("kafka_source.topic.events");
        public static final String GROUP_ID = getParamValue("kafka_source.group.id");
        public static final String AUTO_OFFSET_RESET = getParamValue("kafka_source.auto.offset.reset");

        public static final boolean KERBEROS_ENABLED = Boolean.parseBoolean(getParamValue("kafka_source.kerberos.enabled"));
        public static final String SECURITY_PROTOCOL = getParamValue("kafka_source.security.protocol");
        public static final String SASL_MECHANISM = getParamValue("kafka_source.sasl.mechanism");

        public static final String SASL_JAAS_CONFIG = getParamValue("kafka_source.sasl.jaas.config");
        public static final String JAAS_CONF_PATH = getParamValue("kafka_source.jaas.conf.path");
        public static final String KRB5_CONF_PATH = getParamValue("kafka_source.krb5.conf.path");
    }

    public static class KAFKA_UPDATE_CONTROL {
        public static final String BOOTSTRAP_SERVERS = getParamValue("kafka_update_control.bootstrap.servers");
        public static final String TOPIC_EVENTS = getParamValue("kafka_update_control.topic.events");
        public static final String GROUP_ID = getParamValue("kafka_update_control.group.id");
        public static final String AUTO_OFFSET_RESET = getParamValue("kafka_update_control.auto.offset.reset");

        public static final boolean KERBEROS_ENABLED = Boolean.parseBoolean(getParamValue("kafka_update_control.kerberos.enabled"));
        public static final String SECURITY_PROTOCOL = getParamValue("kafka_update_control.security.protocol");
        public static final String SASL_MECHANISM = getParamValue("kafka_update_control.sasl.mechanism");

        public static final String SASL_JAAS_CONFIG = getParamValue("kafka_update_control.sasl.jaas.config");
        public static final String JAAS_CONF_PATH = getParamValue("kafka_update_control.jaas.conf.path");
        public static final String KRB5_CONF_PATH = getParamValue("kafka_update_control.krb5.conf.path");
    }

    public static class ALERT_SINK {

        public static final String SINK_TYPE = getParamValue("alert_sink.source.type");


        //Kafka
        public static final String BOOTSTRAP_SERVERS = getParamValue("alert_sink.bootstrap.servers");
        public static final String TOPIC_ALERTS = getParamValue("alert_sink.topic.alerts");
        public static final String GROUP_ID = getParamValue("alert_sink.group.id");
        public static final String AUTO_OFFSET_RESET = getParamValue("alert_sink.auto.offset.reset");


        public static final boolean KERBEROS_ENABLED = Boolean.parseBoolean(getParamValue("alert_sink.kerberos.enabled"));
        public static final String SECURITY_PROTOCOL = getParamValue("alert_sink.security.protocol");
        public static final String SASL_MECHANISM = getParamValue("alert_sink.sasl.mechanism");

        public static final String SASL_JAAS_CONFIG = getParamValue("alert_sink.sasl.jaas.config");
        public static final String JAAS_CONF_PATH = getParamValue("alert_sink.jaas.conf.path");
        public static final String KRB5_CONF_PATH = getParamValue("alert_sink.krb5.conf.path");


        //JDBC
        public static final String JDBC_DRIVER = getParamValue("alert_sink.jdbc.driver");
        public static final String JDBC_URL = getParamValue("alert_sink.jdbc.url");
        public static final String JDBC_USERNAME = getParamValue("alert_sink.jdbc.username");
        public static final String JDBC_PASSWORD = getParamValue("alert_sink.jdbc.password");
    }

    public static class ROCKSDB_CONFIG {
        public static final String DB_PATH = getParamValue("rocksdb.path");
        public static final int WRITE_BUFFER_SIZE = Integer.parseInt(getParamValue("rocksdb.write.buffer.size"));
        public static final int MAX_OPEN_FILES = Integer.parseInt(getParamValue("rocksdb.max.open.files"));
        public static final int BLOCK_SIZE = Integer.parseInt(getParamValue("rocksdb.block.size"));
        public static final int CACHE_SIZE_MB = Integer.parseInt(getParamValue("rocksdb.cache.size.mb"));

        public static final int NUMBER_PARALLELISM = Integer.parseInt(getParamValue("number.parallelism"));

        public static final boolean ENABLE_CHECKPOINT = Boolean.parseBoolean(getParamValue("rocksdb.enable.checkpoint"));
        public static final boolean ENABLE_CHECKPOINT_UNALIGNED = Boolean.parseBoolean(getParamValue("rocksdb.enable.unaligned"));
        public static final long CHECKPOINTS_INTERVAL = Long.parseLong(getParamValue("rocksdb.checkpoints.interval"));
        public static final long MIN_PAUSE_BETWEEN_CHECKPOINTS = Long.parseLong(getParamValue("rocksdb.min.pause.between.checkpoints"));
    }
}
