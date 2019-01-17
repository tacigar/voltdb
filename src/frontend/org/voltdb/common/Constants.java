/* This file is part of VoltDB.
 * Copyright (C) 2008-2019 VoltDB Inc.
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with VoltDB.  If not, see <http://www.gnu.org/licenses/>.
 */

package org.voltdb.common;

import java.io.File;
import java.nio.charset.Charset;
import java.util.TimeZone;

/** Constants used by VoltDB */
public class Constants {

    public static final int UNDEFINED = -1;

    public static final Charset UTF8_ENCODING = Charset.forName("UTF-8");
    public static final Charset US_ASCII_ENCODING = Charset.forName("US-ASCII");

    /**
     * ODBC DateTime Format
     * If you need microseconds, you'll have to change this code or
     * export a BIGINT representing microseconds since an epoch
     */
    public static final String ODBC_DATE_FORMAT_STRING = "yyyy-MM-dd HH:mm:ss.SSS";

    /** Default heart beat timeout value */
    public static final int DEFAULT_HEARTBEAT_TIMEOUT_SECONDS = 90;
    public static final String VOLT_TMP_DIR = "volt.tmpdir";

    // Network constants
    public static final int DEFAULT_PORT = 21212;
    public static final int DISABLED_PORT = Constants.UNDEFINED;
    public static final int DEFAULT_ADMIN_PORT = 21211;
    public static final int DEFAULT_IPC_PORT = 10000;
    public static final int DEFAULT_DR_PORT = 5555;
    public static final int DEFAULT_HTTP_PORT = 8080;
    public static final int DEFAULT_HTTPS_PORT = 8443;
    public static final int DEFAULT_INTERNAL_PORT = 3021;
    public static final int DEFAULT_ZK_PORT = 7181;
    public static final String DEFAULT_INTERNAL_INTERFACE = "";
    public static final String DEFAULT_EXTERNAL_INTERFACE = "";

    /**
     * For generating the unique ID.
     * @see org.voltdb.iv2.UniqueIdGenerator#getNextUniqueId()
     */
    public static final int BACKWARD_TIME_FORGIVENESS_WINDOW_MS = 3000;

    // Authentication handshake codes.
    public static final byte AUTH_HANDSHAKE_VERSION = 2;
    public static final byte AUTH_SERVICE_NAME = 4;
    public static final byte AUTH_HANDSHAKE = 5;

    public static final String KERBEROS = "kerberos";

    public static final String DEFAULT_KEYSTORE_RESOURCE = "keystore";
    public static final String DEFAULT_KEYSTORE_PASSWD = "password";
    public static final String DEFAULT_TRUSTSTORE_RESOURCE =
            System.getProperty("java.home") + File.separator + "lib"
                    + File.separator + "security" + File.separator + "cacerts";
    public static final String DEFAULT_TRUSTSTORE_PASSWD = "changeit";

    // Reasons for a connection failure.
    public static final byte AUTHENTICATION_FAILURE = -1;
    public static final byte MAX_CONNECTIONS_LIMIT_ERROR = 1;
    public static final byte WIRE_PROTOCOL_TIMEOUT_ERROR = 2;
    public static final byte WIRE_PROTOCOL_FORMAT_ERROR = 3;
    public static final byte AUTHENTICATION_FAILURE_DUE_TO_REJOIN = 4;
    public static final byte EXPORT_DISABLED_REJECTION = 5;

    // from JDBC metadata generation
    public static final String JSON_PARTITION_PARAMETER = "partitionParameter";
    public static final String JSON_PARTITION_PARAMETER_TYPE = "partitionParameterType";
    public static final String JSON_SINGLE_PARTITION = "singlePartition";
    public static final String JSON_READ_ONLY = "readOnly";

    // The transaction ID layout.
    static final long UNUSED_SIGN_BITS = 1;
    static final long SEQUENCE_BITS = 49;
    static final long PARTITIONID_BITS = 14;

    // Maximum values for the transaction ID fields
    static final long SEQUENCE_MAX_VALUE = (1L << SEQUENCE_BITS) - 1L;
    static final int PARTITIONID_MAX_VALUE = (1 << PARTITIONID_BITS) - 1;

    // from MP Initiator
    public static final int MP_INIT_PID = PARTITIONID_MAX_VALUE;

    /** String that can be used to indicate NULL value in CSV files */
    public static final String CSV_NULL = "\\N";
    /** String that can be used to indicate NULL value in CSV files */
    public static final String QUOTED_CSV_NULL = "\"\\N\"";

    /** Default export group to use when no group name is provided. */
    public static final String DEFAULT_EXPORT_CONNECTOR_NAME = "__default__";

    // Special HTTP port values to disable or trigger auto-scan.
    public static final int HTTP_PORT_DISABLED = UNDEFINED;
    public static final int HTTP_PORT_AUTO = 0;

    public static final String VOLTDB_ROOT = "voltdbroot";
    public static final String CONFIG_DIR = "config";

    // Staged filenames for advanced deployments
    public static final String INITIALIZED_MARKER = ".initialized";
    public static final String TERMINUS_MARKER = ".shutdown_snapshot";
    public static final String INITIALIZED_PATHS = ".paths";
    public static final String STAGED_MESH = "_MESH";
    public static final String DEFAULT_CLUSTER_NAME = "database";
    public static final String DBROOT = Constants.VOLTDB_ROOT;
    public static final String MODULE_CACHE = ".bundles-cache";

    /** The name of the SQLStmt implied by a statement procedure's SQL statement. */
    public static final String ANON_STMT_NAME = "sql";

    /** The GMT time zone you know and love. */
    public static final TimeZone GMT_TIMEZONE = TimeZone.getTimeZone("GMT+0");

    /** The time zone VoltDB is actually using, currently always GMT. */
    public static final TimeZone VOLT_TIMEZONE = GMT_TIMEZONE;
}
