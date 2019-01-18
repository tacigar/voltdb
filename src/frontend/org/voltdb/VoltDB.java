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

package org.voltdb;

import java.util.TimeZone;

import org.voltcore.utils.CoreUtils;
import org.voltdb.client.ClientFactory;
import org.voltdb.common.Constants;
import org.voltdb.exceptions.SimulatedExitException;
import org.voltdb.utils.Poisoner;

/**
 * VoltDB provides main() for the VoltDB server
 */
public class VoltDB {

    /**
     * Entry point for the VoltDB server process.
     * @param args Requires catalog and deployment file locations.
     */
    public static void main(String[] args) {
        VoltConfiguration config = new VoltConfiguration(args);
        try {
            if (!config.validate()) {
                System.exit(-1);
            } else {
                if (config.m_startAction == StartAction.GET) {
                    cli(config);
                } else {
                    initialize(config);
                    instance().run();
                }
            }
        }
        catch (OutOfMemoryError e) {
            String errmsg = "VoltDB Main thread: ran out of Java memory. This node will shut down.";
            Poisoner.crashLocalVoltDB(errmsg, false, e);
        }
    }

    /** If VoltDB is running in your process, prepare to use UTC (GMT) time zone. */
    public synchronized static void setDefaultTimeZone() {
        TimeZone.setDefault(Constants.GMT_TIMEZONE);
    }

    static {
        setDefaultTimeZone();
        ClientFactory.increaseClientCountToOne();
    }

    /** Helper function to access current configuration values */
    public static boolean getLoadLibVOLTDB() {
        return ! m_config.m_noLoadLibVOLTDB;
    }

    public static BackendTarget getEEBackendType() {
        return m_config.m_backend;
    }

    /**
     * Initialize the VoltDB server.
     * @param config  The VoltDB.Configuration to use to initialize the server.
     */
    public static void initialize(VoltConfiguration config) {
        m_config = config;
        instance().initialize(config);
    }

    /**
     * Run CLI operations
     * @param config  The VoltDB.Configuration to use for getting configuration via CLI
     */
    public static void cli(VoltConfiguration config) {
        m_config = config;
        instance().cli(config);
    }

    /**
     * Retrieve a reference to the object implementing VoltDBInterface.  When
     * running a real server (and not a test harness), this instance will only
     * be useful after calling VoltDB.initialize().
     *
     * @return A reference to the underlying VoltDBInterface object.
     */
    public static VoltDBInterface instance() {
        return singleton;
    }

    /**
     * Useful only for unit testing.
     *
     * Replace the default VoltDB server instance with an instance of
     * VoltDBInterface that is used for testing.
     *
     */
    public static void replaceVoltDBInstanceForTest(VoltDBInterface testInstance) {
        singleton = testInstance;
    }

    public static String getPublicReplicationInterface() {
        return (m_config.m_drPublicHost == null || m_config.m_drPublicHost.isEmpty()) ?
                "" : m_config.m_drPublicHost;
    }

    public static int getPublicReplicationPort() {
        return m_config.m_drPublicPort;
    }

    /**
     * Selects the a specified m_drInterface over a specified m_externalInterface from m_config
     * @return an empty string when neither are specified
     */
    public static String getDefaultReplicationInterface() {
        if (m_config.m_drInterface == null || m_config.m_drInterface.isEmpty()) {
            if (m_config.m_externalInterface == null) {
                return "";
            }
            else {
                return m_config.m_externalInterface;
            }
        }
        else {
            return m_config.m_drInterface;
        }
    }

    public static int getReplicationPort(int deploymentFilePort) {
        if (m_config.m_drAgentPortStart != -1) {
            return m_config.m_drAgentPortStart;
        }
        else {
            return deploymentFilePort;
        }
    }

    @Override public Object clone() throws CloneNotSupportedException {
        throw new CloneNotSupportedException();
    }

    public static void exit(int status) {
        if (CoreUtils.isJunitTest() || Poisoner.ignoreCrash) {
            throw new SimulatedExitException(status);
        }
        System.exit(status);
    }

    static VoltConfiguration m_config = new VoltConfiguration();
    private static VoltDBInterface singleton = new RealVoltDB();
}
