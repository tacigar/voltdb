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

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.lang.management.ManagementFactory;
import java.lang.management.ThreadInfo;
import java.lang.management.ThreadMXBean;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.List;
import java.util.TimeZone;
import java.util.concurrent.TimeUnit;

import org.voltcore.logging.VoltLogger;
import org.voltcore.utils.CoreUtils;
import org.voltcore.utils.OnDemandBinaryLogger;
import org.voltcore.utils.ShutdownHooks;
import org.voltdb.client.ClientFactory;
import org.voltdb.common.Constants;
import org.voltdb.snmp.SnmpTrapSender;
import org.voltdb.types.TimestampType;
import org.voltdb.utils.PlatformProperties;
import org.voltdb.utils.StackTrace;
import org.voltdb.utils.VoltTrace;

/**
 * VoltDB provides main() for the VoltDB server
 */
public class VoltDB {

    /** Whatever the default time zone was for this locale before we replaced it. */
    public static final TimeZone REAL_DEFAULT_TIMEZONE;

    // if VoltDB is running in your process, prepare to use UTC (GMT) timezone
    public synchronized static void setDefaultTimezone() {
        TimeZone.setDefault(Constants.GMT_TIMEZONE);
    }

    static {
        REAL_DEFAULT_TIMEZONE = TimeZone.getDefault();
        setDefaultTimezone();
        ClientFactory.increaseClientCountToOne();
    }

    /* helper functions to access current configuration values */
    public static boolean getLoadLibVOLTDB() {
        return !(m_config.m_noLoadLibVOLTDB);
    }

    public static BackendTarget getEEBackendType() {
        return m_config.m_backend;
    }

    public static void crashLocalVoltDB(String errMsg) {
        crashLocalVoltDB(errMsg, false, null);
    }

    /**
     * turn off client interface as fast as possible
     */
    private static boolean turnOffClientInterface() {
        // we don't expect this to ever fail, but if it does, skip to dying immediately
        VoltDBInterface vdbInstance = instance();
        if (vdbInstance != null) {
            ClientInterface ci = vdbInstance.getClientInterface();
            if (ci != null) {
                if (!ci.ceaseAllPublicFacingTrafficImmediately()) {
                    return false;
                }
            }
        }
        return true;
    }

    /**
     * send a SNMP trap crash notification
     * @param msg
     */
    private static void sendCrashSNMPTrap(String msg) {
        if (msg == null || msg.trim().isEmpty()) {
            return;
        }
        VoltDBInterface vdbInstance = instance();
        if (vdbInstance == null) {
            return;
        }
        SnmpTrapSender snmp = vdbInstance.getSnmpTrapSender();
        if (snmp == null) {
            return;
        }
        try {
            snmp.crash(msg);
        } catch (Throwable t) {
            VoltLogger log = new VoltLogger("HOST");
            log.warn("failed to issue a crash SNMP trap", t);
        }
    }
    /**
     * Exit the process with an error message, optionally with a stack trace.
     */
    public static void crashLocalVoltDB(String errMsg, boolean stackTrace, Throwable thrown) {
        crashLocalVoltDB(errMsg, stackTrace, thrown, true);
    }
    /**
     * Exit the process with an error message, optionally with a stack trace.
     */
    public static void crashLocalVoltDB(String errMsg, boolean stackTrace, Throwable thrown, boolean logFatal) {

        if (exitAfterMessage) {
            System.err.println(errMsg);
            VoltDB.exit(-1);
        }
        try {
            OnDemandBinaryLogger.flush();
        } catch (Throwable e) {}

        /*
         * InvocationTargetException suppresses information about the cause, so unwrap until
         * we get to the root cause
         */
        while (thrown instanceof InvocationTargetException) {
            thrown = ((InvocationTargetException)thrown).getCause();
        }

        // for test code
        wasCrashCalled = true;
        crashMessage = errMsg;
        if (ignoreCrash) {
            throw new AssertionError("Faux crash of VoltDB successful.");
        }
        if (CoreUtils.isJunitTest()) {
            VoltLogger log = new VoltLogger("HOST");
            log.warn("Declining to drop a crash file during a junit test.");
        }
        // end test code
        // send a snmp trap crash notification
        sendCrashSNMPTrap(errMsg);
        // try/finally block does its best to ensure death, no matter what context this
        // is called in
        try {
            // slightly less important than death, this try/finally block protects code that
            // prints a message to stdout
            try {
                // turn off client interface as fast as possible
                // we don't expect this to ever fail, but if it does, skip to dying immediately
                if (!turnOffClientInterface()) {
                    return; // this will jump to the finally block and die faster
                }

                // Flush trace files
                try {
                    VoltTrace.closeAllAndShutdown(new File(instance().getVoltDBRootPath(), "trace_logs").getAbsolutePath(),
                                                  TimeUnit.SECONDS.toMillis(10));
                } catch (IOException e) {}

                // Even if the logger is null, don't stop.  We want to log the stack trace and
                // any other pertinent information to a .dmp file for crash diagnosis
                List<String> currentStacktrace = new ArrayList<>();
                currentStacktrace.add("Stack trace from crashLocalVoltDB() method:");

                // Create a special dump file to hold the stack trace
                try
                {
                    TimestampType ts = new TimestampType(new java.util.Date());
                    CatalogContext catalogContext = VoltDB.instance().getCatalogContext();
                    String root = catalogContext != null ? VoltDB.instance().getVoltDBRootPath() + File.separator : "";
                    PrintWriter writer = new PrintWriter(root + "voltdb_crash" + ts.toString().replace(' ', '-') + ".txt");
                    writer.println("Time: " + ts);
                    writer.println("Message: " + errMsg);

                    writer.println();
                    writer.println("Platform Properties:");
                    PlatformProperties pp = PlatformProperties.getPlatformProperties();
                    String[] lines = pp.toLogLines(instance().getVersionString()).split("\n");
                    for (String line : lines) {
                        writer.println(line.trim());
                    }

                    if (thrown != null) {
                        writer.println();
                        writer.println("****** Exception Thread ****** ");
                        thrown.printStackTrace(writer);
                    }

                    StackTrace.print(writer, currentStacktrace);
                    writer.close();
                }
                catch (Throwable err)
                {
                    // shouldn't fail, but..
                    err.printStackTrace();
                }

                VoltLogger log = null;
                try
                {
                    log = new VoltLogger("HOST");
                }
                catch (RuntimeException rt_ex)
                { /* ignore */ }

                if (log != null)
                {
                    if (logFatal) {
                        log.fatal(errMsg);
                    }
                    if (thrown != null) {
                        if (stackTrace) {
                            log.fatal("Fatal exception", thrown);
                        } else {
                            log.fatal(thrown.toString());
                        }
                    } else {
                        if (stackTrace) {
                            for (String currentStackElem : currentStacktrace) {
                                log.fatal(currentStackElem);
                            }
                        }
                    }
                } else {
                    System.err.println(errMsg);
                    if (thrown != null) {
                        if (stackTrace) {
                            thrown.printStackTrace();
                        } else {
                            System.err.println(thrown.toString());
                        }
                    } else {
                        if (stackTrace) {
                            for (String currentStackElem : currentStacktrace) {
                                System.err.println(currentStackElem);
                            }
                        }
                    }
                }
            }
            finally {
                System.err.println("VoltDB has encountered an unrecoverable error and is exiting.");
                System.err.println("The log may contain additional information.");
            }
        }
        finally {
            ShutdownHooks.useOnlyCrashHooks();
            System.exit(-1);
        }
    }

    /*
     * For tests that causes failures,
     * allow them stop the crash and inspect.
     */
    public static boolean ignoreCrash = false;

    public static boolean wasCrashCalled = false;

    public static String crashMessage;

    public static boolean exitAfterMessage = false;
    /**
     * Exit the process with an error message, optionally with a stack trace.
     * Also notify all connected peers that the node is going down.
     */
    public static void crashGlobalVoltDB(String errMsg, boolean stackTrace, Throwable t) {
        // for test code
        wasCrashCalled = true;
        crashMessage = errMsg;
        if (ignoreCrash) {
            throw new AssertionError("Faux crash of VoltDB successful.");
        }
        // end test code

        // send a snmp trap crash notification
        sendCrashSNMPTrap(errMsg);
        try {
            // turn off client interface as fast as possible
            // we don't expect this to ever fail, but if it does, skip to dying immediately
            if (!turnOffClientInterface()) {
                return; // this will jump to the finally block and die faster
            }
            // instruct the rest of the cluster to die
            instance().getHostMessenger().sendPoisonPill(errMsg);
            // give the pill a chance to make it through the network buffer
            Thread.sleep(500);
        } catch (Exception e) {
            e.printStackTrace();
            // sleep even on exception in case the pill got sent before the exception
            try { Thread.sleep(500); } catch (InterruptedException e2) {}
        }
        // finally block does its best to ensure death, no matter what context this
        // is called in
        finally {
            crashLocalVoltDB(errMsg, stackTrace, t);
        }
    }

    /**
     * Entry point for the VoltDB server process.
     * @param args Requires catalog and deployment file locations.
     */
    public static void main(String[] args) {
        //Thread.setDefaultUncaughtExceptionHandler(new VoltUncaughtExceptionHandler());
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
            VoltDB.crashLocalVoltDB(errmsg, false, e);
        }
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

    @Override
    public Object clone() throws CloneNotSupportedException {
        throw new CloneNotSupportedException();
    }

    public static class SimulatedExitException extends RuntimeException {
        private static final long serialVersionUID = 1L;
        private final int status;
        public SimulatedExitException(int status) {
            this.status = status;
        }
        public int getStatus() {
            return status;
        }
    }

    public static void exit(int status) {
        if (CoreUtils.isJunitTest() || ignoreCrash) {
            throw new SimulatedExitException(status);
        }
        System.exit(status);
    }

    public static String generateThreadDump() {
        StringBuilder threadDumps = new StringBuilder();
        ThreadMXBean threadMXBean = ManagementFactory.getThreadMXBean();
        ThreadInfo[] threadInfos = threadMXBean.dumpAllThreads(true, true);
        for (ThreadInfo t : threadInfos) {
            threadDumps.append(t);
        }
        return threadDumps.toString();
    }

    public static boolean dumpThreadTraceToFile(String dumpDir, String fileName) {
        final File dir = new File(dumpDir);
        if (!dir.getParentFile().canWrite() || !dir.getParentFile().canExecute()) {
            System.err.println("Parent directory " + dir.getParentFile().getAbsolutePath() +
                    " is not writable");
            return false;
        }
        if (!dir.exists()) {
            if (!dir.mkdir()) {
                System.err.println("Failed to create directory " + dir.getAbsolutePath());
                return false;
            }
        }
        File file = new File(dumpDir, fileName);
        try (FileWriter writer = new FileWriter(file); PrintWriter out = new PrintWriter(writer)) {
            out.println(generateThreadDump());
        } catch (IOException e) {
            System.err.println("Failed to write to file " + file.getAbsolutePath());
            return false;
        }
        return true;
    }

    static VoltConfiguration m_config = new VoltConfiguration();
    private static VoltDBInterface singleton = new RealVoltDB();
}
