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

package org.voltdb.utils;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.voltcore.logging.VoltLogger;
import org.voltcore.utils.CoreUtils;
import org.voltcore.utils.OnDemandBinaryLogger;
import org.voltcore.utils.ShutdownHooks;
import org.voltdb.CatalogContext;
import org.voltdb.ClientInterface;
import org.voltdb.VoltDB;
import org.voltdb.VoltDBInterface;
import org.voltdb.snmp.SnmpTrapSender;
import org.voltdb.types.TimestampType;

public class Poisoner {

    /**
     * Turn off client interface as fast as possible
     */
    private static boolean turnOffClientInterface() {
        // we don't expect this to ever fail, but if it does, skip to dying immediately
        VoltDBInterface vdbInstance = VoltDB.instance();
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
        VoltDBInterface vdbInstance = VoltDB.instance();
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

    public static void crashLocalVoltDB(String errMsg) {
        crashLocalVoltDB(errMsg, false, null);
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
    public static void crashLocalVoltDB(String errMsg, boolean stackTrace, Throwable cause, boolean logFatal) {
        if (! verboseCrash) {
            System.err.println(errMsg);
            VoltDB.exit(-1);
        }

        OnDemandBinaryLogger.flush();
        /*
         * InvocationTargetException suppresses information about the cause, so unwrap until
         * we get to the root cause.
         */
        while (cause instanceof InvocationTargetException) {
            cause = ((InvocationTargetException)cause).getCause();
        }

        // For test code
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
        // send a SNMP trap crash notification
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
                    VoltTrace.closeAllAndShutdown(new File(VoltDB.instance().getVoltDBRootPath(), "trace_logs").getAbsolutePath(),
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
                    String[] lines = pp.toLogLines(VoltDB.instance().getVersionString()).split("\n");
                    for (String line : lines) {
                        writer.println(line.trim());
                    }

                    if (cause != null) {
                        writer.println();
                        writer.println("****** Exception Thread ****** ");
                        cause.printStackTrace(writer);
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
                    if (cause != null) {
                        if (stackTrace) {
                            log.fatal("Fatal exception", cause);
                        } else {
                            log.fatal(cause.toString());
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
                    if (cause != null) {
                        if (stackTrace) {
                            cause.printStackTrace();
                        } else {
                            System.err.println(cause.toString());
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

    public static boolean verboseCrash = true;
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
            VoltDB.instance().getHostMessenger().sendPoisonPill(errMsg);
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
}
