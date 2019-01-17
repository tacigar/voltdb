package org.voltdb.utils;

import java.io.File;
import java.io.PrintWriter;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.voltcore.logging.VoltLogger;
import org.voltcore.messaging.HostMessenger;
import org.voltcore.utils.CoreUtils;
import org.voltdb.CatalogContext;
import org.voltdb.VoltDB;

/** Stack trace related runtime APIs for VoltDB */
public class StackTrace {

    /**
     * Create a file that starts with the supplied message and contains
     * human readable stack traces for all Java threads in the current process.
     * @param message the supplied message.
     */
    public static void dumpToFile(String message) {
        if (CoreUtils.isJunitTest()) {
            VoltLogger log = new VoltLogger("HOST");
            log.warn("Declining to drop a stack trace during a junit test.");
            return;
        }
        SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd-HH:mm:ss.SSSZ");
        String dateString = sdf.format(new Date());
        CatalogContext catalogContext = VoltDB.instance().getCatalogContext();
        HostMessenger hm = VoltDB.instance().getHostMessenger();
        int hostId = 0;
        if (hm != null) {
            hostId = hm.getHostId();
        }
        String root = catalogContext != null ? VoltDB.instance().getVoltDBRootPath() + File.separator : "";
        try {
            PrintWriter writer = new PrintWriter(root + "host" + hostId + "-" + dateString + ".txt");
            writer.println(message);
            printStackTraces(writer);
            writer.flush();
            writer.close();
        } catch (Exception e) {
            try {
                VoltLogger log = new VoltLogger("HOST");
                log.error("Error while dropping stack trace for \"" + message + "\"", e);
            } catch (RuntimeException rt_ex) {
                e.printStackTrace();
            }
        }
    }

    /**
     * Print stack traces for all threads in the current process to the supplied writer.
     * @param writer the writer for printing the stack trace.
     */
    public static void printStackTraces(PrintWriter writer) {
        print(writer, null);
    }

    /**
     * Print stack traces for all threads in the current process to the supplied writer.
     * @param writer the writer for printing the stack trace.
     * @param currentStacktrace if not null, the stack frames will be stored in this list.
     */
    public static void print(PrintWriter writer, List<String> currentStacktrace) {
        if (currentStacktrace == null) {
            currentStacktrace = new ArrayList<>();
        }

        Map<Thread, StackTraceElement[]> traces = Thread.getAllStackTraces();
        StackTraceElement[] myTrace = traces.get(Thread.currentThread());
        for (StackTraceElement ste : myTrace) {
            currentStacktrace.add(ste.toString());
        }

        writer.println();
        writer.println("****** Current Thread ****** ");
        for (String currentStackElem : currentStacktrace) {
            writer.println(currentStackElem);
        }

        writer.println("****** All Threads ******");
        Iterator<Thread> it = traces.keySet().iterator();
        while (it.hasNext())
        {
            Thread key = it.next();
            writer.println();
            StackTraceElement[] st = traces.get(key);
            writer.println("****** " + key + " ******");
            for (StackTraceElement ste : st) {
                writer.println(ste);
            }
        }
    }
}
