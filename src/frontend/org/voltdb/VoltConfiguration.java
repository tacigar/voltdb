package org.voltdb;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayDeque;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.NavigableSet;
import java.util.Queue;
import java.util.UUID;

import org.eclipse.jetty.util.ssl.SslContextFactory;
import org.voltcore.logging.VoltLog4jLogger;
import org.voltcore.logging.VoltLogger;
import org.voltcore.utils.PortGenerator;
import org.voltdb.common.Constants;
import org.voltdb.probe.MeshProber;
import org.voltdb.settings.ClusterSettings;
import org.voltdb.settings.NodeSettings;
import org.voltdb.settings.Settings;
import org.voltdb.settings.SettingsException;
import org.voltdb.utils.CatalogUtil;
import org.voltdb.utils.MiscUtils;
import org.voltdb.utils.Poisoner;
import org.voltdb.utils.VoltFile;

import com.google_voltpatches.common.collect.ImmutableList;
import com.google_voltpatches.common.collect.ImmutableMap;
import com.google_voltpatches.common.collect.ImmutableSortedSet;
import com.google_voltpatches.common.net.HostAndPort;

import io.netty.handler.ssl.SslContext;

/** Encapsulates VoltDB configuration parameters */
public class VoltConfiguration {

    private boolean m_validateSuccess;

    public int m_ipcPort = Constants.DEFAULT_IPC_PORT;

    protected static final VoltLogger hostLog = new VoltLogger("HOST");

    /** select normal JNI backend.
     *  IPC, Valgrind, HSQLDB, and PostgreSQL are the other options.
     */
    public BackendTarget m_backend = BackendTarget.NATIVE_EE_JNI;

    /** leader hostname */
    public String m_leader = null;

    /** name of the m_catalog JAR file */
    public String m_pathToCatalog = null;

    /** name of the deployment file */
    public String m_pathToDeployment = null;
    public boolean m_deploymentDefault = false;

    /** name of the license file, for commercial editions */
    public String m_pathToLicense = null;

    /** false if voltdb.so shouldn't be loaded (for example if JVM is
     *  started by voltrun).
     */
    public boolean m_noLoadLibVOLTDB = false;

    public String m_zkInterface = "127.0.0.1:" + Constants.DEFAULT_ZK_PORT;

    /** port number for the first client interface for each server */
    public int m_port = Constants.DEFAULT_PORT;
    public String m_clientInterface = "";

    /** override for the admin port number in the deployment file */
    public int m_adminPort = Constants.DISABLED_PORT;
    public String m_adminInterface = "";

    /** ssl context factory */
    public SslContextFactory m_sslContextFactory = null;

    /** ssl context for client and admin ports */
    public SslContext m_sslServerContext = null;
    public SslContext m_sslClientContext = null;

    /** enable ssl */
    public boolean m_sslEnable = Boolean.valueOf(System.getenv("ENABLE_SSL") == null ? Boolean.toString(Boolean.getBoolean("ENABLE_SSL")) : System.getenv("ENABLE_SSL"));

    /** enable ssl for external (https, client and admin port*/
    public boolean m_sslExternal = Boolean.valueOf(System.getenv("ENABLE_SSL") == null ? Boolean.toString(Boolean.getBoolean("ENABLE_SSL")) : System.getenv("ENABLE_SSL"));

    public boolean m_sslDR = Boolean.valueOf(System.getenv("ENABLE_DR_SSL") == null ? Boolean.toString(Boolean.getBoolean("ENABLE_DR_SSL")) : System.getenv("ENABLE_DR_SSL"));

    public boolean m_sslInternal = Boolean.valueOf(System.getenv("ENABLE_INTERNAL_SSL") == null ? Boolean.toString(Boolean.getBoolean("ENABLE_INTERNAL_SSL")) : System.getenv("ENABLE_INTERNAL_SSL"));

    /** port number to use to build intra-cluster mesh */
    public int m_internalPort = Constants.DEFAULT_INTERNAL_PORT;

    /** interface to listen to clients on (default: any) */
    public String m_externalInterface = Constants.DEFAULT_EXTERNAL_INTERFACE;

    /** interface to use for backchannel comm (default: any) */
    public String m_internalInterface = Constants.DEFAULT_INTERNAL_INTERFACE;

    /** port number to use for DR channel (override in the deployment file) */
    public int m_drAgentPortStart = Constants.DISABLED_PORT;
    public String m_drInterface = "";

    /** interface and port used for consumers to connect to DR on this cluster. Used in hosted env primarily **/
    public String m_drPublicHost;
    public int m_drPublicPort = Constants.DISABLED_PORT;

    /** HTTP port can't be set here, but eventually value will be reflected here */
    public int m_httpPort = Constants.HTTP_PORT_DISABLED;
    public String m_httpPortInterface = "";

    public String m_publicInterface = "";

    /** running the enterprise version? */
    public final boolean m_isEnterprise = org.voltdb.utils.MiscUtils.isPro();

    public int m_deadHostTimeoutMS =
        Constants.DEFAULT_HEARTBEAT_TIMEOUT_SECONDS * 1000;

    public boolean m_partitionDetectionEnabled = true;

    /** start up action */
    public StartAction m_startAction = null;

    /** start mode: normal, paused*/
    public OperationMode m_startMode = OperationMode.RUNNING;

    /**
     * At rejoin time an interface will be selected. It will be the
     * internal interface specified on the command line. If none is specified
     * then the interface that the system selects for connecting to
     * the pre-existing node is used. It is then stored here
     * so it can be used for receiving connections by RecoverySiteDestinationProcessor
     */
    public String m_selectedRejoinInterface = null;

    /**
     * Whether or not adhoc queries should generate debugging output
     */
    public boolean m_quietAdhoc = false;

    public final File m_commitLogDir = new File("/tmp");

    /**
     * How much (ms) to skew the timestamp generation for
     * the TransactionIdManager. Should be ZERO except for tests.
     */
    public long m_timestampTestingSalt = 0;

    /** true if we're running the rejoin tests. Not used in production. */
    public boolean m_isRejoinTest = false;

    public final Queue<String> m_networkCoreBindings = new ArrayDeque<>();
    public final Queue<String> m_computationCoreBindings = new ArrayDeque<>();
    public final Queue<String> m_executionCoreBindings = new ArrayDeque<>();
    public String m_commandLogBinding = null;

    /**
     * Allow a secret CLI config option to test multiple versions of VoltDB running together.
     * This is used to test online upgrade (currently, for hotfixes).
     * Also used to test error conditions like incompatible versions running together.
     */
    public String m_versionStringOverrideForTest = null;
    public String m_versionCompatibilityRegexOverrideForTest = null;
    public String m_buildStringOverrideForTest = null;

    /** Placement group */
    public String m_placementGroup = null;

    public boolean m_isPaused = false;

    /** GET option */
    public GetActionArgument m_getOption = null;
    /**
     * Name of output file in which get command will store it's result
     */
    public String m_getOutput = null;
    /**
     * Flag to indicate whether to force store the result even if there is already an existing
     * file with same name
     */
    public boolean m_forceGetCreate = false;

    private final static void referToDocAndExit() {
        System.out.println("Please refer to VoltDB documentation for command line usage.");
        System.out.flush();
        VoltDB.exit(-1);
    }

    public VoltConfiguration() {
        // Set start action create.  The cmd line validates that an action is specified, however,
        // defaulting it to create for local cluster test scripts
        m_startAction = StartAction.CREATE;
    }

    /** Behavior-less arg used to differentiate command lines from "ps" */
    public String m_tag;

    public int m_queryTimeout = 0;

    /** Force catalog upgrade even if version matches. */
    public static boolean m_forceCatalogUpgrade = false;

    /** Allow starting voltdb with non-empty managed directories. */
    public boolean m_forceVoltdbCreate = false;

    /** cluster name designation */
    public String m_clusterName = Constants.DEFAULT_CLUSTER_NAME;

    /** command line provided voltdbroot */
    public File m_voltdbRoot = new VoltFile(Constants.VOLTDB_ROOT);

    /** configuration UUID */
    public final UUID m_configUUID = UUID.randomUUID();

    /** holds a list of comma separated mesh formation coordinators */
    public String m_meshBrokers = null;

    /** holds a set of mesh formation coordinators */
    public NavigableSet<String> m_coordinators = ImmutableSortedSet.of();

    /** number of hosts that participate in a VoltDB cluster */
    public int m_hostCount = Constants.UNDEFINED;

    /** number of hosts that will be missing when the cluster is started up */
    public int m_missingHostCount = 0;

    /** not sites per host actually, number of local sites in this node */
    public int m_sitesperhost = Constants.UNDEFINED;

    /** allow elastic joins */
    public boolean m_enableAdd = false;

    /** apply safe mode strategy when recovering */
    public boolean m_safeMode = false;

    /** location of user supplied schema */
    public File m_userSchema = null;

    /** location of user supplied classes and resources jar file */
    public File m_stagedClassesPath = null;

    public int getZKPort() {
        return MiscUtils.getPortFromHostnameColonPort(m_zkInterface, Constants.DEFAULT_ZK_PORT);
    }

    public VoltConfiguration(PortGenerator ports) {
        // Default iv2 configuration to the environment settings.
        // Let explicit command line override the environment.
        m_port = ports.nextClient();
        m_adminPort = ports.nextAdmin();
        m_internalPort = ports.next();
        m_zkInterface = "127.0.0.1:" + ports.next();
        // Set start action create.  The cmd line validates that an action is specified, however,
        // defaulting it to create for local cluster test scripts
        m_startAction = StartAction.CREATE;
        m_coordinators = MeshProber.hosts(m_internalPort);
    }

    public VoltConfiguration(String args[]) {
        String arg;
        /*
         *  !!! D O  N O T  U S E  hostLog  T O  L O G ,  U S E  System.[out|err]  I N S T E A D
         */
        for (int i=0; i < args.length; ++i) {
            arg = args[i];
            // Some LocalCluster ProcessBuilder instances can result in an empty string
            // in the array args.  Ignore them.
            if (arg.equals(""))
            {
                continue;
            }

            // Handle request for help/usage
            if (arg.equalsIgnoreCase("-h") || arg.equalsIgnoreCase("--help")) {
                // We used to print usage here but now we have too many ways to start
                // VoltDB to offer help that isn't possibly quite wrong.
                // You can probably get here using the legacy voltdb3 script. The usage
                // is now a comment in that file.
                referToDocAndExit();
            }

            if (arg.equals("noloadlib")) {
                m_noLoadLibVOLTDB = true;
            }
            else if (arg.equals("ipc")) {
                m_backend = BackendTarget.NATIVE_EE_IPC;
            }
            else if (arg.equals("jni")) {
                m_backend = BackendTarget.NATIVE_EE_JNI;
            }
            else if (arg.equals("hsqldb")) {
                m_backend = BackendTarget.HSQLDB_BACKEND;
            }
            else if (arg.equals("postgresql")) {
                m_backend = BackendTarget.POSTGRESQL_BACKEND;
            }
            else if (arg.equals("postgis")) {
                m_backend = BackendTarget.POSTGIS_BACKEND;
            }
            else if (arg.equals("valgrind")) {
                m_backend = BackendTarget.NATIVE_EE_VALGRIND_IPC;
            }
            else if (arg.equals("quietadhoc"))
            {
                m_quietAdhoc = true;
            }
            // handle from the command line as two strings <catalog> <filename>
            else if (arg.equals("port")) {
                String portStr = args[++i];
                if (portStr.indexOf(':') != -1) {
                    HostAndPort hap = MiscUtils.getHostAndPortFromHostnameColonPort(portStr, m_port);
                    m_clientInterface = hap.getHost();
                    m_port = hap.getPort();
                } else {
                    m_port = Integer.parseInt(portStr);
                }
            } else if (arg.equals("adminport")) {
                String portStr = args[++i];
                if (portStr.indexOf(':') != -1) {
                    HostAndPort hap = MiscUtils.getHostAndPortFromHostnameColonPort(portStr, Constants.DEFAULT_ADMIN_PORT);
                    m_adminInterface = hap.getHost();
                    m_adminPort = hap.getPort();
                } else {
                    m_adminPort = Integer.parseInt(portStr);
                }
            } else if (arg.equals("internalport")) {
                String portStr = args[++i];
                if (portStr.indexOf(':') != -1) {
                    HostAndPort hap = MiscUtils.getHostAndPortFromHostnameColonPort(portStr, m_internalPort);
                    m_internalInterface = hap.getHost();
                    m_internalPort = hap.getPort();
                } else {
                    m_internalPort = Integer.parseInt(portStr);
                }
            } else if (arg.equals("drpublic")) {
                String publicStr = args[++i];
                if (publicStr.indexOf(':') != -1) {
                    HostAndPort hap = MiscUtils.getHostAndPortFromHostnameColonPort(publicStr, Constants.DEFAULT_DR_PORT);
                    m_drPublicHost = hap.getHost();
                    m_drPublicPort = hap.getPort();
                } else {
                    m_drPublicHost = publicStr;
                }
            } else if (arg.equals("replicationport")) {
                String portStr = args[++i];
                if (portStr.indexOf(':') != -1) {
                    HostAndPort hap = MiscUtils.getHostAndPortFromHostnameColonPort(portStr, Constants.DEFAULT_DR_PORT);
                    m_drInterface = hap.getHost();
                    m_drAgentPortStart = hap.getPort();
                } else {
                    m_drAgentPortStart = Integer.parseInt(portStr);
                }
            } else if (arg.equals("httpport")) {
                String portStr = args[++i];
                if (portStr.indexOf(':') != -1) {
                    HostAndPort hap = MiscUtils.getHostAndPortFromHostnameColonPort(portStr, Constants.DEFAULT_HTTP_PORT);
                    m_httpPortInterface = hap.getHost();
                    m_httpPort = hap.getPort();
                } else {
                    m_httpPort = Integer.parseInt(portStr);
                }
            } else if (arg.startsWith("zkport")) {
                //zkport should be default to loopback but for openshift needs to be specified as loopback is unavalable.
                String portStr = args[++i];
                if (portStr.indexOf(':') != -1) {
                    HostAndPort hap = MiscUtils.getHostAndPortFromHostnameColonPort(portStr, Constants.DEFAULT_ZK_PORT);
                    m_zkInterface = hap.getHost() + ":" + hap.getPort();
                } else {
                    m_zkInterface = "127.0.0.1:" + portStr.trim();
                }
            } else if (arg.equals("mesh")) {
                StringBuilder sbld = new StringBuilder(64);
                while ((++i < args.length && args[i].endsWith(",")) || (i+1 < args.length && args[i+1].startsWith(","))) {
                    sbld.append(args[i]);
                }
                if (i < args.length) {
                    sbld.append(args[i]);
                }
                m_meshBrokers = sbld.toString();
            } else if (arg.startsWith("mesh ")) {
                int next = i + 1;
                StringBuilder sbld = new StringBuilder(64).append(arg.substring("mesh ".length()));
                while ((++i < args.length && args[i].endsWith(",")) || (i+1 < args.length && args[i+1].startsWith(","))) {
                    sbld.append(args[i]);
                }
                if (i > next && i < args.length) {
                    sbld.append(args[i]);
                }
                m_meshBrokers = sbld.toString();
            } else if (arg.equals("hostcount")) {
                m_hostCount = Integer.parseInt(args[++i].trim());
            } else if (arg.equals("missing")) {
                m_missingHostCount = Integer.parseInt(args[++i].trim());
            }else if (arg.equals("sitesperhost")){
                m_sitesperhost = Integer.parseInt(args[++i].trim());
            } else if (arg.equals("publicinterface")) {
                m_publicInterface = args[++i].trim();
            } else if (arg.startsWith("publicinterface ")) {
                m_publicInterface = arg.substring("publicinterface ".length()).trim();
            } else if (arg.equals("externalinterface")) {
                m_externalInterface = args[++i].trim();
            }
            else if (arg.startsWith("externalinterface ")) {
                m_externalInterface = arg.substring("externalinterface ".length()).trim();
            }
            else if (arg.equals("internalinterface")) {
                m_internalInterface = args[++i].trim();
            }
            else if (arg.startsWith("internalinterface ")) {
                m_internalInterface = arg.substring("internalinterface ".length()).trim();
            } else if (arg.startsWith("networkbindings")) {
                for (String core : args[++i].split(",")) {
                    m_networkCoreBindings.offer(core);
                }
                System.out.println("Network bindings are " + m_networkCoreBindings);
            }
            else if (arg.startsWith("computationbindings")) {
                for (String core : args[++i].split(",")) {
                    m_computationCoreBindings.offer(core);
                }
                System.out.println("Computation bindings are " + m_computationCoreBindings);
            }
            else if (arg.startsWith("executionbindings")) {
                for (String core : args[++i].split(",")) {
                    m_executionCoreBindings.offer(core);
                }
                System.out.println("Execution bindings are " + m_executionCoreBindings);
            } else if (arg.startsWith("commandlogbinding")) {
                String binding = args[++i];
                if (binding.split(",").length > 1) {
                    throw new RuntimeException("Command log only supports a single set of bindings");
                }
                m_commandLogBinding = binding;
                System.out.println("Commanglog binding is " + m_commandLogBinding);
            }
            else if (arg.equals("host") || arg.equals("leader")) {
                m_leader = args[++i].trim();
            } else if (arg.startsWith("host")) {
                m_leader = arg.substring("host ".length()).trim();
            } else if (arg.startsWith("leader")) {
                m_leader = arg.substring("leader ".length()).trim();
            }
            // synonym for "rejoin host" for backward compatibility
            else if (arg.equals("rejoinhost")) {
                m_startAction = StartAction.REJOIN;
                m_leader = args[++i].trim();
            }
            else if (arg.startsWith("rejoinhost ")) {
                m_startAction = StartAction.REJOIN;
                m_leader = arg.substring("rejoinhost ".length()).trim();
            }

            else if (arg.equals("initialize")) {
                m_startAction = StartAction.INITIALIZE;
            }
            else if (arg.equals("probe")) {
                m_startAction = StartAction.PROBE;
                if (   args.length > i + 1
                        && args[i+1].trim().equals("safemode")) {
                        i += 1;
                        m_safeMode = true;
                    }
            }
            else if (arg.equals("create")) {
                m_startAction = StartAction.CREATE;
            }
            else if (arg.equals("recover")) {
                m_startAction = StartAction.RECOVER;
                if (   args.length > i + 1
                    && args[i+1].trim().equals("safemode")) {
                    m_startAction = StartAction.SAFE_RECOVER;
                    i += 1;
                    m_safeMode = true;
                }
            } else if (arg.equals("rejoin")) {
                m_startAction = StartAction.REJOIN;
            } else if (arg.startsWith("live rejoin")) {
                m_startAction = StartAction.LIVE_REJOIN;
            } else if (arg.equals("live") && args.length > i + 1 && args[++i].trim().equals("rejoin")) {
                m_startAction = StartAction.LIVE_REJOIN;
            } else if (arg.startsWith("add")) {
                m_startAction = StartAction.JOIN;
                m_enableAdd = true;
            } else if (arg.equals("noadd")) {
                m_enableAdd = false;
            } else if (arg.equals("enableadd")) {
                m_enableAdd = true;
            } else if (arg.equals("replica")) {
                System.err.println("The \"replica\" command line argument is deprecated. Please use " +
                                   "role=\"replica\" in the deployment file.");
                referToDocAndExit();
            } else if (arg.equals("dragentportstart")) {
                m_drAgentPortStart = Integer.parseInt(args[++i]);
            }

            // handle timestampsalt
            else if (arg.equals("timestampsalt")) {
                m_timestampTestingSalt = Long.parseLong(args[++i]);
            }
            else if (arg.startsWith("timestampsalt ")) {
                m_timestampTestingSalt = Long.parseLong(arg.substring("timestampsalt ".length()));
            }

            // handle behaviorless tag field
            else if (arg.equals("tag")) {
                m_tag = args[++i];
            }
            else if (arg.startsWith("tag ")) {
                m_tag = arg.substring("tag ".length());
            }

            else if (arg.equals("catalog")) {
                m_pathToCatalog = args[++i];
            }
            // and from ant as a single string "m_catalog filename"
            else if (arg.startsWith("catalog ")) {
                m_pathToCatalog = arg.substring("catalog ".length());
            }
            else if (arg.equals("deployment")) {
                m_pathToDeployment = args[++i];
            }
            else if (arg.equals("license")) {
                m_pathToLicense = args[++i];
            }
            else if (arg.equalsIgnoreCase("ipcport")) {
                String portStr = args[++i];
                m_ipcPort = Integer.valueOf(portStr);
            }
            else if (arg.equals("forcecatalogupgrade")) {
                System.out.println("Forced catalog upgrade will occur due to command line option.");
                m_forceCatalogUpgrade = true;
            }
            // version string override for testing online upgrade
            else if (arg.equalsIgnoreCase("versionoverride")) {
                m_versionStringOverrideForTest = args[++i].trim();
                m_versionCompatibilityRegexOverrideForTest = args[++i].trim();
            }
            else if (arg.equalsIgnoreCase("buildstringoverride")) {
                m_buildStringOverrideForTest = args[++i].trim();
            } else if (arg.equalsIgnoreCase("placementgroup")) {
                m_placementGroup = args[++i].trim();
            } else if (arg.equalsIgnoreCase("force")) {
                m_forceVoltdbCreate = true;
            } else if (arg.equalsIgnoreCase("paused")) {
                //Start paused.
                m_isPaused = true;
            } else if (arg.equalsIgnoreCase("voltdbroot")) {
                m_voltdbRoot = new VoltFile(args[++i]);
                if ( ! Constants.VOLTDB_ROOT.equals(m_voltdbRoot.getName())) {
                    m_voltdbRoot = new VoltFile(m_voltdbRoot, Constants.VOLTDB_ROOT);
                }
                if (!m_voltdbRoot.exists() && !m_voltdbRoot.mkdirs()) {
                    System.err.println("FATAL: Could not create directory \"" + m_voltdbRoot.getPath() + "\"");
                    referToDocAndExit();
                }
                try {
                    CatalogUtil.validateDirectory(Constants.VOLTDB_ROOT, m_voltdbRoot);
                } catch (RuntimeException e) {
                    System.err.println("FATAL: " + e.getMessage());
                    referToDocAndExit();
                }
            } else if (arg.equalsIgnoreCase("enableSSL")) {
                m_sslEnable = true;
            } else if (arg.equalsIgnoreCase("externalSSL")) {
                m_sslExternal = true;
            } else if (arg.equalsIgnoreCase("internalSSL")) {
                m_sslInternal = true;
            } else if (arg.equalsIgnoreCase("drSSL")) {
                m_sslDR = true;
            } else if (arg.equalsIgnoreCase("getvoltdbroot")) {
                //Can not use voltdbroot which creates directory we dont intend to create for get deployment etc.
                m_voltdbRoot = new VoltFile(args[++i]);
                if ( ! Constants.VOLTDB_ROOT.equals(m_voltdbRoot.getName())) {
                    m_voltdbRoot = new VoltFile(m_voltdbRoot, Constants.VOLTDB_ROOT);
                }
                if (!m_voltdbRoot.exists()) {
                    System.err.println("FATAL: " + m_voltdbRoot.getParentFile().getAbsolutePath() + " does not contain a "
                            + "valid database root directory. Use the --dir option to specify the path to the root.");
                    referToDocAndExit();
                }
            } else if (arg.equalsIgnoreCase("get")) {
                m_startAction = StartAction.GET;
                String argument = args[++i];
                if (argument == null || argument.trim().length() == 0) {
                    System.err.println("FATAL: Supply a valid non-null argument for \"get\" command. "
                            + "Supported arguments for get are: " + GetActionArgument.supportedVerbs());
                    referToDocAndExit();
                }

                try {
                    m_getOption = GetActionArgument.valueOf(GetActionArgument.class, argument.trim().toUpperCase());
                } catch (IllegalArgumentException excp) {
                    System.err.println("FATAL:" + argument + " is not a valid \"get\" command argument. Valid arguments for get command are: " + GetActionArgument.supportedVerbs());
                    referToDocAndExit();
                }
                m_getOutput = m_getOption.getDefaultOutput();
            } else if (arg.equalsIgnoreCase("file")) {
                m_getOutput = args[++i].trim();
            } else if (arg.equalsIgnoreCase("forceget")) {
                m_forceGetCreate = true;
            } else if (arg.equalsIgnoreCase("schema")) {
                m_userSchema = new File(args[++i].trim());
                if (!m_userSchema.exists()) {
                    System.err.println("FATAL: Supplied schema file " + m_userSchema + " does not exist.");
                    referToDocAndExit();
                }
                if (!m_userSchema.canRead()) {
                    System.err.println("FATAL: Supplied schema file " + m_userSchema + " can't be read.");
                    referToDocAndExit();
                }
                if (!m_userSchema.isFile()) {
                    System.err.println("FATAL: Supplied schema file " + m_userSchema + " is not an ordinary file.");
                    referToDocAndExit();
                }
            } else if (arg.equalsIgnoreCase("classes")) {
                m_stagedClassesPath = new File(args[++i].trim());
                if (!m_stagedClassesPath.exists()){
                    System.err.println("FATAL: Supplied classes jar file " + m_stagedClassesPath + " does not exist.");
                    referToDocAndExit();
                }
                if (!m_stagedClassesPath.canRead()) {
                    System.err.println("FATAL: Supplied classes jar file " + m_stagedClassesPath + " can't be read.");
                    referToDocAndExit();
                }
                if (!m_stagedClassesPath.isFile()) {
                    System.err.println("FATAL: Supplied classes jar file " + m_stagedClassesPath + " is not an ordinary file.");
                    referToDocAndExit();
                }
            } else {
                System.err.println("FATAL: Unrecognized option to VoltDB: " + arg);
                referToDocAndExit();
            }
        }
        // Get command
        if (m_startAction == StartAction.GET) {
            // We don't want crash file created.
            Poisoner.verboseCrash = false;
            inspectGetCommand();
            return;
        }
        // set file logger root file directory. From this point on you can use loggers
        if (m_startAction != null && !m_startAction.isLegacy()) {
            VoltLog4jLogger.setFileLoggerRoot(m_voltdbRoot);
        }
        /*
         *  !!! F R O M  T H I S  P O I N T  O N  Y O U  M A Y  U S E  hostLog  T O  L O G
         */
        if (m_forceCatalogUpgrade) {
            hostLog.info("Forced catalog upgrade will occur due to command line option.");
        }

        // If no action is specified, issue an error.
        if (null == m_startAction) {
            hostLog.fatal("You must specify a startup action, either init, start, create, recover, rejoin, collect, or compile.");
            referToDocAndExit();
        }


        // ENG-3035 Warn if 'recover' action has a catalog since we won't
        // be using it. Only cover the 'recover' action since 'start' sometimes
        // acts as 'recover' and other times as 'create'.
        if (m_startAction.doesRecover() && m_pathToCatalog != null) {
            hostLog.warn("Catalog is ignored for 'recover' action.");
        }

        /*
         * ENG-2815 If deployment is null (the user wants the default) and
         * the start action is not rejoin and leader is null, supply the
         * only valid leader value ("localhost").
         */
        if (m_leader == null && m_pathToDeployment == null && !m_startAction.doesRejoin()) {
            m_leader = "localhost";
        }

        if (m_startAction == StartAction.PROBE) {
            checkInitializationMarker();
        } else if (m_startAction == StartAction.INITIALIZE) {
            if (isInitialized() && !m_forceVoltdbCreate) {
                hostLog.fatal(m_voltdbRoot + " is already initialized"
                        + "\nUse the start command to start the initialized database or use init --force"
                        + " to overwrite existing files.");
                referToDocAndExit();
            }
        } else if (m_meshBrokers == null || m_meshBrokers.trim().isEmpty()) {
            if (m_leader != null) {
                m_meshBrokers = m_leader;
            }
        }
        if (m_meshBrokers != null) {
            m_coordinators = MeshProber.hosts(m_meshBrokers);
            if (m_leader == null) {
                m_leader = m_coordinators.first();
            }
        }
        if (m_startAction == StartAction.PROBE && m_hostCount == Constants.UNDEFINED && m_coordinators.size() > 1) {
            m_hostCount = m_coordinators.size();
        }
    }

    private boolean isInitialized() {
        File inzFH = new VoltFile(m_voltdbRoot, Constants.INITIALIZED_MARKER);
        return inzFH.exists() && inzFH.isFile() && inzFH.canRead();
    }

    private void inspectGetCommand() {
        String parentPath = m_voltdbRoot.getParent();
        // check voltdbroot
        if (!m_voltdbRoot.exists()) {
            try {
                parentPath = m_voltdbRoot.getCanonicalFile().getParent();
            } catch (IOException io) {}
            System.err.println("FATAL: " + parentPath + " does not contain a "
                    + "valid database root directory. Use the --dir option to specify the path to the root.");
            referToDocAndExit();
        }
        File configInfoDir = new VoltFile(m_voltdbRoot, Constants.CONFIG_DIR);
        switch (m_getOption) {
            case DEPLOYMENT: {
                File depFH = new VoltFile(configInfoDir, "deployment.xml");
                if (!depFH.exists()) {
                    System.out.println("FATAL: Deployment file \"" + depFH.getAbsolutePath() + "\" not found.");
                    referToDocAndExit();
                }
                m_pathToDeployment = depFH.getAbsolutePath();
                return;
            }
            case SCHEMA:
            case CLASSES: {
                // catalog.jar contains DDL and proc classes with which the database was
                // compiled. Check if catalog.jar exists as it is needed to fetch ddl (get
                // schema) as well as procedures (get classes)
                File catalogFH = new VoltFile(configInfoDir, CatalogUtil.CATALOG_FILE_NAME);
                if (!catalogFH.exists()) {
                    try {
                        parentPath = m_voltdbRoot.getCanonicalFile().getParent();
                    } catch (IOException io) {}
                    System.err.println("FATAL: "+ m_getOption.name().toUpperCase() + " not found in the provided database directory " + parentPath  +
                            ". Make sure the database has been started ");
                    referToDocAndExit();
                }
                m_pathToCatalog = catalogFH.getAbsolutePath();
                return;
            }
        }
    }

    public Map<String,String> asClusterSettingsMap() {
        Settings.initialize(m_voltdbRoot);
        return ImmutableMap.<String, String>builder()
                .put(ClusterSettings.HOST_COUNT, Integer.toString(m_hostCount))
                .build();
    }

    public Map<String,String> asPathSettingsMap() {
        Settings.initialize(m_voltdbRoot);
        return ImmutableMap.<String, String>builder()
                .put(NodeSettings.VOLTDBROOT_PATH_KEY, m_voltdbRoot.getPath())
                .build();
    }

    public Map<String,String> asRelativePathSettingsMap() {
        Settings.initialize(m_voltdbRoot);
        File currDir;
        File voltdbroot;
        try {
            currDir = new File("").getCanonicalFile();
            voltdbroot = m_voltdbRoot.getCanonicalFile();
        } catch (IOException e) {
            throw new SettingsException(
                    "Failed to relativize voltdbroot " +
                    m_voltdbRoot.getPath() +
                    ". Reason: " +
                    e.getMessage());
        }
        String relativePath = currDir.toPath().relativize(voltdbroot.toPath()).toString();
        return ImmutableMap.<String, String>builder()
                .put(NodeSettings.VOLTDBROOT_PATH_KEY, relativePath)
                .build();
    }

    public Map<String, String> asNodeSettingsMap() {
        return ImmutableMap.<String, String>builder()
                .put(NodeSettings.LOCAL_SITES_COUNT_KEY, Integer.toString(m_sitesperhost))
                .build();
    }

    public ClusterSettings asClusterSettings() {
        return ClusterSettings.create(asClusterSettingsMap());
    }

    List<File> getInitMarkers() {
        return ImmutableList.<File>builder()
                .add(new VoltFile(m_voltdbRoot, Constants.INITIALIZED_MARKER))
                .add(new VoltFile(m_voltdbRoot, Constants.INITIALIZED_PATHS))
                .add(new VoltFile(m_voltdbRoot, Constants.CONFIG_DIR))
                .add(new VoltFile(m_voltdbRoot, Constants.STAGED_MESH))
                .add(new VoltFile(m_voltdbRoot, Constants.TERMINUS_MARKER))
                .build();
    }

    /**
     * Checks for the initialization marker on initialized voltdbroot directory
     */
    private void checkInitializationMarker() {

        File inzFH = new VoltFile(m_voltdbRoot, Constants.INITIALIZED_MARKER);
        File deploymentFH = new VoltFile(new VoltFile(m_voltdbRoot, Constants.CONFIG_DIR), "deployment.xml");
        File configCFH = null;
        File optCFH = null;

        if (m_pathToDeployment != null && !m_pathToDeployment.trim().isEmpty()) {
            try {
                configCFH = deploymentFH.getCanonicalFile();
            } catch (IOException e) {
                hostLog.fatal("Could not resolve file location " + deploymentFH, e);
                referToDocAndExit();
            }
            try {
                optCFH = new VoltFile(m_pathToDeployment).getCanonicalFile();
            } catch (IOException e) {
                hostLog.fatal("Could not resolve file location " + optCFH, e);
                referToDocAndExit();
            }
            if (!configCFH.equals(optCFH)) {
                hostLog.fatal("In startup mode you may only specify " + deploymentFH + " for deployment, You specified: " + optCFH);
                referToDocAndExit();
            }
        } else {
            m_pathToDeployment = deploymentFH.getPath();
        }

        if (!inzFH.exists() || !inzFH.isFile() || !inzFH.canRead()) {
            hostLog.fatal("Specified directory is not a VoltDB initialized root");
            referToDocAndExit();
        }

        String stagedName = null;
        try (BufferedReader br = new BufferedReader(new FileReader(inzFH))) {
            stagedName = br.readLine();
        } catch (IOException e) {
            hostLog.fatal("Unable to access initialization marker at " + inzFH, e);
            referToDocAndExit();
        }

        if (m_clusterName != null && !m_clusterName.equals(stagedName)) {
            hostLog.fatal("The database root directory has changed. Either initialization did not complete properly or the directory has been corrupted. You must reinitialize the database directory before using it.");
            referToDocAndExit();
        } else {
            m_clusterName = stagedName;
        }
        try {
            if (m_meshBrokers == null || m_meshBrokers.trim().isEmpty()) {
                File meshFH = new VoltFile(m_voltdbRoot, Constants.STAGED_MESH);
                if (meshFH.exists() && meshFH.isFile() && meshFH.canRead()) {
                    try (BufferedReader br = new BufferedReader(new FileReader(meshFH))) {
                        m_meshBrokers = br.readLine();
                    } catch (IOException e) {
                        hostLog.fatal("Unable to read cluster name given at initialization from " + inzFH, e);
                        referToDocAndExit();
                    }
                }
            }
        } catch (IllegalArgumentException e) {
            hostLog.fatal("Unable to validate mesh argument \"" + m_meshBrokers + "\"", e);
            referToDocAndExit();
        }
    }

    private void generateFatalLog(String fatalMsg) {
        if (m_validateSuccess) {
            m_validateSuccess = false;
            StringBuilder sb = new StringBuilder(2048).append("Command line arguments: ");
            sb.append(System.getProperty("sun.java.command", "[not available]"));
            hostLog.info(sb.toString());
        }
        hostLog.fatal(fatalMsg);
    }


    /**
     * Validates configuration settings and logs errors to the host log.
     * You typically want to have the system exit when this fails, but
     * this functionality is left outside of the method so that it is testable.
     * @return Returns true if all required configuration settings are present.
     */
    public boolean validate() {
        m_validateSuccess = true;

        EnumSet<StartAction> hostNotRequred = EnumSet.of(StartAction.INITIALIZE,StartAction.GET);
        if (m_startAction == null) {
            generateFatalLog("The startup action is missing (either create, recover or rejoin).");
        }
        if (m_leader == null && !hostNotRequred.contains(m_startAction)) {
            generateFatalLog("The hostname is missing.");
        }

        // check if start action is not valid in community
        if ((!m_isEnterprise) && (m_startAction.isEnterpriseOnly())) {
            StringBuilder sb = new StringBuilder().append(
                    "VoltDB Community Edition only supports the \"create\" start action.");
            sb.append(m_startAction.featureNameForErrorString());
            sb.append(" is an Enterprise Edition feature. An evaluation edition is available at http://voltdb.com.");
            generateFatalLog(sb.toString());
        }
        EnumSet<StartAction> requiresDeployment = EnumSet.complementOf(
                EnumSet.of(StartAction.REJOIN,StartAction.LIVE_REJOIN,StartAction.JOIN,StartAction.INITIALIZE, StartAction.PROBE));
        // require deployment file location
        if (requiresDeployment.contains(m_startAction)) {
            // require deployment file location (null is allowed to receive default deployment)
            if (m_pathToDeployment != null && m_pathToDeployment.trim().isEmpty()) {
                generateFatalLog("The deployment file location is empty.");
            }
        }

        //--paused only allowed in CREATE/RECOVER/SAFE_RECOVER
        EnumSet<StartAction> pauseNotAllowed = EnumSet.of(StartAction.JOIN,StartAction.LIVE_REJOIN,StartAction.REJOIN);
        if (m_isPaused && pauseNotAllowed.contains(m_startAction)) {
            generateFatalLog("Starting in admin mode is only allowed when using start, create or recover.");
        }
        if (!hostNotRequred.contains(m_startAction) && m_coordinators.isEmpty()) {
            generateFatalLog("List of hosts is missing");
        }

        if (m_startAction != StartAction.PROBE && m_hostCount != Constants.UNDEFINED) {
            generateFatalLog("Option \"--count\" may only be specified when using start");
        }
        if (m_startAction == StartAction.PROBE && m_hostCount != Constants.UNDEFINED && m_hostCount < m_coordinators.size()) {
            generateFatalLog("List of hosts is greater than option \"--count\"");
        }
        if (m_startAction == StartAction.PROBE && m_hostCount != Constants.UNDEFINED && m_hostCount < 0) {
            generateFatalLog("\"--count\" may not be specified with negative values");
        }
        if (m_startAction == StartAction.JOIN && !m_enableAdd) {
            generateFatalLog("\"add\" and \"noadd\" options cannot be specified at the same time");
        }
        return m_validateSuccess;
    }

    /**
     * Helper to set the path for compiled jar files.
     *  Could also live in VoltProjectBuilder but any code that creates
     *  a catalog will probably start VoltDB with a Configuration
     *  object. Perhaps this is more convenient?
     * @return the path chosen for the catalog.
     */
    public String setPathToCatalogForTest(String jarname) {
        m_pathToCatalog = getPathToCatalogForTest(jarname);
        return m_pathToCatalog;
    }

    public static String getPathToCatalogForTest(String jarname) {
        if (jarname == null) {
            return null; // NewCLI tests that init with schema do not want a pre-compiled catalog
        }

        // first try to get the "right" place to put the thing
        if (System.getenv("TEST_DIR") != null) {
            File testDir = new File(System.getenv("TEST_DIR"));
            // Create the folder as needed so that "ant junitclass" works when run before
            // testobjects is created.
            if (!testDir.exists()) {
                boolean created = testDir.mkdirs();
                assert(created);
            }
            // returns a full path, like a boss
            return testDir.getAbsolutePath() + File.separator + jarname;
        }

        // try to find an obj directory
        String userdir = System.getProperty("user.dir");
        String buildMode = System.getProperty("build");
        if (buildMode == null) {
            buildMode = "release";
        }
        assert(buildMode.length() > 0);
        if (userdir != null) {
            File userObjDir = new File(userdir + File.separator + "obj" + File.separator + buildMode);
            if (userObjDir.exists() && userObjDir.isDirectory() && userObjDir.canWrite()) {
                File testobjectsDir = new File(userObjDir.getPath() + File.separator + "testobjects");
                if (!testobjectsDir.exists()) {
                    boolean created = testobjectsDir.mkdir();
                    assert(created);
                }
                assert(testobjectsDir.isDirectory());
                assert(testobjectsDir.canWrite());
                return testobjectsDir.getAbsolutePath() + File.separator + jarname;
            }
        }

        // otherwise use a local dir
        File testObj = new File("testobjects");
        if (!testObj.exists()) {
            testObj.mkdir();
        }
        assert(testObj.isDirectory());
        assert(testObj.canWrite());
        return testObj.getAbsolutePath() + File.separator + jarname;
    }

    public int getQueryTimeout() {
       return VoltDB.m_config.m_queryTimeout;
    }
}