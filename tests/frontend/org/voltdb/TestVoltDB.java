/* This file is part of VoltDB.
 * Copyright (C) 2008-2019 VoltDB Inc.
 *
 * Permission is hereby granted, free of charge, to any person obtaining
 * a copy of this software and associated documentation files (the
 * "Software"), to deal in the Software without restriction, including
 * without limitation the rights to use, copy, modify, merge, publish,
 * distribute, sublicense, and/or sell copies of the Software, and to
 * permit persons to whom the Software is furnished to do so, subject to
 * the following conditions:
 *
 * The above copyright notice and this permission notice shall be
 * included in all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
 * EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
 * MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.
 * IN NO EVENT SHALL THE AUTHORS BE LIABLE FOR ANY CLAIM, DAMAGES OR
 * OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE,
 * ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
 * OTHER DEALINGS IN THE SOFTWARE.
 */

package org.voltdb;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.voltdb.benchmark.tpcc.TPCCProjectBuilder;
import org.voltdb.catalog.Catalog;
import org.voltdb.common.Constants;
import org.voltdb.compiler.VoltProjectBuilder.RoleInfo;
import org.voltdb.compiler.VoltProjectBuilder.UserInfo;
import org.voltdb.utils.BuildDirectoryUtils;
import org.voltdb.utils.CatalogUtil;
import org.voltdb.utils.MiscUtils;

final public class TestVoltDB {

    @BeforeClass
    public static void setupClass() throws Exception {
        System.setProperty("VOLT_JUSTATEST", "YESYESYES");
    }

    @Before
    public void setup() {
        VoltDB.ignoreCrash = true;
    }

    @Rule
    public final TemporaryFolder tmp = new TemporaryFolder();

    @Test
    public void testConfigurationConstructor() {
        VoltConfiguration blankConfig = new VoltConfiguration();
        assertFalse(blankConfig.m_noLoadLibVOLTDB);
        assertEquals(BackendTarget.NATIVE_EE_JNI, blankConfig.m_backend);
        assertEquals(null, blankConfig.m_pathToCatalog);
        assertEquals(null, blankConfig.m_pathToDeployment);
        assertEquals(Constants.DEFAULT_PORT, blankConfig.m_port);

        String args1[] = { "create", "noloadlib" };
        assertTrue(new VoltConfiguration(args1).m_noLoadLibVOLTDB);

        String args2[] = { "create", "hsqldb" };
        VoltConfiguration cfg2 = new VoltConfiguration(args2);
        assertEquals(BackendTarget.HSQLDB_BACKEND, cfg2.m_backend);
        String args3[] = { "create", "jni" };
        VoltConfiguration cfg3 = new VoltConfiguration(args3);
        assertEquals(BackendTarget.NATIVE_EE_JNI, cfg3.m_backend);
        String args4[] = { "create", "ipc" };
        VoltConfiguration cfg4 = new VoltConfiguration(args4);
        assertEquals(BackendTarget.NATIVE_EE_IPC, cfg4.m_backend);
        // what happens if arguments conflict?
        String args5[] = { "create", "ipc", "hsqldb" };
        VoltConfiguration cfg5 = new VoltConfiguration(args5);
        assertEquals(BackendTarget.HSQLDB_BACKEND, cfg5.m_backend);

        String args9[] = { "create", "catalog xtestxstringx" };
        VoltConfiguration cfg9 = new VoltConfiguration(args9);
        assertEquals("xtestxstringx", cfg9.m_pathToCatalog);
        String args10[] = { "create", "catalog", "ytestystringy" };
        VoltConfiguration cfg10 = new VoltConfiguration(args10);
        assertEquals("ytestystringy", cfg10.m_pathToCatalog);

        String args12[] = { "create", "port", "1234" };
        VoltConfiguration cfg12 = new VoltConfiguration(args12);
        assertEquals(1234, cfg12.m_port);
        String args13[] = { "create", "port", "5678" };
        VoltConfiguration cfg13 = new VoltConfiguration(args13);
        assertEquals(5678, cfg13.m_port);

        String args14[] = { "create" };
        VoltConfiguration cfg14 = new VoltConfiguration(args14);
        assertEquals(StartAction.CREATE, cfg14.m_startAction);
        String args15[] = { "recover" };
        VoltConfiguration cfg15 = new VoltConfiguration(args15);
        assertEquals(StartAction.RECOVER, cfg15.m_startAction);
        String args16[] = { "recover", "safemode" };
        VoltConfiguration cfg16 = new VoltConfiguration(args16);
        assertEquals(StartAction.SAFE_RECOVER, cfg16.m_startAction);

        // test host:port formats
        String args18[] = {"create", "port", "localhost:5678"};
        VoltConfiguration cfg18 = new VoltConfiguration(args18);
        assertEquals(5678, cfg18.m_port);
        assertEquals("localhost", cfg18.m_clientInterface);

        String args19[] = {"create", "adminport", "localhost:5678"};
        VoltConfiguration cfg19 = new VoltConfiguration(args19);
        assertEquals(5678, cfg19.m_adminPort);
        assertEquals("localhost", cfg19.m_adminInterface);

        String args20[] = {"create", "httpport", "localhost:7777"};
        VoltConfiguration cfg20 = new VoltConfiguration(args20);
        assertEquals(7777, cfg20.m_httpPort);
        assertEquals("localhost", cfg20.m_httpPortInterface);

        String args21[] = {"create", "internalport", "localhost:7777"};
        VoltConfiguration cfg21 = new VoltConfiguration(args21);
        assertEquals(7777, cfg21.m_internalPort);
        assertEquals("localhost", cfg21.m_internalInterface);

        //with override
        String args22[] = {"create", "internalinterface", "xxxxxx", "internalport", "localhost:7777"};
        VoltConfiguration cfg22 = new VoltConfiguration(args22);
        assertEquals(7777, cfg22.m_internalPort);
        assertEquals("localhost", cfg22.m_internalInterface);

        // XXX don't test what happens if port is invalid, because the code
        // doesn't handle that
    }

    @Test
    public void testConfigurationValidate() throws Exception {
        VoltConfiguration config;

        // missing leader provided deployment - not okay.
        String[] argsya = {"create", "catalog", "qwerty", "deployment", "qwerty"};
        config = new VoltConfiguration(argsya);
        assertFalse(config.validate());

        // missing deployment (it's okay now that a default deployment is supported)
        String[] args3 = {"create", "host", "hola", "catalog", "teststring2"};
        config = new VoltConfiguration(args3);
        assertTrue(config.validate());

        // default deployment with default leader -- okay.
        config = new VoltConfiguration(new String[]{"create", "catalog", "catalog.jar"});
        assertTrue(config.validate());

        // empty leader -- tests could pass in empty leader to indicate bind to all interfaces on mac
        String[] argsyo = {"create", "host", "", "catalog", "sdfs", "deployment", "sdfsd"};
        config = new VoltConfiguration(argsyo);
        assertTrue(config.validate());

        // empty deployment
        String[] args6 = {"create", "host", "hola", "catalog", "teststring6", "deployment", ""};
        config = new VoltConfiguration(args6);
        assertFalse(config.validate());

        // replica with explicit create
        String[] args8 = {"host", "hola", "deployment", "teststring4", "catalog", "catalog.jar", "create"};
        config = new VoltConfiguration(args8);
        assertTrue(config.validate());

        // valid config
        String[] args10 = {"create", "leader", "localhost", "deployment", "te", "catalog", "catalog.jar"};
        config = new VoltConfiguration(args10);
        assertTrue(config.validate());

        // valid config
        String[] args100 = {"create", "host", "hola", "deployment", "teststring4", "catalog", "catalog.jar"};
        config = new VoltConfiguration(args100);
        assertTrue(config.validate());

        // valid rejoin config
        String[] args200 = {"rejoin", "host", "localhost"};
        config = new VoltConfiguration(args200);
        assertTrue(config.validate());

        // invalid rejoin config, missing rejoin host
        String[] args250 = {"rejoin"};
        config = new VoltConfiguration(args250);
        assertFalse(config.validate()); // false in both pro and community

        // rejoinhost should still work
        String[] args201 = {"rejoinhost", "localhost"};
        config = new VoltConfiguration(args201);
        assertTrue(config.validate());

        // valid rejoin config
        String[] args300 = {"live", "rejoin", "host", "localhost"};
        config = new VoltConfiguration(args300);
        assertTrue(config.validate());
        assertEquals(StartAction.LIVE_REJOIN, config.m_startAction);
    }

    AtomicReference<Throwable> serverException = new AtomicReference<>(null);

    final Thread.UncaughtExceptionHandler handleUncaught = new Thread.UncaughtExceptionHandler() {
        @Override
        public void uncaughtException(Thread t, Throwable e) {
            serverException.compareAndSet(null, e);
        }
    };

    @Test
    public void testHostCountValidations() throws Exception {
        final File path = tmp.newFolder();

        String [] init = {"initialize", "voltdbroot", path.getPath()};
        VoltConfiguration config = new VoltConfiguration(init);
        assertTrue(config.validate()); // false in both pro and community]

        ServerThread server = new ServerThread(config);
        server.setUncaughtExceptionHandler(handleUncaught);
        server.start();
        server.join();

        // invalid host count
        String [] args400 = {"probe", "voltdbroot", path.getPath(), "hostcount", "2", "mesh", "uno,", "due", ",","tre", ",quattro" };
        config = new VoltConfiguration(args400);
        assertFalse(config.validate()); // false in both pro and community

        String [] args401 = {"probe", "voltdbroot", path.getPath(), "hostcount", "-3" , "mesh", "uno,", "due", ",","tre", ",quattro"};
        config = new VoltConfiguration(args401);
        assertFalse(config.validate()); // false in both pro and community

        String [] args402 = {"probe", "voltdbroot", path.getPath(), "hostcount", "4" , "mesh", "uno,", "due", ",","tre", ",quattro"};
        config = new VoltConfiguration(args402);
        assertTrue(config.validate()); // false in both pro and community

        String [] args403 = {"probe", "voltdbroot", path.getPath(), "hostcount", "6" , "mesh", "uno,", "due", ",","tre", ",quattro"};
        config = new VoltConfiguration(args403);
        assertTrue(config.validate()); // false in both pro and community

        String [] args404 = {"probe", "voltdbroot", path.getPath(), "mesh", "uno,", "due", ",","tre", ",quattro"};
        config = new VoltConfiguration(args404);
        assertTrue(config.validate()); // false in both pro and community
        assertEquals(4, config.m_hostCount);
    }

    /**
     * ENG-7088: Validate that deployment file users that want to belong to roles which
     * don't yet exist don't render the deployment file invalid.
     */
    @Test
    public void testCompileDeploymentAddUserToNonExistentGroup() throws IOException {
        TPCCProjectBuilder project = new TPCCProjectBuilder();
        project.addDefaultSchema();
        project.addDefaultPartitioning();
        project.addDefaultProcedures();

        project.setSecurityEnabled(true, true);
        RoleInfo groups[] = new RoleInfo[] {
                new RoleInfo("foo", false, false, false, false, false, false),
                new RoleInfo("blah", false, false, false, false, false, false)
        };
        project.addRoles(groups);
        UserInfo users[] = new UserInfo[] {
                new UserInfo("john", "hugg", new String[] {"foo"}),
                new UserInfo("ryan", "betts", new String[] {"foo", "bar"}),
                new UserInfo("ariel", "weisberg", new String[] {"bar"})
        };
        project.addUsers(users);

        String testDir = BuildDirectoryUtils.getBuildDirectoryPath();
        String jarName = "compile-deployment.jar";
        String catalogJar = testDir + File.separator + jarName;
        assertTrue("Project failed to compile", project.compile(catalogJar));

        byte[] bytes = MiscUtils.fileToBytes(new File(catalogJar));
        String serializedCatalog = CatalogUtil.getSerializedCatalogStringFromJar(CatalogUtil.loadAndUpgradeCatalogFromJar(bytes, false).getFirst());
        assertNotNull("Error loading catalog from jar", serializedCatalog);

        Catalog catalog = new Catalog();
        catalog.execute(serializedCatalog);

        // this should succeed even though group "bar" does not exist
        assertTrue("Deployment file should have been able to validate",
                CatalogUtil.compileDeployment(catalog, project.getPathToDeployment(), true) == null);
    }
}
