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

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.file.Files;
import java.util.EnumSet;
import java.util.Map.Entry;
import java.util.concurrent.atomic.AtomicReference;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.voltdb.catalog.Catalog;
import org.voltdb.compiler.VoltCompiler;
import org.voltdb.compiler.VoltProjectBuilder;
import org.voltdb.exceptions.SimulatedExitException;
import org.voltdb.utils.CatalogUtil;
import org.voltdb.utils.InMemoryJarfile;
import org.voltdb.utils.Poisoner;
import org.voltdb.utils.VoltFile;

import com.google_voltpatches.common.base.Joiner;
import com.google_voltpatches.common.io.CharStreams;
import com.google_voltpatches.common.reflect.ClassPath;
import com.google_voltpatches.common.reflect.ClassPath.ClassInfo;

/** Tests starting the server with init + start without a schema,
 * and 'init --schema --classes'.
 * Starting after 'init --schema' is covered in "TestStartWithSchema" and (in Pro) "TestStartWithSchemaAndDurability".
 */
final public class TestInitStartAction {

    static File rootDH;
    static File cmdlogDH;

    private static final String[] deploymentXML = {
            "<?xml version=\"1.0\"?>",
            "<deployment>",
            "    <cluster hostcount=\"1\"/>",
            "    <paths>",
            "        <voltdbroot path=\"_VOLTDBROOT_PATH_\"/>",
            "        <commandlog path=\"_COMMANDLOG_PATH_\"/>",
            "    </paths>",
            "    <httpd enabled=\"true\">",
            "        <jsonapi enabled=\"true\"/>",
            "    </httpd>",
            "    <commandlog enabled=\"false\"/>",
            "</deployment>"
        };

    static final Pattern voltdbrootRE = Pattern.compile("_VOLTDBROOT_PATH_");
    static final Pattern commandlogRE = Pattern.compile("_COMMANDLOG_PATH_");
    static File legacyDeploymentFH;

    @ClassRule
    static public final TemporaryFolder tmp = new TemporaryFolder();

    @BeforeClass
    public static void setupClass() throws Exception {
        rootDH = tmp.newFolder();
        cmdlogDH = new File(tmp.newFolder(), "commandlog");

        legacyDeploymentFH = new File(rootDH, "deployment.xml");
        try (FileWriter fw = new FileWriter(legacyDeploymentFH)) {
            Matcher mtc = voltdbrootRE.matcher(Joiner.on('\n').join(deploymentXML));
            String expnd = mtc.replaceAll(new File(rootDH, "voltdbroot").getPath());

            mtc = commandlogRE.matcher(expnd);
            expnd = mtc.replaceAll(cmdlogDH.getPath());

            fw.write(expnd);
        }
        System.setProperty("VOLT_JUSTATEST", "YESYESYES");
        Poisoner.ignoreCrash = true;
    }

    AtomicReference<Throwable> serverException = new AtomicReference<>(null);

    final Thread.UncaughtExceptionHandler handleUncaught = new Thread.UncaughtExceptionHandler() {
        @Override
        public void uncaughtException(Thread t, Throwable e) {
            serverException.compareAndSet(null, e);
        }
    };

    /** Verifies that the VoltDB exit 'crash' was a simulated exit with the specified exit code.
     * @param exitCode Expected exit code from VoltDB
     */
    private void expectSimulatedExit(int exitCode){
        assertNotNull(serverException.get());
        if (!(serverException.get() instanceof SimulatedExitException)) {
            System.err.println("got an unexpected exception");
            serverException.get().printStackTrace(System.err);
            if (Poisoner.wasCrashCalled) {
                System.err.println("Crash message is:\n  "+ Poisoner.crashMessage);
            }
        }

        assertTrue(serverException.get() instanceof SimulatedExitException);
        SimulatedExitException exitex = (SimulatedExitException)serverException.get();
        assertEquals(exitCode, exitex.getStatus());
    }

    /** Clears recorded crash (or simulated exit) in preparation for another test.
     */
    private void clearCrash(){
        Poisoner.wasCrashCalled = false;
        Poisoner.crashMessage = null;
        serverException.set(null);
    }

    /** Tests starting an empty database with the NewCLI commands "init" and "start",
     * plus a few error cases.
     */
    @Test
    public void testInitStartAction() throws Exception {

        File deplFH = new VoltFile(new VoltFile(new VoltFile(rootDH, "voltdbroot"), "config"), "deployment.xml");
        VoltConfiguration c1 = new VoltConfiguration(
                new String[]{"initialize", "voltdbroot", rootDH.getPath(), "force", "deployment", legacyDeploymentFH.getPath()});
        ServerThread server = new ServerThread(c1);
        server.setUncaughtExceptionHandler(handleUncaught);

        server.start();
        server.join();
        expectSimulatedExit(0);

        assertTrue(deplFH.exists() && deplFH.isFile() && deplFH.canRead());

        if (c1.m_isEnterprise) {
            assertTrue(cmdlogDH.exists()
                    && cmdlogDH.isDirectory()
                    && cmdlogDH.canRead()
                    && cmdlogDH.canWrite()
                    && cmdlogDH.canExecute());

            for (int i=0; i<10; ++i) {
                new FileOutputStream(new File(cmdlogDH, String.format("dummy-%02d.log", i))).close();
            }
            assertEquals(10, cmdlogDH.list().length);
        }

        serverException.set(null);
        // server thread sets m_forceVoltdbCreate to true by default
        c1 = new VoltConfiguration(
                new String[]{"initialize", "voltdbroot", rootDH.getPath(), "force", "deployment", legacyDeploymentFH.getPath()});
        assertTrue(c1.m_forceVoltdbCreate);
        server = new ServerThread(c1);
        server.setUncaughtExceptionHandler(handleUncaught);

        server.start();
        server.join();

        expectSimulatedExit(0);

        assertTrue(deplFH.exists() && deplFH.isFile() && deplFH.canRead());
        if (c1.m_isEnterprise) {
            assertTrue(cmdlogDH.exists()
                    && cmdlogDH.isDirectory()
                    && cmdlogDH.canRead()
                    && cmdlogDH.canWrite()
                    && cmdlogDH.canExecute());
            assertEquals(0, cmdlogDH.list().length);
        }

        try {
            c1 = new VoltConfiguration(new String[]{"initialize", "voltdbroot", rootDH.getPath()});
            fail("did not detect prexisting initialization");
        } catch (SimulatedExitException e) {
            assertEquals(-1, e.getStatus());
        }

        Poisoner.wasCrashCalled = false;
        Poisoner.crashMessage = null;
        serverException.set(null);

        c1 = new VoltConfiguration(new String[]{"create", "deployment", legacyDeploymentFH.getPath(), "host", "localhost"});
        server = new ServerThread(c1);
        server.setUncaughtExceptionHandler(handleUncaught);

        server.start();
        server.join();

        assertNotNull(serverException.get());
        assertTrue(serverException.get() instanceof AssertionError);
        assertTrue(Poisoner.wasCrashCalled);
        assertTrue(Poisoner.crashMessage.contains("Cannot use legacy start action"));

        if (!c1.m_isEnterprise) {
            return;
        }

        clearCrash();

        c1 = new VoltConfiguration(new String[]{"recover", "deployment", legacyDeploymentFH.getPath(), "host", "localhost"});
        server = new ServerThread(c1);
        server.setUncaughtExceptionHandler(handleUncaught);

        server.start();
        server.join();

        assertNotNull(serverException.get());
        assertTrue(serverException.get() instanceof AssertionError);
        assertTrue(Poisoner.wasCrashCalled);
        assertTrue(Poisoner.crashMessage.contains("Cannot use legacy start action"));

        // this test which action should be considered legacy
        EnumSet<StartAction> legacyOnes = EnumSet.complementOf(EnumSet.of(StartAction.INITIALIZE,StartAction.PROBE, StartAction.GET));
        assertTrue(legacyOnes.stream().allMatch(StartAction::isLegacy));
    }

    /*
     * "voltdb init --schema --procedures" tests:
     * 1.  Positive test with valid schema that requires no procedures
     * 2a. Positive test with valid schema and procedures that are in CLASSPATH
     * 2b. Negative test with valid files but not "init --force"
     * 3.  Negative test with a bad schema
     * 4.  Negative test with procedures missing
     *
     * Note that SimulatedExitException is thrown by the command line parser with no descriptive details.
     * PoisonPill.crashLocalVoltDB() throws an AssertionError with the message "Faux crash of VoltDB successful."
     */

    /** Verifies that the staged catalog matches what VoltCompiler emits given the supplied schema.
     * @param schema Schema used to generate the staged catalog
     * @throws Exception upon test failure or error (unable to write temp file for example)
     */
    private void validateStagedCatalog(String schema, InMemoryJarfile proceduresJar) throws Exception {
        File schemaFile = null;
        if (schema != null) {
            // setup reference point for the supplied schema
            schemaFile = VoltProjectBuilder.writeStringToTempFile(schema);
            schemaFile.deleteOnExit();
        }
        File referenceFile = File.createTempFile("reference", ".jar");
        referenceFile.deleteOnExit();
        VoltCompiler compiler = new VoltCompiler(false);
        compiler.setInitializeDDLWithFiltering(true);
        boolean success;
        if (schema == null) {
            success = compiler.compileEmptyCatalog(referenceFile.getAbsolutePath());
        } else {
            success = compiler.compileFromDDL(referenceFile.getAbsolutePath(), schemaFile.getPath());
        }
        assertEquals(true, success);
        InMemoryJarfile referenceCatalogJar = new InMemoryJarfile(referenceFile);
        Catalog referenceCatalog = new Catalog();
        referenceCatalog.execute(CatalogUtil.getSerializedCatalogStringFromJar(referenceCatalogJar));

        // verify that the staged catalog is identical
        File stagedJarFile = new VoltFile(RealVoltDB.getStagedCatalogPath(rootDH.getPath() + File.separator + "voltdbroot"));
        assertEquals(true, stagedJarFile.isFile());
        InMemoryJarfile stagedCatalogJar = new InMemoryJarfile(stagedJarFile);
        Catalog stagedCatalog = new Catalog();
        stagedCatalog.execute(CatalogUtil.getSerializedCatalogStringFromJar(stagedCatalogJar));

        assertEquals(true, referenceCatalog.equals(stagedCatalog));
        assertEquals(true, stagedCatalog.equals(referenceCatalog));

        assertEquals(true, referenceFile.delete());
        // If schema is not null we have a real file else we have a dummy reader.
        if (schema != null) {
            assertEquals(true, schemaFile.delete());
        }

        if (proceduresJar != null) {
            // Validate that the list of files in the supplied jarfile are present in the staged catalog also.
            InMemoryJarfile strippedReferenceJar = CatalogUtil.getCatalogJarWithoutDefaultArtifacts(proceduresJar);
            InMemoryJarfile strippedTestJar = CatalogUtil.getCatalogJarWithoutDefaultArtifacts(stagedCatalogJar);
            for (Entry<String, byte[]> entry : strippedReferenceJar.entrySet()) {
                System.out.println("Checking " + entry.getKey());
                byte[] testClass = strippedTestJar.get(entry.getKey());
                assertNotNull(entry.getKey() + " was not found in staged catalog", testClass);
                assertArrayEquals(entry.getValue(), testClass);
            }
        }
    }

    /** Test that a valid schema with no procedures can be used to stage a matching catalog.
     * @throws Exception upon failure or error
     */
    @Test
    public void testInitWithSchemaValidNoProcedures() throws Exception {

        final String schema =
                "create table books (cash integer default 23 not null, title varchar(3) default 'foo', PRIMARY KEY(cash));\n" +
                "-- here is a comment\n" +
                "create procedure Foo as select * from books;\n" +
                "-- here is another comment\n" +
                "PARTITION TABLE -- hmmm\n" +
                "-- VoltDB-specific mid statement comment\n" +
                "books ON COLUMN cash; -- end of line comment\n" +
                "-- comment comment comment\n" +
                "CREATE STREAM mystream (\n" +
                "  -- a column:\n" +
                "  eventid BIGINT NOT NULL,\n" +
                "  data VARBINARY(64)\n" +
                "); -- end of line comment\n" +
                "-- the end.\n";
        File schemaFile = VoltProjectBuilder.writeStringToTempFile(schema);

        VoltConfiguration c1 = new VoltConfiguration(
                new String[]{"initialize", "voltdbroot", rootDH.getPath(), "force", "schema", schemaFile.getPath()});
        ServerThread server = new ServerThread(c1);
        server.setUncaughtExceptionHandler(handleUncaught);
        server.start();
        server.join();
        expectSimulatedExit(0);
        validateStagedCatalog(schema, null);
        assertEquals(true, schemaFile.delete());
    }

    /** Test that a valid schema with procedures can be used to stage a matching catalog,
     * but running a second time without 'init --force' fails due to existing artifacts.
     * @throws Exception upon failure or error
     */
    @Test
    public void testInitWithSchemaValidWithProcedures() throws Exception {

        String schema =
                "create table books" +
                " (cash integer default 23 not null," +
                " title varchar(3) default 'foo'," +
                " PRIMARY KEY(cash));" +
                "PARTITION TABLE books ON COLUMN cash;" +
                "CREATE PROCEDURE partition on table books column cash FROM CLASS org.voltdb.compiler.procedures.AddBook;";
        File schemaFile = VoltProjectBuilder.writeStringToTempFile(schema);
        {
            // valid use case
            VoltConfiguration c1 = new VoltConfiguration(
                    new String[]{"initialize", "force", "voltdbroot", rootDH.getPath(), "schema", schemaFile.getPath()});
            ServerThread server = new ServerThread(c1);
            server.setUncaughtExceptionHandler(handleUncaught);
            server.start();
            server.join();
            expectSimulatedExit(0);
            validateStagedCatalog(schema, null);
            clearCrash();
        }
        try {
            // second attempt is not valid due to existing artifacts
            new VoltConfiguration(
                    new String[]{"initialize", "voltdbroot", rootDH.getPath(), "schema", schemaFile.getPath()});
        } catch (SimulatedExitException e){
            assertEquals(e.getStatus(), -1);
        }
        assertEquals(true, schemaFile.delete());
    }

    /** Test that a valid schema with no procedures can be used to stage a matching catalog.
     * @throws Exception upon failure or error
     */
    @Test
    public void testInitWithSchemaInvalidJunkSchema() throws Exception {

        File schemaFile = Files.createTempDirectory("junk").toFile();
        try {
            new VoltConfiguration(
                    new String[]{"initialize", "voltdbroot", rootDH.getPath(), "force", "schema", schemaFile.getPath()});
            fail("did not detect unusable schema file");
        } catch (SimulatedExitException e) {
            assertEquals(e.getStatus(), -1);
        }

        assertEquals(true, schemaFile.delete());
    }

    /** Test that init accepts classes without a schema.
     * @throws Exception upon failure or error
     */
    @Test
    public void testInitWithNoSchema() throws Exception {
        try {
            new VoltConfiguration(
                    new String[]{"initialize", "voltdbroot", rootDH.getPath(), "force"});
        } catch (SimulatedExitException e) {
            fail("Should init without schema.");
        }
    }

    /** Test that a valid schema with no procedures can be used to stage a matching catalog.
     * @throws Exception upon failure or error
     */
    @Test
    public void testInitWithSchemaInvalidMissingClass() throws Exception {

        String schema =
                "CREATE TABLE unicorns" +
                " (horn_size integer DEFAULT 12 NOT NULL," +
                " name varchar(32) DEFAULT 'Pixie' NOT NULL," +
                " PRIMARY KEY(name));" +
                "PARTITION TABLE unicorns ON COLUMN name;" +
                "CREATE PROCEDURE FROM CLASS org.voltdb.unicorns.ComputeSocialStanding;";
        File schemaFile = VoltProjectBuilder.writeStringToTempFile(schema);

        VoltConfiguration c1 = new VoltConfiguration(
                    new String[]{"initialize", "voltdbroot", rootDH.getPath(), "force", "schema", schemaFile.getPath()});
        ServerThread server = new ServerThread(c1);
        server.setUncaughtExceptionHandler(handleUncaught);
        server.start();
        server.join();

        assertNotNull(serverException.get());
        assertTrue(serverException.get().getMessage().equals("Faux crash of VoltDB successful."));
        assertTrue(Poisoner.wasCrashCalled);
        assertTrue(Poisoner.crashMessage.contains("Could not compile specified schema"));
        assertEquals(true, schemaFile.delete());
    }

    /** Tests that when there are base classes and non-class files in the stored procedures,
     * that these also exist in the staged catalog.
     * @throws Exception upon failure or error
     */
    @Test
    public void testInitWithClassesAndArtifacts() throws Exception {

        System.out.println("Loading the schema from testprocs");
        File resource = new File("tests/testprocs/org/voltdb_testprocs/fakeusecase/greetings/ddl.sql");
        InputStream schemaReader = new FileInputStream(resource);
        assertNotNull("Could not find " + resource, schemaReader);
        String schema = CharStreams.toString(new InputStreamReader(schemaReader));

        System.out.println("Creating a .jar file using all of the classes associated with this test.");
        InMemoryJarfile originalInMemoryJar = new InMemoryJarfile();
        VoltCompiler compiler = new VoltCompiler(false, false);
        ClassPath classpath = ClassPath.from(this.getClass().getClassLoader());
        String packageName = "org.voltdb_testprocs.fakeusecase.greetings";
        int classesFound = 0;
        for (ClassInfo myclass : classpath.getTopLevelClassesRecursive(packageName)) {
            compiler.addClassToJar(originalInMemoryJar, myclass.load());
            classesFound++;
        }
        // check that classes were found and loaded. If another test modifies "fakeusecase.greetings" it should modify this assert also.
        assertEquals(5, classesFound);

        System.out.println("Writing " + classesFound + " classes to jar file");
        File classesJarfile = File.createTempFile("TestInitStartWithClasses-procedures", ".jar");
        classesJarfile.deleteOnExit();
        originalInMemoryJar.writeToFile(classesJarfile);

        VoltConfiguration c1 = new VoltConfiguration(
                    new String[]{"initialize", "voltdbroot", rootDH.getPath(), "force", "schema", resource.getPath(), "classes", classesJarfile.getPath()});
        ServerThread server = new ServerThread(c1);
        server.setUncaughtExceptionHandler(handleUncaught);
        server.start();
        server.join();

        validateStagedCatalog(schema, originalInMemoryJar);
    }

    /** Tests that when there are base classes and non-class files in the stored procedures,
     * that these also exist in the staged catalog.
     * @throws Exception upon failure or error
     */
    @Test
    public void testInitWithClassesAndNoSchema() throws Exception {

        System.out.println("Creating a .jar file using all of the classes associated with this test.");
        InMemoryJarfile originalInMemoryJar = new InMemoryJarfile();
        VoltCompiler compiler = new VoltCompiler(false, false);
        ClassPath classpath = ClassPath.from(this.getClass().getClassLoader());
        String packageName = "org.voltdb_testprocs.fakeusecase.greetings";
        int classesFound = 0;
        for (ClassInfo myclass : classpath.getTopLevelClassesRecursive(packageName)) {
            compiler.addClassToJar(originalInMemoryJar, myclass.load());
            classesFound++;
        }
        // check that classes were found and loaded. If another test modifies "fakeusecase.greetings" it should modify this assert also.
        assertEquals(5, classesFound);

        System.out.println("Writing " + classesFound + " classes to jar file");
        File classesJarfile = File.createTempFile("TestInitStartWithClassesNoSchema-procedures", ".jar");
        classesJarfile.deleteOnExit();
        originalInMemoryJar.writeToFile(classesJarfile);

        VoltConfiguration c1 = new VoltConfiguration(
                    new String[]{"initialize", "voltdbroot", rootDH.getPath(), "force", "classes", classesJarfile.getPath()});
        ServerThread server = new ServerThread(c1);
        server.setUncaughtExceptionHandler(handleUncaught);
        server.start();
        server.join();

        //Only validate jar
        validateStagedCatalog(null, originalInMemoryJar);
    }

    /* For 'voltdb start' test coverage see TestStartWithSchema and (in Pro) TestStartWithSchemaAndDurability.
     */
}
