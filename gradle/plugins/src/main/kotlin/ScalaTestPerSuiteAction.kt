/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import org.gradle.api.Action
import org.gradle.api.GradleException
import org.gradle.api.tasks.testing.Test
import org.gradle.api.tasks.util.PatternSet
import java.io.DataInputStream
import java.io.File
import java.lang.reflect.Modifier
import java.util.concurrent.Executors
import java.util.concurrent.Future
import java.util.concurrent.atomic.AtomicInteger

/**
 * Custom [Action] for Gradle's [Test] task that replaces the gradle-scalatest plugin's
 * default ScalaTestAction. Instead of running all suites in a single JVM via
 * `Runner -R <dir>`, this action invokes `org.scalatest.tools.Runner -s <Suite>`
 * in a separate `javaexec` for each test suite class.
 *
 * This achieves per-suite JVM isolation without depending on JUnit 5 Platform,
 * preventing Spark's JVM-global state (SparkContext, daemon threads) from leaking
 * between suites. Suites run in parallel up to [Test.getMaxParallelForks] concurrent
 * JVMs.
 *
 * Tag filtering is read from the gradle-scalatest plugin's `tags` extension
 * (a [PatternSet] registered on each Test task). `tags.includes` map to ScalaTest
 * Runner's `-n` flags, `tags.excludes` map to `-l` flags. Exclude tags are also
 * used for class-level pre-filtering via constant pool scanning to avoid spawning
 * JVMs for excluded suites.
 *
 * Test filter patterns from `--tests` (Gradle's `filter.includePatterns` and
 * `filter.commandLineIncludePatterns`) are translated to ScalaTest's `-q` (suite
 * name match) or `-z` (substring match) flags, consistent with the plugin's behavior.
 */
class ScalaTestPerSuiteAction : Action<Test> {

    override fun execute(task: Test) {
        // Read tags from the plugin's extension
        val tags = task.extensions.findByName("tags") as? PatternSet
        val tagsToInclude = tags?.includes ?: emptySet()
        val tagsToExclude = tags?.excludes ?: emptySet()

        val suites = discoverSuites(task, tagsToExclude)
        if (suites.isEmpty()) {
            task.logger.lifecycle("No test suites found in ${task.path}")
            return
        }

        task.logger.lifecycle("Discovered ${suites.size} test suite(s) in ${task.path}")

        val reportsDir = File(task.reports.junitXml.outputLocation.asFile.get().absolutePath)
        reportsDir.mkdirs()

        if (task.reports.html.required.get()) {
            val htmlReportsDir = File(task.reports.html.outputLocation.asFile.get().absolutePath)
            htmlReportsDir.mkdirs()
        }

        // Collect test filter patterns from --tests flag
        val filterPatterns = collectFilterPatterns(task)

        val parallelism = task.maxParallelForks.coerceAtLeast(1)
        val failedSuites = java.util.Collections.synchronizedList(mutableListOf<String>())
        val passedCount = AtomicInteger(0)

        if (parallelism == 1) {
            // Sequential execution
            for (suite in suites) {
                runSuite(task, suite, reportsDir, tagsToInclude, tagsToExclude, filterPatterns,
                    failedSuites, passedCount)
            }
        } else {
            // Parallel execution with bounded thread pool
            task.logger.lifecycle("Running suites with parallelism=$parallelism")
            val executor = Executors.newFixedThreadPool(parallelism)
            try {
                val futures = suites.map { suite ->
                    executor.submit {
                        runSuite(task, suite, reportsDir, tagsToInclude, tagsToExclude,
                            filterPatterns, failedSuites, passedCount)
                    }
                }
                futures.forEach { it.get() }
            } finally {
                executor.shutdown()
            }
        }

        task.logger.lifecycle(
            "\nScalaTest results: ${passedCount.get()} passed, ${failedSuites.size} failed, " +
                "${suites.size} total"
        )

        if (failedSuites.isNotEmpty()) {
            val failureMessage = "ScalaTest failures in ${task.path}:\n" +
                failedSuites.joinToString("\n") { "  - $it" }
            if (!task.ignoreFailures) {
                throw GradleException(failureMessage)
            } else {
                task.logger.warn(failureMessage)
            }
        }
    }

    companion object {
        /** Matches class names that look like test suites (same heuristic as the plugin). */
        private val MAYBE_TEST = Regex("Spec|Test|Suite")
    }

    private fun runSuite(
        task: Test,
        suite: String,
        reportsDir: File,
        tagsToInclude: Set<String>,
        tagsToExclude: Set<String>,
        filterPatterns: List<String>,
        failedSuites: MutableList<String>,
        passedCount: AtomicInteger
    ) {
        task.logger.lifecycle("Running suite: $suite")

        val runnerArgs = buildRunnerArgs(
            task, suite, reportsDir, tagsToInclude, tagsToExclude, filterPatterns
        )

        try {
            val result = task.project.javaexec {
                mainClass.set("org.scalatest.tools.Runner")
                classpath = task.classpath
                args = runnerArgs
                jvmArgs = task.allJvmArgs
                isIgnoreExitValue = true
            }

            if (result.exitValue != 0) {
                failedSuites.add(suite)
                task.logger.error("FAILED: $suite (exit code ${result.exitValue})")
            } else {
                passedCount.incrementAndGet()
                task.logger.lifecycle("PASSED: $suite")
            }
        } catch (e: Exception) {
            failedSuites.add(suite)
            task.logger.error("ERROR: $suite - ${e.message}")
        }
    }

    /**
     * Collects test filter patterns from Gradle's `--tests` flag.
     * Both `commandLineIncludePatterns` and `includePatterns` are checked,
     * matching the plugin's `ScalaTestAction.getArgs()` behavior.
     */
    private fun collectFilterPatterns(task: Test): List<String> {
        val patterns = mutableListOf<String>()
        try {
            // commandLineIncludePatterns is available in newer Gradle versions
            val cmdPatterns = task.filter.javaClass
                .getMethod("getCommandLineIncludePatterns")
                .invoke(task.filter) as? Set<*>
            cmdPatterns?.filterIsInstance<String>()?.let { patterns.addAll(it) }
        } catch (_: Exception) {
            // Method not available in this Gradle version
        }
        patterns.addAll(task.filter.includePatterns)
        return patterns
    }

    private fun buildRunnerArgs(
        task: Test,
        suite: String,
        reportsDir: File,
        tagsToInclude: Set<String>,
        tagsToExclude: Set<String>,
        filterPatterns: List<String>
    ): List<String> {
        val args = mutableListOf<String>()

        // Single suite mode
        args.add("-s")
        args.add(suite)

        // Output with durations
        args.add("-oD")

        // JUnit XML reporter for CI integration
        if (task.reports.junitXml.required.get()) {
            args.add("-u")
            args.add(reportsDir.absolutePath)
        }

        // HTML reporter
        if (task.reports.html.required.get()) {
            args.add("-h")
            args.add(task.reports.html.outputLocation.asFile.get().absolutePath)
        }

        // Per-test tag filtering via ScalaTest Runner's native flags
        tagsToInclude.forEach { tag ->
            args.add("-n")
            args.add(tag)
        }
        tagsToExclude.forEach { tag ->
            args.add("-l")
            args.add(tag)
        }

        // --tests filter patterns: -q for suite names, -z for substrings
        // (same logic as ScalaTestAction.getArgs)
        filterPatterns.forEach { pattern ->
            if (MAYBE_TEST.containsMatchIn(pattern)) {
                args.add("-q")
            } else {
                args.add("-z")
            }
            args.add(pattern)
        }

        return args
    }

    /**
     * Discovers concrete ScalaTest suite classes from the test classes directories.
     * Excludes abstract classes, interfaces, Scala companion objects ($), inner classes,
     * and classes with excluded tag annotations.
     *
     * Note: Only exclusion is done at the class level (annotation descriptors are visible
     * in the constant pool). Inclusion filtering is deferred to ScalaTest Runner's `-n` flag
     * because per-test `Tag` objects (e.g., `test("name", MyTag)`) don't produce annotation
     * descriptors on the suite class — they're only visible at runtime.
     */
    private fun discoverSuites(task: Test, tagsToExclude: Set<String>): List<String> {
        val excludeDescriptors = tagsToExclude.map { "L${it.replace('.', '/')};" }.toSet()

        // Build a map of all class files for superclass chain resolution
        val classFileMap = mutableMapOf<String, File>()
        task.testClassesDirs.files.forEach { classesDir ->
            if (!classesDir.exists()) return@forEach
            classesDir.walkTopDown()
                .filter { it.isFile && it.name.endsWith(".class") }
                .forEach { classFile ->
                    val internalName = classFile.relativeTo(classesDir).path
                        .removeSuffix(".class")
                    classFileMap[internalName] = classFile
                }
        }

        val suites = mutableListOf<String>()

        classFileMap.forEach { (internalName, classFile) ->
            // Skip $ classes (companions, inner classes)
            if (internalName.substringAfterLast('/').contains("$")) return@forEach

            val info = readClassInfo(classFile) ?: return@forEach

            // Skip abstract classes and interfaces
            if (Modifier.isAbstract(info.accessFlags) || Modifier.isInterface(info.accessFlags)) {
                return@forEach
            }

            // Check if this class is a ScalaTest suite by walking the superclass chain
            if (!isScalaTestSuite(internalName, classFileMap)) return@forEach

            // Tag-based exclusion (class-level annotations only)
            if (excludeDescriptors.isNotEmpty() &&
                excludeDescriptors.any { it in info.utf8Entries }
            ) {
                return@forEach
            }

            val fqcn = internalName.replace('/', '.')
            suites.add(fqcn)
        }

        return suites.sorted()
    }

    private data class ClassInfo(
        val accessFlags: Int,
        val superClass: String?,
        val utf8Entries: Set<String>
    )

    private val classInfoCache = mutableMapOf<String, ClassInfo?>()

    /**
     * Reads class file metadata: access flags, superclass name, and constant pool UTF-8 entries.
     */
    private fun readClassInfo(classFile: File): ClassInfo? {
        return try {
            DataInputStream(classFile.inputStream()).use { din ->
                din.skipBytes(8) // magic(4) + version(4)

                // Read constant pool
                val utf8Entries = mutableSetOf<String>()
                val classRefs = mutableMapOf<Int, Int>() // class index -> name index
                val utf8ByIndex = mutableMapOf<Int, String>()
                val cpCount = din.readUnsignedShort()
                var i = 1
                while (i < cpCount) {
                    when (din.readUnsignedByte()) {
                        1 -> { // Utf8
                            val len = din.readUnsignedShort()
                            val bytes = ByteArray(len)
                            din.readFully(bytes)
                            val str = String(bytes)
                            utf8Entries.add(str)
                            utf8ByIndex[i] = str
                        }
                        3, 4 -> din.skipBytes(4)    // Integer, Float
                        5, 6 -> { din.skipBytes(8); i++ } // Long, Double (2 pool slots)
                        7 -> { // Class ref
                            val nameIndex = din.readUnsignedShort()
                            classRefs[i] = nameIndex
                        }
                        8, 16, 19, 20 -> din.skipBytes(2)
                        9, 10, 11, 12, 17, 18 -> din.skipBytes(4)
                        15 -> din.skipBytes(3)      // MethodHandle
                    }
                    i++
                }

                val accessFlags = din.readUnsignedShort()
                din.readUnsignedShort() // this_class
                val superClassIndex = din.readUnsignedShort()
                val superClass = if (superClassIndex != 0) {
                    val nameIndex = classRefs[superClassIndex]
                    if (nameIndex != null) utf8ByIndex[nameIndex] else null
                } else null

                ClassInfo(accessFlags, superClass, utf8Entries)
            }
        } catch (_: Exception) {
            null
        }
    }

    /**
     * Checks whether a class is a ScalaTest suite by walking the superclass chain.
     * A class is a ScalaTest suite if it (or any of its superclasses in the test classes
     * directories) references `org/scalatest/` in its constant pool.
     */
    private fun isScalaTestSuite(internalName: String, classFileMap: Map<String, File>): Boolean {
        val visited = mutableSetOf<String>()
        var current: String? = internalName
        while (current != null && current !in visited) {
            visited.add(current)

            val classFile = classFileMap[current] ?: return false
            val info = classInfoCache.getOrPut(current) { readClassInfo(classFile) } ?: return false

            // Check if any constant pool entry references ScalaTest
            if (info.utf8Entries.any { it.startsWith("org/scalatest/") }) {
                return true
            }

            current = info.superClass
        }
        return false
    }
}
