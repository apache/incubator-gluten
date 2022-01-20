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

package com.intel.oap.row;

import io.kyligence.jni.engine.LocalEngine;
import org.apache.commons.lang3.StringUtils;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

/**
 * Java API for in-process profiling. Serves as a wrapper around
 * async-profiler native library. This class is a singleton.
 * The first call to {@link #getInstance()} initiates loading of
 * libasyncProfiler.so.
 */
public class JniInstance {

    private static JniInstance instance;

    private String currLibPath = "";

    private JniInstance() {
    }

    public static JniInstance getInstance() {
        return getInstance(null);
    }

    public static synchronized JniInstance getInstance(String libPath) {
        if (instance != null) {
            return instance;
        }

        File file = null;
        boolean libPathExists = false;
        if (StringUtils.isNotBlank(libPath)) {
            file = new File(libPath);
            libPathExists = file.isFile() && file.exists();
        }
        if (!libPathExists) {
            String soFileName = "/liblocal_engine_jnid.so";
            try {
                InputStream is = JniInstance.class.getResourceAsStream(soFileName);
                file = File.createTempFile("lib", ".so");
                OutputStream os = new FileOutputStream(file);
                byte[] buffer = new byte[128 << 10];
                int length;
                while ((length = is.read(buffer)) != -1) {
                    os.write(buffer, 0, length);
                }
                is.close();
                os.close();
            } catch (IOException e) {
            }
        }
        if (file != null) {
            try {
                file.setReadable(true, false);
                System.load(file.getAbsolutePath());
                libPath = file.getAbsolutePath();
            } catch (UnsatisfiedLinkError error) {
                throw error;
            }
        }
        instance = new JniInstance();
        instance.setCurrLibPath(libPath);
        LocalEngine.initEngineEnv();
        return instance;
    }

    public void setCurrLibPath(String currLibPath) {
        this.currLibPath = currLibPath;
    }

    public LocalEngine buildLocalEngine(byte[] substraitPlan) {
        LocalEngine localEngine = new LocalEngine(substraitPlan);
        return localEngine;
    }
}
