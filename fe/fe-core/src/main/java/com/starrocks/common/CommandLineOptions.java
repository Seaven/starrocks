// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package com.starrocks.common;

import com.starrocks.journal.bdbje.BDBToolOptions;

public class CommandLineOptions {

    private boolean isVersion;
    private boolean enableFailPoint;
    private BDBToolOptions bdbToolOpts = null;
    private String helpers;
    private String hostType;
    private boolean startFromSnapshot;

    public void setVersion(boolean version) {
        isVersion = version;
    }

    public void setEnableFailPoint(boolean enableFailPoint) {
        this.enableFailPoint = enableFailPoint;
    }

    public void setBdbToolOpts(BDBToolOptions bdbToolOpts) {
        this.bdbToolOpts = bdbToolOpts;
    }

    public void setHelpers(String helpers) {
        this.helpers = helpers;
    }

    public void setHostType(String hostType) {
        this.hostType = hostType;
    }

    public void setStartFromSnapshot(boolean startFromSnapshot) {
        this.startFromSnapshot = startFromSnapshot;
    }

    public boolean isVersion() {
        return isVersion;
    }

    public BDBToolOptions getBdbToolOpts() {
        return bdbToolOpts;
    }

    public boolean isEnableFailPoint() {
        return enableFailPoint;
    }

    public String getHelpers() {
        return helpers;
    }

    public String getHostType() {
        return hostType;
    }

    public boolean isStartFromSnapshot() {
        return startFromSnapshot;
    }
}
