/*
* Copyright 2022 the original author or authors.
*
* Licensed under the Apache License, Version 2.0 (the "License");
* you may not use this file except in compliance with the License.
* You may obtain a copy of the License at
*
*    http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*/
package com.morpheusdata.azure

import com.morpheusdata.azure.datasets.DatastoreDatasetProvider
import com.morpheusdata.azure.datasets.VaultDatasetProvider
import com.morpheusdata.core.MorpheusContext
import com.morpheusdata.core.Plugin

class AzureBackupPlugin extends Plugin {

    @Override
    String getCode() {
        return 'azure'
    }

    @Override
    void initialize() {
        this.setName("Azure")
        this.registerProvider(new AzureBackupProvider(this,this.morpheus))
        this.registerProvider(new VaultDatasetProvider(this,this.morpheus))
        this.registerProvider(new DatastoreDatasetProvider(this,this.morpheus))
    }

    /**
     * Called when a plugin is being removed from the plugin manager (aka Uninstalled)
     */
    @Override
    void onDestroy() {
        //nothing to do for now
    }

    MorpheusContext getMorpheusContext() {
        this.morpheus
    }
}
