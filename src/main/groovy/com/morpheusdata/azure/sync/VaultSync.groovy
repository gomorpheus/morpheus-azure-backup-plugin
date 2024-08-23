package com.morpheusdata.azure.sync

import com.morpheusdata.azure.services.ApiService
import com.morpheusdata.azure.AzureBackupPlugin
import com.morpheusdata.core.MorpheusContext
import com.morpheusdata.core.data.DataFilter
import com.morpheusdata.core.data.DataQuery
import com.morpheusdata.core.util.SyncTask
import com.morpheusdata.model.BackupProvider
import com.morpheusdata.model.ReferenceData
import groovy.util.logging.Slf4j
import io.reactivex.rxjava3.core.Observable

@Slf4j
class VaultSync {
    private AzureBackupPlugin plugin
    private MorpheusContext morpheusContext
    private BackupProvider backupProviderModel
    private ApiService apiService


    public VaultSync(BackupProvider backupProviderModel, ApiService apiService, AzureBackupPlugin plugin) {
        this.backupProviderModel = backupProviderModel
        this.apiService = apiService
        this.plugin = plugin
        this.morpheusContext = plugin.morpheusContext
    }

    def execute() {
        try {
            log.debug("VaultSync execute")
            Map authConfig = apiService.getAuthConfig(backupProviderModel)

            def cloudId = backupProviderModel.getConfigProperty('cloudId')
            def poolCategory = "azure.resourcepool.${cloudId}"
            def resourceGroups = morpheusContext.services.cloud.pool.listIdentityProjections(
                    new DataQuery().withFilters(
                            new DataFilter<String>("refType", 'ComputeZone'),
                            new DataFilter<Long>("refId", cloudId as Long),
                            new DataFilter<String>("category", poolCategory),
                            new DataFilter<String>("type", 'resourceGroup'),
                    )
            )

            resourceGroups.each { resourceGroup ->
                def listResults = apiService.listVaults(authConfig, [resourceGroup: resourceGroup.externalId])
                if(listResults.success) {
                    def cloudItems = listResults.results.value
                    def objCategory = "azure.backup.vault.${backupProviderModel.id}"
                    Observable<ReferenceData> existingItems = morpheusContext.async.referenceData.list(new DataQuery().withFilters([
                            new DataFilter('category', objCategory),
                            new DataFilter('account.id', backupProviderModel.account.id),
                            new DataFilter('refType', 'ComputeZonePool'),
                            new DataFilter('refId', resourceGroup.id),
                    ]))

                    SyncTask<ReferenceData, ArrayList<Map>, ReferenceData> syncTask = new SyncTask<>(existingItems, cloudItems)
                    syncTask.addMatchFunction { ReferenceData domainObject, Map cloudItem ->
                        domainObject.externalId == cloudItem.id
                    }.onDelete { List<ReferenceData> removeItems ->
                        deleteVault(removeItems)
                    }.onUpdate { List<SyncTask.UpdateItem<ReferenceData, Map>> updateItems ->
                        updateMatchedVaults(updateItems)
                    }.onAdd { itemsToAdd ->
                        addMissingVaults(itemsToAdd, objCategory, resourceGroup)
                    }.withLoadObjectDetailsFromFinder { List<SyncTask.UpdateItemDto<ReferenceData, Map>> updateItems ->
                        return morpheusContext.async.referenceData.list( new DataQuery().withFilters([
                                new DataFilter('id', 'in', updateItems.collect { it.existingItem.id } as List<Long>)
                        ]))
                    }.start()
                } else {
                    log.error("Error listing backup vaults for resource group ${resourceGroup.name}")
                }
            }
        } catch (Exception ex) {
            log.error("VaultSync error: {}", ex, ex)
        }
    }

    private addMissingVaults(itemsToAdd, objCategory, resourceGroup) {
        log.debug "addMissingVaults: ${itemsToAdd}"

        def adds = []
        for(cloudItem in itemsToAdd) {
            def addConfig = [account:backupProviderModel.account, code:objCategory + '.' + cloudItem.name, category:objCategory,
                             name:cloudItem.name, keyValue:cloudItem.name, value:cloudItem.name, externalId:cloudItem.id, type: 'string',
                             refType: 'ComputeZonePool', refId: resourceGroup.id]

            def add = new ReferenceData(addConfig)
            add.setConfigMap(cloudItem)
            adds << add
        }

        if(adds) {
            log.debug "adding backup servers: ${adds}"
            def success =  morpheusContext.async.referenceData.bulkCreate(adds).blockingGet()
            if(!success) {
                log.error "Error adding backup servers"
            }
        }
    }

    private deleteVault(List<ReferenceData> removeItems) {
        log.debug "deleteVault: ${removeItems}"
        morpheusContext.async.referenceData.bulkRemove(removeItems).blockingGet()
    }

    private updateMatchedVaults(List<SyncTask.UpdateItem<ReferenceData, Map>> updateItems) {
        log.debug "updateMatchedVaults: ${updateItems.size()}"
        def saveList = []
        try {
            for (SyncTask.UpdateItem<ReferenceData, Map> update in updateItems) {
                Map masterItem = update.masterItem
                ReferenceData existingItem = update.existingItem

                def doSave = false
                if (existingItem.name != masterItem.name) {
                    existingItem.name = masterItem.name
                    doSave = true
                }
                if (doSave == true) {
                    saveList << existingItem
                }
            }
            if (saveList) {
                morpheusContext.async.referenceData.bulkSave(saveList).blockingGet()
            }
        } catch (e) {
            log.error("updateMatchedVaults error: ${e}", e)
        }
    }
}
