package com.morpheusdata.azure.datasets

import com.morpheusdata.azure.AzureBackupPlugin
import com.morpheusdata.core.MorpheusContext
import com.morpheusdata.core.data.DataFilter
import com.morpheusdata.core.data.DataQuery
import com.morpheusdata.core.data.DatasetInfo
import com.morpheusdata.core.data.DatasetQuery
import com.morpheusdata.core.providers.AbstractDatasetProvider
import com.morpheusdata.model.Datastore
import groovy.util.logging.Slf4j
import io.reactivex.rxjava3.core.Observable

@Slf4j
class DatastoreDatasetProvider extends AbstractDatasetProvider<Datastore, String> {
    public static final providerName = 'Azure Datastore Dataset Provider'
    public static final providerNamespace = 'azureBackup'
    public static final providerKey = 'azureBackupDatastores'
    public static final providerDescription = 'Get available datastores from Azure'

    DatastoreDatasetProvider(AzureBackupPlugin plugin, MorpheusContext morpheus) {
        this.plugin = plugin
        this.morpheusContext = morpheus
    }

    @Override
    DatasetInfo getInfo() {
        return new DatasetInfo(
                name: providerName,
                namespace: providerNamespace,
                key: providerKey,
                description: providerDescription
        )
    }

    @Override
    Class<Datastore> getItemType() {
        return Datastore.class
    }

    /**
     * List the available datastores for a given account and zone stored in the local cache.
     * @param datasetQuery
     * @return a list of vaults represented as ReferenceData
     */
    @Override
    Observable list(DatasetQuery datasetQuery) {
        log.debug("datastores datasetQuery.parameters: ${datasetQuery.parameters}")
        def account = datasetQuery.user.account
        def zoneId = datasetQuery.get("zoneId")?.toLong()
        if(account && zoneId) {
            return morpheusContext.async.cloud.datastore.list(new DataQuery().withFilters(
                    new DataFilter('owner.id', account.id),
                    new DataFilter('zone.id', zoneId),
                    new DataFilter('active', true),
                    new DataFilter('category', "azure.storage.service.${zoneId}")
            ))
        }

        return Observable.empty()
    }

    /**
     * List the available datastores for a given vault location stored in the local cache or fetched from the API if no cached data is available.
     * @param datasetQuery
     * @return a list of vaults represented as a collection of key/value pairs.
     */
    @Override
    Observable<Map> listOptions(DatasetQuery datasetQuery) {
        def datastores = list(datasetQuery).toList().blockingGet()

        // filter by vault location
        def backupId = datasetQuery.get("backupId")?.toLong()
        if(backupId){
            def backup = morpheusContext.services.backup.find(new DataQuery().withFilter("id", backupId))
            def vaultName = backup.getConfigProperty('vault')
            if(vaultName) {
                def backupProvider = morpheus.services.backupProvider.find(new DataQuery().withFilter("account", datasetQuery.user.account).withFilter("id", backup.backupProvider.id))
                def vault = morpheusContext.services.referenceData.find(new DataQuery().withFilters(
                        new DataFilter('category', "${backupProvider.type.code}.backup.vault.${backupProvider.id}"),
                        new DataFilter('account.id', backupProvider.account.id),
                        new DataFilter('name', vaultName)
                ))

                def vaultCloudItem = vault.getConfigProperty('cloudItem')
                if(vault && vaultCloudItem?.location) {
                    def filteredDatastores = datastores.findAll{it.regionCode == vaultCloudItem.location}
                    // only update datastores if we have any that match the location
                    if(filteredDatastores.size() > 0) {
                        log.debug("filteredDatastores found: ${filteredDatastores.size()}")
                        datastores = filteredDatastores
                    }
                }
            }
        }

        def rtn = datastores?.collect { ds -> [name:ds.name, value:ds.externalId] } ?: []
        return Observable.fromIterable(rtn)
    }

    @Override
    Datastore fetchItem(Object value) {
        return null
    }

    @Override
    Datastore item(String value) {
        return null
    }

    /**
     * gets the name for an item
     * @param item an item
     * @return the corresponding name for the name/value pair list
     */
    @Override
    String itemName(Datastore item) {
        return item.name
    }

    /**
     * gets the value for an item
     * @param item an item
     * @return the corresponding value for the name/value pair list
     */
    @Override
    String itemValue(Datastore item) {
        return item.externalId
    }

    /**
     * Returns true if the Provider is a plugin. Always true for plugin but null or false for Morpheus internal providers.
     * @return provider is plugin
     */
    @Override
    boolean isPlugin() {
        return true
    }
}


