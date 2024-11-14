package com.morpheusdata.azure.datasets

import com.morpheusdata.azure.AzureBackupPlugin
import com.morpheusdata.core.MorpheusContext
import com.morpheusdata.core.data.DataFilter
import com.morpheusdata.core.data.DataQuery
import com.morpheusdata.core.data.DatasetInfo
import com.morpheusdata.core.data.DatasetQuery
import com.morpheusdata.core.providers.AbstractDatasetProvider
import com.morpheusdata.model.Cloud
import com.morpheusdata.model.ReferenceData
import groovy.util.logging.Slf4j
import io.reactivex.rxjava3.core.Observable

@Slf4j
class VaultDatasetProvider extends AbstractDatasetProvider<ReferenceData, String> {

    public static final providerName = 'Azure Vault Dataset Provider'
    public static final providerNamespace = 'azureBackup'
    public static final providerKey = 'azureBackupVaults'
    public static final providerDescription = 'Get available vaults from Azure'

    VaultDatasetProvider(AzureBackupPlugin plugin, MorpheusContext morpheus) {
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
    Class<ReferenceData> getItemType() {
        return ReferenceData.class
    }

    /**
     * List the available vaults for a given resource group stored in the local cache.
     * @param datasetQuery
     * @return a list of vaults represented as ReferenceData
     */
    @Override
    Observable list(DatasetQuery datasetQuery) {
        log.debug("vaults: ${datasetQuery.parameters}")
        def account = datasetQuery.user.account
        Long cloudId = datasetQuery.get("zoneId")?.toLong()
        String resourceGroupExternalId = datasetQuery.get("backup.config.resourceGroup")
        if(cloudId && resourceGroupExternalId) {
            def backupProvider
            def cloud = morpheus.async.cloud.get(cloudId).blockingGet()
            def resourceGroup = morpheusContext.services.cloud.pool.find(
                    new DataQuery().withFilters(
                            new DataFilter<String>("externalId", resourceGroupExternalId),
                            new DataFilter<String>("refType", 'ComputeZone'),
                            new DataFilter<Long>("refId", cloudId as Long),
                            new DataFilter<String>("category", "azure.resourcepool.${cloudId}"),
                            new DataFilter<String>("type", 'resourceGroup'),
                    )
            )

            if(cloud?.backupProvider) {
                backupProvider = morpheus.services.backupProvider.find(new DataQuery().withFilter("account", account).withFilter("id", cloud.backupProvider.id))
            }

            if(cloud && resourceGroup && backupProvider) {
                return morpheusContext.async.referenceData.list(new DataQuery().withFilters(
                        new DataFilter('category', "${backupProvider.type.code}.backup.vault.${backupProvider.id}"),
                        new DataFilter('account.id', backupProvider.account.id),
                        new DataFilter('refType', 'ComputeZonePool'),
                        new DataFilter('refId', resourceGroup.id),
                ))
            }
        }
        return Observable.empty()
    }

    /**
     * List the available vaults for a given cloud stored in the local cache or fetched from the API of no cached data is available.
     * @param datasetQuery
     * @return a list of vaults represented as a collection of key/value pairs.
     */
    @Override
    Observable<Map> listOptions(DatasetQuery datasetQuery) {
        def vaults = list(datasetQuery).toList().blockingGet().collect {refData -> [name: refData.name, value: refData.externalId] }

        def rtn = vaults?.sort { it.name } ?: []

        return Observable.fromIterable(rtn)
    }

    @Override
    ReferenceData fetchItem(Object value) {
        return null
    }

    @Override
    ReferenceData item(String value) {
        return null
    }

    /**
     * gets the name for an item
     * @param item an item
     * @return the corresponding name for the name/value pair list
     */
    @Override
    String itemName(ReferenceData item) {
        return item.name
    }

    /**
     * gets the value for an item
     * @param item an item
     * @return the corresponding value for the name/value pair list
     */
    @Override
    String itemValue(ReferenceData item) {
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
