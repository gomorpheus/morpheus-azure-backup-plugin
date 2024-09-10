package com.morpheusdata.azure.datasets

import com.morpheusdata.azure.AzureBackupPlugin
import com.morpheusdata.core.MorpheusContext
import com.morpheusdata.core.data.DataFilter
import com.morpheusdata.core.data.DataQuery
import com.morpheusdata.core.data.DatasetInfo
import com.morpheusdata.core.data.DatasetQuery
import com.morpheusdata.core.providers.AbstractDatasetProvider
import com.morpheusdata.model.BackupJob
import groovy.util.logging.Slf4j
import io.reactivex.rxjava3.core.Observable

@Slf4j
class BackupJobDatasetProvider extends AbstractDatasetProvider<BackupJob, Long> {

    public static final providerName = 'Azure Backup Backup Job Dataset Provider'
    public static final providerNamespace = 'azureBackup'
    public static final providerKey = 'azureBackupJobs'
    public static final providerDescription = 'Get available backup jobs from Azure Backup'

    BackupJobDatasetProvider(AzureBackupPlugin plugin, MorpheusContext morpheus) {
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
    Class<BackupJob> getItemType() {
        return BackupJob.class
    }

    /**
     * List the available backup jobs for a given vault group stored in the local cache.
     * @param datasetQuery
     * @return a list of backup jobs represented as BackupJob
     */
    @Override
    Observable list(DatasetQuery datasetQuery) {
        def account = datasetQuery.user.account
        Long cloudId = datasetQuery.get("zoneId")?.toLong()

        def cloud = morpheus.async.cloud.get(cloudId).blockingGet()
        def backupProvider
        def vault = datasetQuery.get("backup.config.vault")
        def phrase = datasetQuery.get("phrase")
        if (cloud?.backupProvider) {
            backupProvider = morpheus.services.backupProvider.find(new DataQuery().withFilter("account", account).withFilter("id", cloud.backupProvider.id))
        }
        if(backupProvider && phrase && vault) {
            return morpheusContext.async.backupJob.list(new DataQuery().withFilters([
                    new DataFilter('account.id', account.id),
                    new DataFilter('backupProvider.id', backupProvider.id),
                    new DataFilter('name', '=~', phrase),
            ]))
        }

        return Observable.empty()
    }

    /**
     * List the available backup jobs for a given vault stored in the local cache or fetched from the API if no cached data is available.
     * @param datasetQuery
     * @return a list of backup jobs represented as a collection of key/value pairs.
     */
    @Override
    Observable<Map> listOptions(DatasetQuery datasetQuery) {
        def backupJobs = list(datasetQuery).toList().blockingGet()
        def vault = datasetQuery.get("backup.config.vault")
        def rtn = []
        if (backupJobs && vault) {
            rtn = backupJobs.findAll{it.getConfigProperty('vault') == vault}.collect { job -> [name: job.name, value: job.id] }.sort { it.name }
        }

        return Observable.fromIterable(rtn)
    }

    /**
     * returns the matching item from the list with the value as a string or object - since option values
     *   are often stored or passed as strings or unknown types. lets the provider do its own conversions to call
     *   item with the proper type. did object for flexibility but probably is usually a string
     * @param value the value to match the item in the list
     * @return the item
     */
    @Override
    BackupJob fetchItem(Object value) {
        def rtn = null
        if (value instanceof Long) {
            rtn = item((Long) value)
        } else if (value instanceof CharSequence) {
            def longValue = value.isNumber() ? value.toLong() : null
            if (longValue) {
                rtn = item(longValue)
            }
        }
        return rtn
    }

    /**
     * returns the item from the list with the matching value
     * @param value the value to match the item in the list
     * @return the
     */
    @Override
    BackupJob item(Long value) {
        return morpheus.services.backupJob.get(value)
    }

    /**
     * gets the name for an item
     * @param item an item
     * @return the corresponding name for the name/value pair list
     */
    @Override
    String itemName(BackupJob item) {
        return item.name
    }

    /**
     * gets the value for an item
     * @param item an item
     * @return the corresponding value for the name/value pair list
     */
    @Override
    Long itemValue(BackupJob item) {
        return item.id
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
