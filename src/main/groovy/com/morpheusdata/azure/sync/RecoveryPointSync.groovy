package com.morpheusdata.azure.sync

import com.morpheusdata.azure.services.ApiService
import com.morpheusdata.azure.AzureBackupPlugin
import com.morpheusdata.azure.util.AzureBackupUtility
import com.morpheusdata.core.BulkCreateResult
import com.morpheusdata.core.MorpheusContext
import com.morpheusdata.core.data.DataFilter
import com.morpheusdata.core.data.DataQuery
import com.morpheusdata.core.util.HttpApiClient
import com.morpheusdata.core.util.SyncTask
import com.morpheusdata.model.Backup
import com.morpheusdata.model.BackupProvider
import com.morpheusdata.model.BackupResult
import groovy.util.logging.Slf4j
import java.time.ZonedDateTime
import java.time.temporal.ChronoUnit
import com.morpheusdata.core.backup.util.BackupResultUtility

@Slf4j
class RecoveryPointSync {
    private AzureBackupPlugin plugin
    private MorpheusContext morpheusContext
    private BackupProvider backupProviderModel
    private ApiService apiService

    public RecoveryPointSync(BackupProvider backupProviderModel, ApiService apiService, AzureBackupPlugin plugin) {
        this.backupProviderModel = backupProviderModel
        this.apiService = apiService
        this.plugin = plugin
        this.morpheusContext = plugin.morpheusContext
    }

    def execute() {
        try {
            log.debug("RecoveryPointSync execute")
            Map authConfig = apiService.getAuthConfig(backupProviderModel)
            def client = new HttpApiClient()
            List<Backup> backups = morpheusContext.services.backup.list(new DataQuery().withFilters([
                new DataFilter("backupProvider.id", backupProviderModel.id),
                new DataFilter("account.id", backupProviderModel.account.id),
            ]))

            backups.each { backup ->
                def resourceGroup = backup.getConfigProperty('resourceGroup')
                def vault = backup.getConfigProperty('vault')
                def containerName = backup.getConfigProperty('containerName')
                def protectedItemName = backup.getConfigProperty('protectedItemName')

                def listResults = apiService.getVmRecoveryPoints(authConfig, [resourceGroup: resourceGroup, vault: vault, containerName: containerName, protectedItemName: protectedItemName, client: client])
                if(listResults.success) {
                    def cloudItems = listResults.results.value
                    def existingItems = morpheusContext.async.backup.backupResult.list(new DataQuery().withFilters([
                        new DataFilter('backup.id', '=', backup.id),
                        new DataFilter('account.id', '=', backupProviderModel.account.id),
                        new DataFilter("status", BackupResult.Status.SUCCEEDED) // only successful backup will have an externalId
                    ]))

                    def inProgressItems = morpheusContext.services.backup.backupResult.list(new DataQuery().withFilters([
                        new DataFilter('backup.id', '=', backup.id),
                        new DataFilter('account.id', '=', backupProviderModel.account.id),
                        new DataFilter("status", BackupResult.Status.IN_PROGRESS)
                    ]))

                    SyncTask<BackupResult, ArrayList<Map>, BackupResult> syncTask = new SyncTask<>(existingItems, cloudItems)
                    syncTask.addMatchFunction { BackupResult domainObject, Map cloudItem ->
                        domainObject.externalId == cloudItem.name
                    }.onDelete { List<BackupResult> removeItems ->
                        deleteRecoveryPoints(removeItems)
                    }.onUpdate { List<SyncTask.UpdateItem<BackupResult, Map>> updateItems ->
                        updateMatchedRecoveryPoints(updateItems)
                    }.onAdd { itemsToAdd ->
                        addMissingRecoveryPoints(itemsToAdd, backup, inProgressItems)
                    }.withLoadObjectDetailsFromFinder { List<SyncTask.UpdateItemDto<BackupResult, Map>> updateItems ->
                        return morpheusContext.async.backup.backupResult.list( new DataQuery(backupProviderModel.account).withFilter("id", 'in', updateItems.collect { it.existingItem.id }))
                    }.start()
                } else {
                    log.error("Error listing recovery points for protectedItemName: ${protectedItemName}")
                }
            }
        } catch (Exception ex) {
            log.error("RecoveryPointSync error: {}", ex, ex)
        }
    }

    private addMissingRecoveryPoints(itemsToAdd, backup, inProgressItems) {
        log.debug "addMissingRecoveryPoints: ${itemsToAdd}"
        def adds = []
        for(cloudItem in itemsToAdd) {
            Date createdDate = AzureBackupUtility.parseDate(cloudItem.properties.recoveryPointTime)
            def skipItem = false
            // check if any in progress item is within 5 minutes of this recovery point, refreshBackupResult will set the externalId when successful
            for(inProgressItem in inProgressItems) {
                if(inProgressItem.startDate && inProgressItem.startDate.toInstant().isAfter(createdDate.toInstant().minusSeconds(300)) && inProgressItem.startDate.toInstant().isBefore(createdDate.toInstant().plusSeconds(300))) {
                    log.debug "skipping in progress recovery point: ${cloudItem.name}"
                    skipItem = true
                    break
                }
            }
            if(skipItem) {
                continue
            }
            Date createdDay = createdDate ? Date.from(createdDate.toInstant().truncatedTo(ChronoUnit.DAYS)) : null
            def addConfig = [
                zoneId: backup.zoneId,
                status: BackupResult.Status.SUCCEEDED,
                externalId: cloudItem.name,
//                internalId: cloudItem.id, // this is too large for the field, over 255 characters
                account: backup.account,
                backup: backup,
                backupName: backup.name,
                backupType: backup.backupType,
                serverId: backup.computeServerId,
                active: true,
                containerId: backup.containerId,
                instanceId: backup.instanceId,
                instanceLayoutId: backup.instanceLayoutId,
                containerTypeId: backup.containerTypeId,
                startDay: createdDay,
                startDate: createdDate,
                endDay: createdDay,
                endDate: createdDate,
                backupSetId: backup.backupSetId ?: BackupResultUtility.generateBackupResultSetId()
            ]

            def add = new BackupResult(addConfig)
            add.setConfigMap(cloudItem: cloudItem)
            adds << add
        }

        if(adds.size() > 0) {
            log.debug "adding backup results: ${adds}"
            BulkCreateResult<BackupResult> result =  morpheusContext.async.backup.backupResult.bulkCreate(adds).blockingGet()
            if(!result.success) {
                log.error "Error adding backup results: ${result.errorCode} - ${result.msg}"
            }
        }
    }

    private deleteRecoveryPoints(List<BackupResult> removeItems) {
        log.debug "deleteRecoveryPoints: ${removeItems}"
        // azure doesn't support deleting recovery points
    }

    private updateMatchedRecoveryPoints(List<SyncTask.UpdateItem<BackupResult, Map>> updateItems) {
        log.debug "updateMatchedRecoveryPoints: ${updateItems.size()}"
        // azure doesn't support updating recovery points
    }
}
