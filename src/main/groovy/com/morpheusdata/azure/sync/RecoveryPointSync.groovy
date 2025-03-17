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
import java.text.SimpleDateFormat
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
                    def server = morpheusContext.services.computeServer.get(backup.computeServerId)

                    SyncTask<BackupResult, ArrayList<Map>, BackupResult> syncTask = new SyncTask<>(existingItems, cloudItems)
                    syncTask.addMatchFunction { BackupResult domainObject, Map cloudItem ->
                        domainObject.externalId == cloudItem.name
                    }.onDelete { List<BackupResult> removeItems ->
                        deleteRecoveryPoints(removeItems)
                    }.onUpdate { List<SyncTask.UpdateItem<BackupResult, Map>> updateItems ->
                        updateMatchedRecoveryPoints(updateItems, [authConfig: authConfig, resourceGroup: resourceGroup, vault: vault, client: client])
                    }.onAdd { itemsToAdd ->
                        addMissingRecoveryPoints(itemsToAdd, backup, server, inProgressItems, [authConfig: authConfig, resourceGroup: resourceGroup, vault: vault, client: client, containerName: containerName])
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

    private addMissingRecoveryPoints(itemsToAdd, backup, server, inProgressItems, opts) {
        log.debug "addMissingRecoveryPoints: ${itemsToAdd}"
        def adds = []
        if(itemsToAdd.size() == 0){
            return
        }

        itemsToAdd = itemsToAdd.sort { a, b -> AzureBackupUtility.parseDate(a.properties.recoveryPointTime) <=> AzureBackupUtility.parseDate(b.properties.recoveryPointTime) }
        def firstRecoveryDate = AzureBackupUtility.parseDate(itemsToAdd.first().properties.recoveryPointTime)
        def searchStartDate = firstRecoveryDate.toLocalDateTime().minusMinutes(2).toDate()
        def lastRecoveryDate = AzureBackupUtility.parseDate(itemsToAdd.last().properties.recoveryPointTime)
        def searchEndDate = lastRecoveryDate.toLocalDateTime().plusHours(24).toDate() // add 24 hours to the last recovery point, hopefully job is done by then
        def dateFormat = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss a")
        // can't filter by multiple statuses or by containerName, so we need to filter later
        def filter = "startTime eq '${dateFormat.format(searchStartDate)}' and endTime eq '${dateFormat.format(searchEndDate)}' and operation eq 'Backup' and backupManagementType eq 'AzureIaasVM'"
        def backupJobsResults = apiService.listBackupJobs(opts.authConfig, [resourceGroup: opts.resourceGroup, vault: opts.vault, filter: filter, client: opts.client])
        if(!backupJobsResults.success) {
            log.error "Error getting backup jobs: ${backupJobsResults.errorCode} - ${backupJobsResults.msg}"
            return
        }

        for(cloudItem in itemsToAdd) {
            if(!server) {
                log.warn("server not found for backup: ${backup}")
                continue
            }
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
            Date createdDay = createdDate ? createdDate.toLocalDate().atStartOfDay().toDate() : null
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
                backupSetId: backup.backupSetId ?: BackupResultUtility.generateBackupResultSetId(),
                source: 'azure',
            ]

            // remove IaasVMContainer; from containerName
            def containerName = opts?.containerName?.indexOf("IaasVMContainer;") >= -1 ? opts.containerName.substring(opts.containerName.indexOf("IaasVMContainer;") + 16) : opts.containerName
            def backupJob
            def snapshotCompleted = false
            // match completed or in progress backup job that started less than 2 minutes before the recovery point
            def matchedBackupJobs = backupJobsResults.results?.value?.findAll {
                it.properties.containerName == containerName &&
                (it.properties.status == 'Completed' || it.properties.status == 'InProgress') &&
                AzureBackupUtility.parseDate(it.properties.startTime).toInstant().isBefore(createdDate.toInstant()) &&
                AzureBackupUtility.parseDate(it.properties.startTime).toInstant().isAfter(createdDate.toInstant().minusSeconds(120))
            }
            if(matchedBackupJobs.size() == 1) {
                backupJob = matchedBackupJobs.first()
            }

            if(backupJob) {
                // need to individually fetch backup job as list doesn't include size
                def getBackupJobResult = apiService.getBackupJob(opts.authConfig, [resourceGroup: opts.resourceGroup, vault: opts.vault, jobId: backupJob.name, client: opts.client])
                if(getBackupJobResult.success == true && getBackupJobResult.results) {
                    backupJob = getBackupJobResult.results
                    if(backupJob.properties?.extendedInfo?.tasksList?.getAt(0)?.taskId == 'Take Snapshot' && backupJob.properties?.extendedInfo?.tasksList?.getAt(0)?.status == 'Completed') {
                        def startDate = AzureBackupUtility.parseDate(backupJob.properties?.extendedInfo?.tasksList?.getAt(0)?.startTime)
                        def endDate = AzureBackupUtility.parseDate(backupJob.properties?.extendedInfo?.tasksList?.getAt(0)?.endTime)
                        addConfig.startDate = startDate
                        addConfig.endDate = endDate
                        addConfig.durationMillis = (endDate && startDate) ? (endDate.time - startDate.time) : 0
                        snapshotCompleted = true
                    } else if(backupJob.properties.status == 'Completed') {
                        if (backupJob.properties.startTime && backupJob.properties.endTime) {
                            def startDate = AzureBackupUtility.parseDate(backupJob.properties.startTime)
                            def endDate = AzureBackupUtility.parseDate(backupJob.properties.endTime)
                            addConfig.startDate = startDate
                            addConfig.endDate = endDate
                            addConfig.durationMillis = (endDate && startDate) ? (endDate.time - startDate.time) : 0
                        }
                    }
                    def backupSize = backupJob.properties?.extendedInfo?.propertyBag?.'Backup Size'
                    if (backupSize) {
                        addConfig.sizeInMb = backupSize.split(" MB")[0] as Long
                    }
                } else{
                    log.error("Error getting backup job by name: ${backupJob.name} - ${getBackupJobResult.errorCode} - ${getBackupJobResult.msg}")
                }
            } else{
                log.warn("no backup job found for recovery point: ${cloudItem.name}")
            }
            def add = new BackupResult(addConfig)

            // build condensed config of what is needed for the recovery point
            def resourcePoolId = server.getConfigProperty('resourcePoolId') ?: server.resourcePool?.id ? "pool-${server.resourcePool?.id}".toString() : null
            def networkInterfaces = parseInterfacesToConfig(server)
            def instanceConfig = [
                    networkInterfaces: networkInterfaces,
                    config: [resourcePoolId: resourcePoolId]
            ]

            if(!resourcePoolId || !networkInterfaces?.network?.id || !networkInterfaces?.network?.subnet ) {
                log.error "missing required config values for recovery point: ${addConfig} config: ${instanceConfig}"
                continue
            }
            log.debug("instanceConfig: ${instanceConfig}")

            def config = [cloudItem: cloudItem, instanceConfig: instanceConfig]
            if(backupJob){
                config.backupJobId = backupJob.name
            }
            if(snapshotCompleted && backupJob.properties.status == 'InProgress') {
                config.azureBackupJobInProgress = true
            }
            add.setConfigMap(config)
            adds << add
        }

        if(adds.size() > 0) {
            log.debug "adding backup results: ${adds}"
            BulkCreateResult<BackupResult> result =  morpheusContext.async.backup.backupResult.bulkCreate(adds).blockingGet()
            if(!result.success) {
                log.error "Error adding backup results: ${result.errorCode} - ${result.msg}"
            }

            def lastResult = morpheusContext.services.backup.backupResult.find(
                new DataQuery().withFilters([
                    new DataFilter('backup.id', '=', backup.id),
                    new DataFilter('account.id', '=', backupProviderModel.account.id),
                ]).withSort('startDate', DataQuery.SortOrder.desc)
            )

            if(lastResult && backup.lastResult?.id != lastResult.id) {
                backup.lastResult = lastResult
                backup.lastBackupResultId = String.valueOf(lastResult.id)
                morpheusContext.services.backup.save(backup)
            }
        }
    }

    private deleteRecoveryPoints(List<BackupResult> removeItems) {
        log.debug "deleteRecoveryPoints: ${removeItems}"

        // only delete completed recovery points
        def itemsToRemove = []
        removeItems.each { BackupResult removeItem ->
            if(removeItem.status == BackupResult.Status.SUCCEEDED.toString()) {
                itemsToRemove << removeItem
            }
        }
        morpheusContext.async.backup.backupResult.bulkRemove(itemsToRemove).blockingGet()
    }

    private updateMatchedRecoveryPoints(List<SyncTask.UpdateItem<BackupResult, Map>> updateItems, Map opts) {
        log.debug "updateMatchedRecoveryPoints: ${updateItems.size()}"
        def saveList = []
        for(updateItem in updateItems) {
            def config = updateItem.existingItem.getConfigMap()
            if(config.azureBackupJobInProgress && config.backupJobId) {
                log.debug("checking backup job for recovery point: ${updateItem.existingItem.externalId}")
                def backupJobResult = apiService.getBackupJob(opts.authConfig, [resourceGroup: opts.resourceGroup, vault: opts.vault, jobId: config.backupJobId, client: opts.client])
                if(backupJobResult.success) {
                    def backupJob = backupJobResult.results
                    if(backupJob.properties.status == 'Completed') {
                        def backupSize = backupJob.properties.extendedInfo?.propertyBag?.'Backup Size'
                        if(backupSize) {
                            updateItem.existingItem.sizeInMb = backupSize.split(" MB")[0] as Long
                        }
                        updateItem.existingItem.setConfigProperty('azureBackupJobInProgress', false)
                        saveList << updateItem.existingItem
                    }
                } else {
                    log.error("Error getting backup job by id: ${config.backupJobId} - ${backupJobResult.errorCode} - ${backupJobResult.msg}")
                    updateItem.existingItem.setConfigProperty('azureBackupJobInProgress', false)
                    saveList << updateItem.existingItem
                }
            }
        }
        if(saveList.size() > 0) {
            morpheusContext.async.backup.backupResult.bulkSave(saveList).blockingGet()
        }
    }

    def parseInterfacesToConfig(server) {
        def interfaces = server.interfaces ?: []
        interfaces.collect { nInterface ->
            def parsedInterface = [
                    primaryInterface: nInterface.primaryInterface,
                    displayOrder:nInterface.displayOrder,
                    network: [
                            hasPool: nInterface.networkPool != null,
                            dhcpServer: nInterface.dhcp
                    ],
                    ipAddress:nInterface?.ipAddress,
                    networkInterfaceTypeId:nInterface?.type?.id,
                    networkInterfaceTypeIdName:nInterface?.type?.name,
                    ipMode: nInterface?.ipMode
            ]
            if(nInterface.network != null)
                parsedInterface.network.id = 'network-' + nInterface.network.id
            else if(nInterface.networkGroup != null)
                parsedInterface.network.id = 'networkGroup-' + nInterface.networkGroup.id
            if(nInterface.subnet != null)
                parsedInterface.network.subnet = 'subnet-' + nInterface.subnet.id
            if(nInterface.networkPool){
                parsedInterface.network.pool = [id: nInterface.networkPool.id, name: nInterface.networkPool.name]
            }
            parsedInterface
        }
    }
}
