package com.morpheusdata.azure.sync

import com.morpheusdata.azure.services.ApiService
import com.morpheusdata.azure.AzureBackupPlugin
import com.morpheusdata.core.BulkCreateResult
import com.morpheusdata.core.MorpheusContext
import com.morpheusdata.core.data.DataFilter
import com.morpheusdata.core.data.DataQuery
import com.morpheusdata.core.util.SyncTask
import com.morpheusdata.model.BackupJob
import com.morpheusdata.model.BackupProvider
import com.morpheusdata.model.ReferenceData
import com.morpheusdata.model.projection.BackupJobIdentityProjection
import groovy.util.logging.Slf4j
import io.reactivex.rxjava3.core.Observable
import java.time.ZonedDateTime
import java.time.format.DateTimeFormatter


@Slf4j
class PolicySync {
    private AzureBackupPlugin plugin
    private MorpheusContext morpheusContext
    private BackupProvider backupProviderModel
    private ApiService apiService

    public PolicySync(BackupProvider backupProviderModel, ApiService apiService, AzureBackupPlugin plugin) {
        this.backupProviderModel = backupProviderModel
        this.apiService = apiService
        this.plugin = plugin
        this.morpheusContext = plugin.morpheusContext
    }

    def execute() {
        try {
            log.debug("PolicySync execute")
            Map authConfig = apiService.getAuthConfig(backupProviderModel)
            def objCategory = "azure.backup.vault.${backupProviderModel.id}"
            List<ReferenceData> vaults = morpheusContext.services.referenceData.list(new DataQuery().withFilters([
                new DataFilter('category', objCategory),
                new DataFilter('account.id', backupProviderModel.account.id),
            ]))

            vaults.each { vault ->
                def listResults = apiService.listPolicies(authConfig, [vault: vault])
                if(listResults.success) {
                    def cloudItems = listResults.results.value
                    Observable<BackupJobIdentityProjection> existingItems = morpheusContext.async.backupJob.listIdentityProjections(backupProviderModel)
                    SyncTask<BackupJobIdentityProjection, ArrayList<Map>, BackupJob> syncTask = new SyncTask<>(existingItems, cloudItems)
                    syncTask.addMatchFunction { BackupJobIdentityProjection domainObject, Map cloudItem ->
                        domainObject.externalId == cloudItem.id
                    }.onDelete { List<BackupJobIdentityProjection> removeItems ->
                        deleteBackupJobs(removeItems)
                    }.onUpdate { List<SyncTask.UpdateItem<BackupJob, Map>> updateItems ->
                        updateMatchedBackupJobs(updateItems)
                    }.onAdd { itemsToAdd ->
                        addMissingBackupJobs(itemsToAdd)
                    }.withLoadObjectDetailsFromFinder { List<SyncTask.UpdateItemDto<BackupJobIdentityProjection, Map>> updateItems ->
                        return morpheusContext.async.backupJob.list( new DataQuery(backupProviderModel.account).withFilter("id", 'in', updateItems.collect { it.existingItem.id }))
                    }.start()
                } else {
                    log.error("Error listing policies for vault: ${vault.name}")
                }
            }
        } catch (Exception ex) {
            log.error("PolicySync error: {}", ex, ex)
        }
    }

    private addMissingBackupJobs(itemsToAdd) {
        log.debug "addMissingBackupJobs: ${itemsToAdd}"

        def adds = []
        def objCategory = "azure.job.${backupProviderModel.id}"
        for(cloudItem in itemsToAdd) {
            def addConfig = [
                    account: backupProviderModel.account, backupProvider: backupProviderModel, code: objCategory + '.' + cloudItem.name,
                    category: objCategory, name: cloudItem.name, externalId: cloudItem.id, source: 'azure',
                    enabled: 'true', cronExpression: parseCronExpression(cloudItem.properties.schedulePolicy)
            ]

            def add = new BackupJob(addConfig)
            add.setConfigMap(cloudItem)
            adds << add
        }

        if(adds.size() > 0) {
            log.debug "adding backup jobs: ${adds}"
            BulkCreateResult<BackupJob> result =  morpheusContext.async.backupJob.bulkCreate(adds).blockingGet()
            if(!result.success) {
                log.error "Error adding backup jobs: ${result.errorCode} - ${result.msg}"
            }
        }
    }

    private deleteBackupJobs(List<BackupJobIdentityProjection> removeItems) {
        log.debug "deleteBackupJobs: ${removeItems}"
        morpheusContext.async.backupJob.bulkRemove(removeItems).blockingGet()
    }

    private updateMatchedBackupJobs(List<SyncTask.UpdateItem<BackupJob, Map>> updateItems) {
        log.debug "updateMatchedBackupJobs"
        for(SyncTask.UpdateItem<BackupJob, Map> update in updateItems) {
            Map masterItem = update.masterItem
            BackupJob existingItem = update.existingItem

            Boolean doSave = false
            if (existingItem.name != masterItem.name) {
                existingItem.name = masterItem.name
                doSave = true
            }
            def cronExpression = parseCronExpression(masterItem.properties.schedulePolicy)
            if (existingItem.cronExpression != cronExpression) {
                existingItem.cronExpression = cronExpression
                doSave = true
            }
            if (doSave == true) {
                log.debug "updating backup job!! ${existingItem.name}"
                morpheusContext.async.backupJob.save(existingItem).blockingGet()
            }
        }
    }

    private def parseCronExpression(schedulePolicy) {
        def cronExpression

        def minHourCron
        if(schedulePolicy.scheduleRunTimes?.size() == 1) {
            def datetime = ZonedDateTime.parse(schedulePolicy.scheduleRunTimes.first(), DateTimeFormatter.ISO_ZONED_DATE_TIME)
            minHourCron = "${datetime.minute} ${datetime.hour}"
        }
        if(schedulePolicy.scheduleRunFrequency == 'Daily') {
            cronExpression = minHourCron + " * * *"
        } else if(schedulePolicy.scheduleRunFrequency == 'Weekly') {
            def dayMap = [
                    'Sunday': '0',
                    'Monday': '1',
                    'Tuesday': '2',
                    'Wednesday': '3',
                    'Thursday': '4',
                    'Friday': '5',
                    'Saturday': '6'
            ]
            StringJoiner joiner = new StringJoiner(",");
            schedulePolicy.scheduleRunDays.each { day ->
                joiner.add(dayMap[day])
            }
            def daysCron = joiner.toString()
            cronExpression = minHourCron + " * * " + daysCron
        } else if(schedulePolicy.scheduleRunFrequency == 'Hourly') {
            def datetime = ZonedDateTime.parse(schedulePolicy.hourlySchedule.scheduleWindowStartTime, DateTimeFormatter.ISO_ZONED_DATE_TIME)
            def interval = schedulePolicy.hourlySchedule.interval
            def windowDuration = schedulePolicy.hourlySchedule.scheduleWindowDuration
            def hour = datetime.hour
            def StringJoiner hours = new StringJoiner(",");
            while(windowDuration > 0) {
                hours.add(hour.toString())
                hour += interval
                if(hour > 23) {
                    hour = hour - 24
                }
                windowDuration -= interval
            }

            cronExpression = "${datetime.minute} ${hours.toString()} * * *"
        }

        return cronExpression
    }
}
