package com.morpheusdata.azure

import com.morpheusdata.core.MorpheusContext
import com.morpheusdata.core.Plugin
import com.morpheusdata.core.backup.BackupJobProvider
import com.morpheusdata.core.backup.response.BackupExecutionResponse
import com.morpheusdata.core.data.DataFilter
import com.morpheusdata.core.data.DataQuery
import com.morpheusdata.core.util.HttpApiClient
import com.morpheusdata.model.BackupJob
import com.morpheusdata.model.BackupProvider
import com.morpheusdata.model.BackupResult
import com.morpheusdata.model.Cloud
import com.morpheusdata.model.ComputeServer
import com.morpheusdata.response.ServiceResponse
import com.morpheusdata.azure.services.ApiService
import groovy.util.logging.Slf4j
import com.morpheusdata.model.Backup

@Slf4j
class AzureBackupJobProvider implements BackupJobProvider {

    Plugin plugin;
    MorpheusContext morpheus;
    ApiService apiService
    AzureBackupExecutionProvider executionProvider

    AzureBackupJobProvider(Plugin plugin, MorpheusContext morpheus, ApiService apiService) {
        this.plugin = plugin
        this.morpheus = morpheus
        this.apiService = apiService
        this.executionProvider = new AzureBackupExecutionProvider(plugin, morpheus)
    }

    @Override
    ServiceResponse configureBackupJob(BackupJob backupJob, Map config, Map opts) {
        return ServiceResponse.success()
    }

    @Override
    ServiceResponse validateBackupJob(BackupJob backupJob, Map config, Map opts) {
        return ServiceResponse.success()
    }

    @Override
    ServiceResponse createBackupJob(BackupJob backupJob, Map map) {
        return ServiceResponse.error("Create backup job not supported.")
    }

    @Override
    ServiceResponse cloneBackupJob(BackupJob sourceBackupJobModel, BackupJob backupJobModel, Map opts) {
        return ServiceResponse.error("Clone backup job not supported.")
    }

    @Override
    ServiceResponse addToBackupJob(BackupJob backupJob, Map map) {
        return ServiceResponse.success()
    }

    @Override
    ServiceResponse deleteBackupJob(BackupJob backupJobModel, Map opts) {
        log.debug("deleteBackupJob: {}", backupJobModel)

        def rtn = ServiceResponse.prepare()
        def authConfig = apiService.getAuthConfig(backupJobModel.backupProvider)
        def deleteResponse = apiService.deletePolicy(authConfig, [backupJob: backupJobModel])
        if(deleteResponse.success) {
            rtn.success = true
        } else {
            rtn.msg = "Failed to delete backup job ${backupJobModel.id}"
        }
        return rtn
    }

    @Override
    ServiceResponse executeBackupJob(BackupJob backupJobModel, Map opts) {
        log.debug("executeBackupJob: {}, {}", backupJobModel, opts)

        ServiceResponse<List<BackupExecutionResponse>> rtn = ServiceResponse.prepare(new ArrayList<BackupExecutionResponse>())
        if (backupJobModel.backupProvider.enabled == false) {
            rtn.msg = 'Azure backup provider is disabled'
            return rtn
        }
        try {
            BackupProvider backupProvider = backupJobModel.backupProvider

            List<Backup> jobBackups = []
            this.morpheus.services.backup.list(new DataQuery().withFilters(
                    new DataFilter<>('account.id', backupJobModel.account.id),
                    new DataFilter<>('backupJob.id', backupJobModel.id),
                    new DataFilter<>('active', true)
            )).each { Backup backup ->
                if (!opts.user || backup.createdBy?.id == opts.user.id) {
                    jobBackups << backup
                }
            }
            def authConfig = apiService.getAuthConfig(backupProvider)
            def client = new HttpApiClient()
            // make empty cloud and computeServer for executeBackup
            def cloud = new Cloud()

            jobBackups.each { backup ->
                try {
                    BackupResult backupResult = new BackupResult(backup: backup)
                    def computeServer
                    if(backup.computeServerId) {
                        computeServer = this.morpheus.services.computeServer.get(backup.computeServerId)
                    } else {
                        def workload = this.morpheus.services.workload.get(backup.containerId)
                        computeServer = this.morpheus.services.computeServer.get(workload?.server.id)
                    }

                    if(computeServer){
                        def executionResponse = executionProvider.executeBackup(backup, backupResult, [:], cloud, computeServer, [authConfig: authConfig, client: client])
                        if(executionResponse.success && executionResponse.data) {
                            rtn.data.add(executionResponse.data)
                        } else {
                            log.error("Failed to execute backup for: ${backup.id}, error: ${executionResponse.error}")
                            return ServiceResponse.error("Failed to execute backup for: ${backup.id}")
                        }
                    } else {
                        log.debug("no compute server for backup: ${backup.id}")
                    }
                } catch (Exception ex) {
                    log.error("Failed to create backup result backup ${backup.id}", ex)
                    return ServiceResponse.error("Failed to create backup result for backup ${backup.id}")
                }
            }
            rtn.success = true
        } catch (e) {
            log.error("Failed to execute backup job ${backupJobModel.id}: ${e}", e)
            rtn.msg = "Failed to execute backup job ${backupJobModel.id}"
        }

        return rtn
    }
}
