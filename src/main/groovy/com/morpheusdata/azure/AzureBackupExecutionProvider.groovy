package com.morpheusdata.azure

import com.morpheusdata.azure.util.AzureBackupUtility
import com.morpheusdata.core.MorpheusContext
import com.morpheusdata.core.Plugin
import com.morpheusdata.core.backup.BackupExecutionProvider
import com.morpheusdata.core.backup.response.BackupExecutionResponse
import com.morpheusdata.core.backup.util.BackupResultUtility
import com.morpheusdata.core.util.HttpApiClient
import com.morpheusdata.model.Backup
import com.morpheusdata.model.BackupResult
import com.morpheusdata.model.Cloud
import com.morpheusdata.model.ComputeServer
import com.morpheusdata.response.ServiceResponse
import com.morpheusdata.azure.services.ApiService
import groovy.util.logging.Slf4j

@Slf4j
class AzureBackupExecutionProvider implements BackupExecutionProvider {

	Plugin plugin
	MorpheusContext morpheusContext
	ApiService apiService

	AzureBackupExecutionProvider(Plugin plugin, MorpheusContext morpheusContext) {
		this.plugin = plugin
		this.morpheusContext = morpheusContext
		this.apiService = new ApiService(morpheusContext)
	}
	
	/**
	 * Returns the Morpheus Context for interacting with data stored in the Main Morpheus Application
	 * @return an implementation of the MorpheusContext for running Future based rxJava queries
	 */
	MorpheusContext getMorpheus() {
		return morpheusContext
	}

	/**
	 * Add additional configurations to a backup. Morpheus will handle all basic configuration details, this is a
	 * convenient way to add additional configuration details specific to this backup provider.
	 * @param backupModel the current backup the configurations are applied to.
	 * @param config the configuration supplied by external inputs.
	 * @param opts optional parameters used for configuration.
	 * @return a {@link ServiceResponse} object. A ServiceResponse with a false success will indicate a failed
	 * configuration and will halt the backup creation process.
	 */
	@Override
	ServiceResponse configureBackup(Backup backup, Map config, Map opts) {
		log.debug("configureBackup: {}, {}, {}", backup, config, opts)

		def resourceGroup = config.config?.resourceGroup
		backup.setConfigProperty("resourceGroup", resourceGroup)
		def vault = config.config?.vault
		backup.setConfigProperty("vault", vault)
		def client = new HttpApiClient()
		def backupProvider = backup.backupProvider
		def authConfig = apiService.getAuthConfig(backupProvider)

		def cacheResponse = apiService.triggerCacheProtectableVms(authConfig, [resourceGroup: resourceGroup, vault: vault, client: client])
		if(cacheResponse.success == true) {
			sleep(1500)
			def attempts = 0
			def keepGoing = true
			while(keepGoing) {
				def asyncResponse = apiService.getAsyncOpertationStatus(authConfig, [url: cacheResponse.results, client: client])
				// 204 means async task is done
				if((asyncResponse.success == true && asyncResponse.statusCode == '204') || attempts > 20) {
					keepGoing = false
				}

				if(keepGoing) {
					sleep(1500)
					attempts++
				}
			}
		}

		def server
		if(backup.computeServerId) {
			server = morpheusContext.services.computeServer.get(backup.computeServerId)
		} else {
			def workload = morpheusContext.services.workload.get(backup.containerId)
			server = morpheusContext.services.computeServer.get(workload?.server.id)
		}

		def protectedItemName
		def containerName
		def vmId
		def protectableVmsResponse = apiService.listProtectableVms(authConfig, [resourceGroup: resourceGroup, vault: vault, client: client])
		if(protectableVmsResponse.success == true) {
			for (protectableVm in protectableVmsResponse.results?.value) {
				if (protectableVm.properties.friendlyName == server.externalId) {
					protectedItemName = 'VM;' + protectableVm.name
					containerName = 'IaasVMContainer;' + protectableVm.name
					vmId = protectableVm.properties.virtualMachineId
					backup.setConfigProperty("protectedItemName", protectedItemName)
					backup.setConfigProperty("containerName", containerName)
					backup.setConfigProperty("vmId", vmId)
					break
				}
			}
		}

		if (!protectedItemName) {
			log.error("protectable vm not found for: ${server.externalId}")
			return ServiceResponse.error("protectable vm not found")
		}
		return ServiceResponse.success(backup)
	}

	/**
	 * Validate the configuration of the backup. Morpheus will validate the backup based on the supplied option type
	 * configurations such as required fields. Use this to either override the validation results supplied by the
	 * default validation or to create additional validations beyond the capabilities of option type validation.
	 * @param backupModel the backup to validate
	 * @param config the original configuration supplied by external inputs.
	 * @param opts optional parameters used for
	 * @return a {@link ServiceResponse} object. The errors field of the ServiceResponse is used to send validation
	 * results back to the interface in the format of {@code errors['fieldName'] = 'validation message' }. The msg
	 * property can be used to send generic validation text that is not related to a specific field on the model.
	 * A ServiceResponse with any items in the errors list or a success value of 'false' will halt the backup creation
	 * process.
	 */
	@Override
	ServiceResponse validateBackup(Backup backup, Map config, Map opts) {
		log.debug("validateBackup: {}, {}, {}", backup, config, opts)
		return ServiceResponse.success(backup)
	}

	/**
	 * Create the backup resources on the external provider system.
	 * @param backupModel the fully configured and validated backup
	 * @param opts additional options used during backup creation
	 * @return a {@link ServiceResponse} object. A ServiceResponse with a success value of 'false' will indicate the
	 * creation on the external system failed and will halt any further backup creation processes in morpheus.
	 */
	@Override
	ServiceResponse createBackup(Backup backup, Map opts) {
		log.debug("createBackup {}:{} to job {} with opts: {}", backup.id, backup.name, backup.backupJob.id, opts)
		ServiceResponse rtn = ServiceResponse.prepare()
		try {
			def backupProvider = backup.backupProvider
			def authConfig = apiService.getAuthConfig(backupProvider)
			def backupJob = backup.backupJob
			def resourceGroup = backup.getConfigProperty('resourceGroup')
			def vault = backup.getConfigProperty('vault')
			def protectedItemName = backup.getConfigProperty('protectedItemName')
			def containerName = backup.getConfigProperty('containerName')
			def vmId = backup.getConfigProperty('vmId')

			def results = apiService.enableProtection(authConfig, [resourceGroup: resourceGroup, vault: vault, containerName: containerName, protectedItemName: protectedItemName, vmId: vmId, policyId: backupJob.internalId])
			if(results.success == true && results.statusCode == '202') {
				rtn.success = true
			} else if (results.error?.message) {
				log.error("enableProtection error: ${results.error}")
				rtn.msg = results.error.message
			} else {
				log.error("enableProtection error: ${results}")
				rtn.msg = "Error Creating Backup"
			}

		} catch(e) {
			log.error("createBackup error: ${e}", e)
		}
		return rtn
	}

	/**
	 * Delete the backup resources on the external provider system.
	 * @param backupModel the backup details
	 * @param opts additional options used during the backup deletion process
	 * @return a {@link ServiceResponse} indicating the results of the deletion on the external provider system.
	 * A ServiceResponse object with a success value of 'false' will halt the deletion process and the local refernce
	 * will be retained.
	 */
	@Override
	ServiceResponse deleteBackup(Backup backup, Map opts) {
		log.debug("deleteBackup {}:{} with opts: {}", backup.id, backup.name, opts)
		ServiceResponse rtn = ServiceResponse.prepare()
		try {
			def backupProvider = backup.backupProvider
			def authConfig = apiService.getAuthConfig(backupProvider)
			def backupJob = backup.backupJob

			if(backupJob) {
				def resourceGroup = backup.getConfigProperty('resourceGroup')
				def vault = backup.getConfigProperty('vault')
				def containerName = backup.getConfigProperty('containerName')
				def protectedItemName = backup.getConfigProperty('protectedItemName')

				def results = apiService.deleteBackup(authConfig, [resourceGroup: resourceGroup, vault: vault, containerName: containerName, protectedItemName: protectedItemName, policyId: backupJob.internalId])
				if (results.success == true && results.statusCode == '202') {
					rtn.success = true
					rtn.data = [skipJob: true]
				} else if (results.error?.message) {
					log.error("deleteBackup error: ${results.error}")
					rtn.msg = results.error.message
				} else {
					log.error("deleteBackup error: ${results}")
					rtn.msg = "Error Deleting Backup"
				}
			}
		} catch(e) {
			log.error("deleteBackup error: ${e}", e)
		}
		return rtn
	}

	/**
	 * Delete the results of a backup execution on the external provider system.
	 * @param backupResultModel the backup results details
	 * @param opts additional options used during the backup result deletion process
	 * @return a {@link ServiceResponse} indicating the results of the deletion on the external provider system.
	 * A ServiceResponse object with a success value of 'false' will halt the deletion process and the local refernce
	 * will be retained.
	 */
	@Override
	ServiceResponse deleteBackupResult(BackupResult backupResult, Map opts) {
		return ServiceResponse.success()
	}

	/**
	 * A hook into the execution process. This method is called before the backup execution occurs.
	 * @param backupModel the backup details associated with the backup execution
	 * @param opts additional options used during the backup execution process
	 * @return a {@link ServiceResponse} indicating the success or failure of the execution preperation. A success value
	 * of 'false' will halt the execution process.
	 */
	@Override
	ServiceResponse prepareExecuteBackup(Backup backup, Map opts) {
		return ServiceResponse.success()
	}

	/**
	 * Provide additional configuration on the backup result. The backup result is a representation of the output of
	 * the backup execution including the status and a reference to the output that can be used in any future operations.
	 * @param backupResultModel
	 * @param opts
	 * @return a {@link ServiceResponse} indicating the success or failure of the method. A success value
	 * of 'false' will halt the further execution process.
	 */
	@Override
	ServiceResponse prepareBackupResult(BackupResult backupResultModel, Map opts) {
		return ServiceResponse.success()
	}

	/**
	 * Initiate the backup process on the external provider system.
	 * @param backup the backup details associated with the backup execution.
	 * @param backupResult the details associated with the results of the backup execution.
	 * @param executionConfig original configuration supplied for the backup execution.
	 * @param cloud cloud context of the target of the backup execution
	 * @param computeServer the target of the backup execution
	 * @param opts additional options used during the backup execution process
	 * @return a {@link ServiceResponse} indicating the success or failure of the backup execution. A success value
	 * of 'false' will halt the execution process.
	 */
	@Override
	ServiceResponse<BackupExecutionResponse> executeBackup(Backup backup, BackupResult backupResult, Map executionConfig, Cloud cloud, ComputeServer computeServer, Map opts) {
		log.debug("executeBackup: {}", backup)
		ServiceResponse<BackupExecutionResponse> rtn = ServiceResponse.prepare(new BackupExecutionResponse(backupResult))
		if(!backup.backupProvider.enabled) {
			rtn.error = "Azure backup provider is disabled"
			return rtn
		}
		try {
			def backupProvider = backup.backupProvider
			def authConfig = opts.authConfig ?: apiService.getAuthConfig(backupProvider)
			def resourceGroup = backup.getConfigProperty('resourceGroup')
			def vault = backup.getConfigProperty('vault')
			def containerName = backup.getConfigProperty('containerName')
			def protectedItemName = backup.getConfigProperty('protectedItemName')
			def vmId = backup.getConfigProperty('vmId')
			def client = opts.client ?: new HttpApiClient()

			def onDemandResults = apiService.triggerOnDemandBackup(authConfig, [resourceGroup: resourceGroup, vault: vault, containerName: containerName, protectedItemName: protectedItemName, vmId: vmId, policyId: backup.backupJob.internalId, client: client])
			if(onDemandResults.success == true && onDemandResults.statusCode == '202') {
				rtn.success = true

				def jobId
				sleep(1000)
				def attempts = 0
				def keepGoing = true
				while(keepGoing) {
					def asyncResponse = apiService.getAsyncOpertationStatus(authConfig, [url: onDemandResults.results, client: client])
					if((asyncResponse.success == true && asyncResponse.results?.properties?.jobId) || attempts > 9) {
						keepGoing = false
						jobId = asyncResponse.results?.properties?.jobId
						rtn.data.backupResult.setConfigProperty("backupJobId", jobId)
						rtn.data.backupResult.backupSetId = BackupResultUtility.generateBackupResultSetId()
						rtn.data.updates = true
					}

					if(keepGoing) {
						sleep(1000)
						attempts++
					}
				}
			} else if (onDemandResults.error?.message) {
				log.error("executeBackup error: ${onDemandResults.error}")
				rtn.error = onDemandResults.error.message
			} else {
				log.error("executeBackup error: ${onDemandResults}")
				rtn.error = "Error executing backup"
			}
		}
		catch (e) {
			log.error("executeBackup error: ${e}", e)
		}
		return rtn
	}

	/**
	 * Periodically call until the backup execution has successfully completed. The default refresh interval is 60 seconds.
	 * @param backupResult the reference to the results of the backup execution including the last known status. Set the
	 *                     status to a canceled/succeeded/failed value from one of the {@link BackupStatusUtility} values
	 *                     to end the execution process.
	 * @return a {@link ServiceResponse} indicating the success or failure of the method. A success value
	 * of 'false' will halt the further execution process.n
	 */
	@Override
	ServiceResponse<BackupExecutionResponse> refreshBackupResult(BackupResult backupResult) {
		log.debug("refreshBackupResult: {}", backupResult)
		ServiceResponse<BackupExecutionResponse> rtn = ServiceResponse.prepare(new BackupExecutionResponse(backupResult))
		def backup = backupResult.backup
		def backupProvider = backup.backupProvider

		if(backupProvider?.enabled) {
			def authConfig = apiService.getAuthConfig(backupProvider)
			def backupJobId = backupResult.getConfigProperty('backupJobId')
			def resourceGroup = backup.getConfigProperty('resourceGroup')
			def vault = backup.getConfigProperty('vault')

			if(backupJobId) {
				def client = new HttpApiClient()
				def getBackupJobResult = apiService.getBackupJob(authConfig, [resourceGroup: resourceGroup, vault: vault, jobId: backupJobId, client: client])
				if(getBackupJobResult.success == true && getBackupJobResult.results) {
					def backupJob = getBackupJobResult.results
					boolean doUpdate = false

					def updatedStatus = AzureBackupUtility.getBackupStatus(backupJob.properties.status)
					if(rtn.data.backupResult.status != updatedStatus) {
						rtn.data.backupResult.status = updatedStatus
						doUpdate = true
					}

					def backupSize = backupJob.properties.extendedInfo?.propertyBag?.'Backup Size'
					if(backupSize) {
						def sizeInMb = backupSize.split(" MB")[0] as Long
						if(rtn.data.backupResult.sizeInMb != sizeInMb) {
							rtn.data.backupResult.sizeInMb = sizeInMb
							doUpdate = true
						}
					}

					if(backupJob.properties.startTime && backupJob.properties.endTime) {
						def startDate = AzureBackupUtility.parseDate(backupJob.properties.startTime)
						def endDate = AzureBackupUtility.parseDate(backupJob.properties.endTime)
						if (startDate && rtn.data.backupResult.startDate != startDate) {
							rtn.data.backupResult.startDate = startDate
							doUpdate = true
						}
						if (endDate && rtn.data.backupResult.endDate != endDate) {
							rtn.data.backupResult.endDate = endDate
							doUpdate = true
						}
						def durationMillis = (endDate && startDate) ? (endDate.time - startDate.time) : 0
						if (rtn.data.backupResult.durationMillis != durationMillis) {
							rtn.data.backupResult.durationMillis = durationMillis
							doUpdate = true
						}
					}

					if(backupJob.properties.status == 'Completed'){
						def containerName = backup.getConfigProperty('containerName')
						def protectedItemName = backup.getConfigProperty('protectedItemName')
						// Azure doesn't link the job to the recovery point, so we need to find the closest recovery point to the startDate
						def vmRecoveryPointsResponse = apiService.getVmRecoveryPoints(authConfig, [resourceGroup: resourceGroup, vault: vault, containerName: containerName, protectedItemName: protectedItemName, client: client])
						if (vmRecoveryPointsResponse.success == true) {
							def recoveryPoints = vmRecoveryPointsResponse.results.value
							def startDate = backupResult.startDate
							def endDate = backupResult.endDate
							def recoveryPointsBetweenDates = recoveryPoints.findAll { recoveryPoint ->
								def recoveryPointTime = AzureBackupUtility.parseDate(recoveryPoint.properties.recoveryPointTime)
								(recoveryPointTime.after(startDate) || recoveryPointTime.equals(startDate)) && (recoveryPointTime.before(endDate) || recoveryPointTime.equals(endDate))
							}

							// Find the recovery point with the closest recoveryPointTime to the start date
							def closestRecoveryPoint = recoveryPointsBetweenDates.min { recoveryPoint ->
								Math.abs(startDate.getTime() - AzureBackupUtility.parseDate(recoveryPoint.properties.recoveryPointTime).getTime())
							}

							if(closestRecoveryPoint) {
								rtn.data.backupResult.setConfigProperty("recoveryPointId", closestRecoveryPoint.name)
								rtn.data.backupResult.externalId = closestRecoveryPoint.name
								doUpdate = true
							}
						}
					}
					rtn.data.updates = doUpdate
					rtn.success = true
				}
			}
		}
		return rtn
	}
	
	/**
	 * Cancel the backup execution process without waiting for a result.
	 * @param backupResult the details associated with the results of the backup execution.
	 * @param opts additional options.
	 * @return a {@link ServiceResponse} indicating the success or failure of the backup execution cancellation.
	 */
	@Override
	ServiceResponse cancelBackup(BackupResult backupResult, Map opts) {
		log.debug("cancelBackup: backupResult: {}, opts {}:", backupResult, opts)
		ServiceResponse response = ServiceResponse.prepare()
		if(backupResult != null) {
			try {
				def backup = backupResult.backup
				def backupProvider = backup.backupProvider
				def authConfig = apiService.getAuthConfig(backupProvider)
				def backupJobId = backupResult.externalId ?: backupResult.getConfigProperty("backupJobId")
				def resourceGroup = backup.getConfigProperty('resourceGroup')
				def vault = backup.getConfigProperty('vault')

				def result = apiService.cancelBackupJob(authConfig, [resourceGroup: resourceGroup, vault: vault, jobId: backupJobId])
				log.debug("cancelBackup : result: ${result}")
				if(result.success == true && result.statusCode == '202') {
					response.success = true
				}
			} catch(e) {
				log.error("cancelBackup error: ${e}", e)
			}
		}
		return response
	}

	/**
	 * Extract the results of a backup. This is generally used for packaging up a full backup for the purposes of
	 * a download or full archive of the backup.
	 * @param backupResultModel the details associated with the results of the backup execution.
	 * @param opts additional options.
	 * @return a {@link ServiceResponse} indicating the success or failure of the backup extraction.
	 */
	@Override
	ServiceResponse extractBackup(BackupResult backupResultModel, Map opts) {
		return ServiceResponse.success()
	}

}		
