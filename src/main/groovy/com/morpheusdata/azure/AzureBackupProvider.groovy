package com.morpheusdata.azure

import com.morpheusdata.azure.sync.RecoveryPointSync
import com.morpheusdata.azure.sync.VaultSync
import com.morpheusdata.core.MorpheusContext
import com.morpheusdata.core.Plugin
import com.morpheusdata.core.backup.AbstractBackupProvider
import com.morpheusdata.core.backup.BackupJobProvider
import com.morpheusdata.core.backup.DefaultBackupJobProvider
import com.morpheusdata.model.BackupJob
import com.morpheusdata.model.BackupProvider as BackupProviderModel
import com.morpheusdata.model.Icon
import com.morpheusdata.model.OptionType
import com.morpheusdata.response.ServiceResponse
import com.morpheusdata.azure.services.ApiService
import com.morpheusdata.azure.sync.PolicySync
import groovy.util.logging.Slf4j

@Slf4j
class AzureBackupProvider extends AbstractBackupProvider {

	BackupJobProvider backupJobProvider;
	ApiService apiService

	AzureBackupProvider(Plugin plugin, MorpheusContext morpheusContext) {
		super(plugin, morpheusContext)
		apiService = new ApiService(morpheusContext)

		AzureBackupTypeProvider backupTypeProvider = new AzureBackupTypeProvider(plugin, morpheus)
		plugin.registerProvider(backupTypeProvider)
		addScopedProvider(backupTypeProvider, "azure", null)
	}

	/**
	 * A unique shortcode used for referencing the provided provider. Make sure this is going to be unique as any data
	 * that is seeded or generated related to this provider will reference it by this code.
	 * @return short code string that should be unique across all other plugin implementations.
	 */
	@Override
	String getCode() {
		return 'azure-backup'
	}

	/**
	 * Provides the provider name for reference when adding to the Morpheus Orchestrator
	 * NOTE: This may be useful to set as an i18n key for UI reference and localization support.
	 *
	 * @return either an English name of a Provider or an i18n based key that can be scanned for in a properties file.
	 */
	@Override
	String getName() {
		return 'Azure Backup'
	}

	/**
	 * Returns the integration logo for display when a user needs to view or add this integration
	 * @return Icon representation of assets stored in the src/assets of the project.
	 */
	@Override
	Icon getIcon() {
		return new Icon(path:"icon.svg", darkPath: "icon-dark.svg")
	}

	/**
	 * Sets the enabled state of the provider for consumer use.
	 */
	@Override
	public Boolean getEnabled() { return true; }

	/**
	 * The backup provider is creatable by the end user. This could be false for providers that may be
	 * forced by specific CloudProvider plugins, for example.
	 */
	@Override
	public Boolean getCreatable() { return true; }

	/**
	 * The backup provider supports restoring to a new workload.
	 */
	@Override
	public Boolean getRestoreNewEnabled() { return true; }

	/**
	 * The backup provider supports backups. For example, a backup provider may be intended for disaster recovery failover
	 * only and may not directly support backups.
	 */
	@Override
	public Boolean getHasBackups() { return true; }

	/**
	 * The backup provider supports creating new jobs.
	 */
	@Override
	public Boolean getHasCreateJob() { return false; }

	/**
	 * The backup provider supports cloning a job from an existing job.
	 */
	@Override
	public Boolean getHasCloneJob() { return false; }

	/**
	 * The backup provider can add a workload backup to an existing job.
	 */
	@Override
	public Boolean getHasAddToJob() { return true; }

	/**
	 * The backup provider supports backups outside an encapsulating job.
	 */
	@Override
	public Boolean getHasOptionalJob() { return false; }

	/**
	 * The backup provider supports scheduled backups. This is primarily used for display of the schedules and providing
	 * options during the backup configuration steps.
	 */
	@Override
	public Boolean getHasSchedule() { return false; }

	/**
	 * The backup provider supports running multiple workload backups within an encapsulating job.
	 */
	@Override
	public Boolean getHasJobs() { return true; }

	/**
	 * The backup provider supports retention counts for maintaining the desired number of backups.
	 */
	@Override
	public Boolean getHasRetentionCount() { return false; }

	/**
	 * Get the list of option types for the backup provider. The option types are used for creating and updating an
	 * instance of the backup provider.
	 */
	@Override
	Collection<OptionType> getOptionTypes() {
		Collection<OptionType> optionTypes = []

		optionTypes << new OptionType(
				code:'backupProviderType.azure.cloudId', inputType: OptionType.InputType.SELECT, name:'Cloud', category:'backupProviderType.azure', noBlank: true,
				fieldName:'cloudId', fieldCode: 'gomorpheus.label.cloud', fieldLabel:'Cloud', fieldContext:'config', fieldSet:'', fieldGroup:'Options',
				required:true, enabled:true, editable:true, global:false, optionSource: 'clouds', optionSourceType:'azure',
				placeHolder:null, helpBlock:'', defaultValue:'global', custom:false, displayOrder:0, fieldClass:null, fieldSize:15)

		return optionTypes
	}

	/**
	 * Get the list of replication group option types for the backup provider. The option types are used for creating and updating
	 * replication groups.
	 */
	@Override
	Collection<OptionType> getReplicationGroupOptionTypes() {
		Collection<OptionType> optionTypes = []
		return optionTypes;
	}

	/**
	 * Get the list of replication option types for the backup provider. The option types are used for creating and updating
	 * replications.
	 */
	@Override
	Collection<OptionType> getReplicationOptionTypes() {
		Collection<OptionType> optionTypes = new ArrayList()
		return optionTypes;
	}

	/**
	 * Get the list of backup job option types for the backup provider. The option types are used for creating and updating
	 * backup jobs.
	 */
	@Override
	Collection<OptionType> getBackupJobOptionTypes() {
		Collection<OptionType> optionTypes = []
		return optionTypes;
	}

	/**
	 * Get the list of backup option types for the backup provider. The option types are used for creating and updating
	 * backups.
	 */
	@Override
	Collection<OptionType> getBackupOptionTypes() {
		Collection<OptionType> optionTypes = []
		return optionTypes;
	}

	/**
	 * Get the list of replication group option types for the backup provider. The option types are used for creating
	 * replications on an instance during provisioning.
	 */
	@Override
	Collection<OptionType> getInstanceReplicationGroupOptionTypes() {
		Collection<OptionType> optionTypes = new ArrayList()
		return optionTypes;
	}

	/**
	 * Get the {@link BackupJobProvider} responsible for all backup job operations in this backup provider
	 * The {@link DefaultBackupJobProvider} can be used if the provider would like morpheus to handle all job operations.
	 * @return the {@link BackupJobProvider} for this backup provider
	 */
	@Override
	BackupJobProvider getBackupJobProvider() {
		// The default backup job provider allows morpheus to handle the
		// scheduling and execution of the jobs. Replace the default job provider
		// if jobs are to be managed on the external backup system.
		if(!this.backupJobProvider) {
			this.backupJobProvider = new AzureBackupJobProvider(getPlugin(), morpheus, apiService)
		}
		return this.backupJobProvider
	}

	/**
	 * Apply provider specific configurations to a {@link com.morpheusdata.model.BackupProvider}. The standard configurations are handled by the core system.
	 * @param backupProviderModel backup provider to configure
	 * @param config the configuration supplied by external inputs.
	 * @param opts optional parameters used for configuration.
	 * @return a {@link ServiceResponse} object. A ServiceResponse with a false success will indicate a failed
	 * configuration and will halt the backup creation process.
	 */
	@Override
	ServiceResponse configureBackupProvider(BackupProviderModel backupProviderModel, Map config, Map opts) {
		return ServiceResponse.success(backupProviderModel)
	}

	/**
	 * Validate the configuration of the {@link com.morpheusdata.model.BackupProvider}. Morpheus will validate the backup based on the supplied option type
	 * configurations such as required fields. Use this to either override the validation results supplied by the
	 * default validation or to create additional validations beyond the capabilities of option type validation.
	 * @param backupProviderModel backup provider to validate
	 * @param opts additional options
	 * @return a {@link ServiceResponse} object. A ServiceResponse with a false success will indicate a failed
	 * validation and will halt the backup provider creation process.
	 */
	@Override
	ServiceResponse validateBackupProvider(BackupProviderModel backupProviderModel, Map opts) {
		def rtn = [success:false, errors:[:]]
		try {
			def apiOpts = [:]

			if(rtn.errors.size() == 0) {
				def testResults = verifyAuthentication(backupProviderModel, apiOpts)
				log.debug("api test results: {}", testResults)
				if(testResults.success == true) {
					rtn.success = true
				} else if(testResults.invalidLogin == true) {
					rtn.msg = testResults.msg ?: 'unauthorized - invalid credentials'
				} else if(testResults.found == false) {
					rtn.msg = testResults.msg ?: 'Azure backup service not found - invalid host'
				} else {
					rtn.msg = testResults.msg ?: 'unable to connect to Azure backup service'
				}
			}
		} catch(e) {
			log.error("error validating Azure Backup configuration: ${e}", e)
			rtn.msg = 'unknown error connecting to Azure Backup service'
			rtn.success = false
		}
		if(rtn.success) {
			return ServiceResponse.success(backupProviderModel, rtn.msg)
		} else {
			return ServiceResponse.error(rtn.msg, rtn.errors as Map, backupProviderModel)
		}
	}

	/**
	 * Delete the backup provider. Typically used to clean up any provider specific data that will not be cleaned
	 * up by the default remove in the core system.
	 * @param backupProviderModel the backup provider being removed
	 * @param opts additional options
	 * @return a {@link ServiceResponse} object. A ServiceResponse with a false success will indicate a failed
	 * delete and will halt the process.
	 */
	@Override
	ServiceResponse deleteBackupProvider(BackupProviderModel backupProviderModel, Map opts) {
		return ServiceResponse.success()
	}

	/**
	 * The main refresh method called periodically by Morpheus to sync any necessary objects from the integration.
	 * This can call sub services for better organization. It is recommended that {@link com.morpheusdata.core.util.SyncTask} is used.
	 * @param backupProvider the current instance of the backupProvider being refreshed
	 * @return the success state of the refresh
	 */
	@Override
	ServiceResponse refresh(BackupProviderModel backupProviderModel) {
		log.debug("refresh backup provider: [{}:{}]", backupProviderModel.name, backupProviderModel.id)

		ServiceResponse rtn = ServiceResponse.prepare()
		try {
			def authConfig = apiService.getAuthConfig(backupProviderModel)
			def apiOpts = [authConfig:authConfig]
			def hostOnline = true
			if(hostOnline) {
				def testResults = verifyAuthentication(backupProviderModel, apiOpts)
				if(testResults.success == true) {
					morpheus.async.backupProvider.updateStatus(backupProviderModel, 'ok', null).subscribe().dispose()

					new PolicySync(backupProviderModel, apiService, plugin).execute()
					new VaultSync(backupProviderModel, apiService, plugin).execute()
					new RecoveryPointSync(backupProviderModel, apiService, plugin).execute()

					rtn.success = true
				} else {
					if(testResults.invalidLogin == true) {
						log.debug("refreshBackupProvider: Invalid credentials")
						morpheus.async.backupProvider.updateStatus(backupProviderModel, 'error', 'invalid credentials').subscribe().dispose()
					} else {
						log.debug("refreshBackupProvider: error connecting to host")
						morpheus.async.backupProvider.updateStatus(backupProviderModel, 'error', 'error connecting').subscribe().dispose()
					}
				}
			} else {
				morpheus.async.backupProvider.updateStatus(backupProviderModel, 'offline', 'azure not reachable').subscribe().dispose()
			}
		} catch(Exception e) {
			log.error("error refreshing backup provider {}::{}: {}", plugin.name, this.name, e)
		}
		return rtn
	}

	Collection<BackupJob> filterBackupJobs(Collection<BackupJob> backupJobs, Map opts) {
		log.debug("filterBackupJobs: {}", backupJobs)

		return backupJobs.findAll{it.getConfigProperty('vault') == opts['config[vault]'] && it.getConfigProperty('resourceGroup') == opts['config[resourceGroup]']}
	}

	private verifyAuthentication(BackupProviderModel backupProviderModel, Map opts) {
		def rtn = [success:false, invalidLogin:false, found:true]
		opts.authConfig = opts.authConfig ?: apiService.getAuthConfig(backupProviderModel)
		def requestResults = apiService.listSubscriptions(opts.authConfig, opts)

		if(requestResults.success == true) {
			rtn.success = true
		} else {
			if(requestResults?.errorCode == '404' || requestResults?.errorCode == 404)
				rtn.found = false
			if(requestResults?.errorCode == '401' || requestResults?.errorCode == 401)
				rtn.invalidLogin = true
		}
		return rtn
	}
}
