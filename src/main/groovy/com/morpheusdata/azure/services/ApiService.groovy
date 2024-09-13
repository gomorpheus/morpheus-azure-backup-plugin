package com.morpheusdata.azure.services

import com.morpheusdata.core.MorpheusContext
import com.morpheusdata.core.util.HttpApiClient
import com.morpheusdata.model.AccountCredential
import com.morpheusdata.model.Cloud
import com.morpheusdata.azure.util.AzureBackupUtility
import com.morpheusdata.model.BackupProvider
import com.morpheusdata.model.NetworkProxy
import groovy.util.logging.Slf4j

@Slf4j
class ApiService {
    MorpheusContext morpheusContext

    ApiService(MorpheusContext morpheusContext) {
        this.morpheusContext = morpheusContext
    }

    static tokenBuffer = 1000l * 10l //10 second buffer

    //auth config
    Map getAuthConfig(BackupProvider backupProviderModel) {
        def cloudId = backupProviderModel.getConfigProperty('cloudId') as Long
        def cloud = morpheusContext.async.cloud.get(cloudId).blockingGet()

        if(!cloud.accountCredentialLoaded) {
            AccountCredential accountCredential
            try {
                accountCredential = morpheusContext.services.accountCredential.loadCredentials(cloud)
            } catch(e) {
                // If there is no credential on the cloud, then this will error
            }
            cloud.accountCredentialLoaded = true
            cloud.accountCredentialData = accountCredential?.data
        }

        def rtn = [
                apiUrl:AzureBackupUtility.getAzureManagementUrl(cloud),
                identityUrl:AzureBackupUtility.getAzureIdentityUrl(cloud),
                resourceUrl:AzureBackupUtility.getAzureIdentityResourceUrl(cloud),
                subscriberId:AzureBackupUtility.getAzureSubscriberId(cloud),
                identityPath:AzureBackupUtility.getAzureIdentityPath(cloud),
                tenantId:AzureBackupUtility.getAzureTenantId(cloud),
                username:AzureBackupUtility.getAzureClientId(cloud),
                password:AzureBackupUtility.getAzureClientSecret(cloud),
                networkProxy: getAzureProxy(cloud) ?: morpheusContext.services.setting.getGlobalNetworkProxy(),
                basePath:'/',
                cloudType: cloud.cloudType
        ]
        return rtn
    }

    static buildHeaders(Map headers, String token, Map opts) {
        headers = (headers ?: [:]) + ['Content-Type':'application/json']
        if(token)
            headers['Authorization'] = (opts.authType ?: 'Bearer') + ' ' + token
        return headers
    }

    static getApiToken(Map authConfig, Map opts) {
        def rtn = [success:false]
        def requestToken = true
        if(authConfig.token) {
            if(authConfig.expires) {
                def checkDate = new Date()
                if(authConfig.expires && authConfig.expires.time < (checkDate.time - tokenBuffer)) {
                    log.info("api access token is expired, expires: ${authConfig.expires}, re-authenticating now to get a new token")
                    requestToken = true
                } else {
                    requestToken = false
                    rtn.success = true
                    rtn.token = authConfig.token
                }
            } else {
                log.info("api access token is valid, expires: ${authConfig.expires}, using existing token")
                requestToken = false
                rtn.success = true
                rtn.token = authConfig.token
            }
        }
        //if need a new one
        if(requestToken == true) {
            HttpApiClient client = opts.client ?: new HttpApiClient()
            client.networkProxy = authConfig.networkProxy
            def apiUrl = authConfig.identityUrl
            def apiPath = authConfig.identityPath
            def body = [
                    grant_type:'client_credentials',
                    resource:authConfig.resourceUrl,
                    client_id:authConfig.username,
                    client_secret:authConfig.password
            ]
            def headers = ['Content-Type':'application/x-www-form-urlencoded']
            HttpApiClient.RequestOptions requestOpts = new HttpApiClient.RequestOptions([headers:headers, body:body, contentType: 'form'])
            def results = client.callJsonApi(apiUrl, apiPath, null, null, requestOpts, 'POST')
            rtn.success = results?.success && results?.error != true
            if(rtn.success == true) {
                rtn.results = results.data
                rtn.token = rtn.results.access_token
                if(rtn.results.expires_in) {
                    rtn.expires = new Date(System.currentTimeMillis() + (rtn.results.expires_in.toLong() * 1000l)) // returned as "3599"
                } else {
                    // no expiration? it is always returned though
                    // rtn.expires = new Date(System.currentTimeMillis() + (1000l * 3599l))
                }
                log.debug("Successfully retrieved a new api token that expires: ${rtn.expires}")
                authConfig.token = rtn.token
                authConfig.expires = rtn.expires
            } else {
                rtn.content = results.content
                rtn.data = results.data
                rtn.errorCode = results.errorCode
                rtn.headers = results.headers
                authConfig.token = null
                authConfig.expires = null
            }
        }
        return rtn
    }

    static listSubscriptions(Map authConfig, Map opts) {
        def rtn = [success:false]
        try {
            HttpApiClient client = opts.client ?: new HttpApiClient()
            client.networkProxy = authConfig.networkProxy
            def token = authConfig.token ?: getApiToken(authConfig, [client: client])?.token
            def apiPath = '/subscriptions'
            def apiVersion = authConfig.cloudType.code == 'azure' ? '2015-01-01' : '2018-05-01'
            def headers = buildHeaders(null, token, opts)
            HttpApiClient.RequestOptions requestOpts = new HttpApiClient.RequestOptions([headers:headers, queryParams: ['api-version': apiVersion]])

            def results = client.callJsonApi(authConfig.apiUrl, apiPath, null, null, requestOpts, 'GET')
            if(results.success && results.data) {
                rtn.results = results.data
                rtn.success = true
            }
        } catch(e) {
            log.error("listSubscriptions error: ${e}", e)
        }
        return rtn
    }

    static listVaults(Map authConfig, Map opts) {
        def rtn = [success:false]
        try {
            HttpApiClient client = opts.client ?: new HttpApiClient()
            client.networkProxy = authConfig.networkProxy
            def token = authConfig.token ?: getApiToken(authConfig, [client: client])?.token
            def apiPath = "/subscriptions/${authConfig.subscriberId}/resourceGroups/${opts.resourceGroup}/providers/Microsoft.RecoveryServices/vaults"
            def apiVersion = '2023-04-01'
            def headers = buildHeaders(null, token, opts)
            HttpApiClient.RequestOptions requestOpts = new HttpApiClient.RequestOptions([headers:headers, queryParams: ['api-version': apiVersion]])

            def results = client.callJsonApi(authConfig.apiUrl, apiPath, null, null, requestOpts, 'GET')
            if(results.success && results.data) {
                rtn.results = results.data
                rtn.success = true
            }
        } catch(e) {
            log.error("listVaults error: ${e}", e)
        }
        return rtn
    }

    static listPolicies(Map authConfig, Map opts) {
        def rtn = [success: false]
        try {
            HttpApiClient client = opts.client ?: new HttpApiClient()
            client.networkProxy = authConfig.networkProxy
            def token = authConfig.token ?: getApiToken(authConfig, [client: client])?.token
            def apiPath = opts.vault?.internalId + '/backupPolicies'
            def apiVersion = '2024-04-01'
            def headers = buildHeaders(null, token, opts)
            HttpApiClient.RequestOptions requestOpts = new HttpApiClient.RequestOptions([headers:headers, queryParams: ['api-version': apiVersion, $filter: "backupManagementType eq 'AzureIaasVM'"]])

            def results = client.callJsonApi(authConfig.apiUrl, apiPath, null, null, requestOpts, 'GET')
            if(results.success && results.data) {
                rtn.results = results.data
                rtn.success = true
            }
        } catch (e) {
            log.error("listPolicies error: ${e}", e)
        }
        return rtn
    }

    static triggerCacheProtectableVms(Map authConfig, Map opts) {
        def rtn = [success: false]
        try {
            HttpApiClient client = opts.client ?: new HttpApiClient()
            client.networkProxy = authConfig.networkProxy
            def token = authConfig.token ?: getApiToken(authConfig, [client: client])?.token
            def apiPath = "/subscriptions/${authConfig.subscriberId}/resourceGroups/${opts.resourceGroup}/providers/Microsoft.RecoveryServices/vaults/${opts.vault}/backupFabrics/Azure/refreshContainers"
            def apiVersion = '2016-12-01'
            def headers = buildHeaders(null, token, opts)
            HttpApiClient.RequestOptions requestOpts = new HttpApiClient.RequestOptions([headers:headers, queryParams: ['api-version': apiVersion]])

            def results = client.callJsonApi(authConfig.apiUrl, apiPath, null, null, requestOpts, 'POST')
            if(results.success) {
                rtn.results = results.headers?.Location
                rtn.success = true
            }
        } catch (e) {
            log.error("cacheProtectableVms error: ${e}", e)
        }
        return rtn
    }

    static getAsyncOpertationStatus(Map authConfig, Map opts) {
        def rtn = [success: false]
        try {
            HttpApiClient client = opts.client ?: new HttpApiClient()
            client.networkProxy = authConfig.networkProxy
            def token = authConfig.token ?: getApiToken(authConfig, [client: client])?.token
            def headers = buildHeaders(null, token, opts)
            HttpApiClient.RequestOptions requestOpts = new HttpApiClient.RequestOptions([headers:headers])

            def results = client.callJsonApi(opts.url, null, null, null, requestOpts, 'GET')
            if(results.success) {
                rtn.results = results.data
                rtn.statusCode = results.statusCode
                rtn.success = true
            }
        } catch (e) {
            log.error("getAsyncOpertationStatus error: ${e}", e)
        }
        return rtn
    }

    static listProtectableVms(Map authConfig, Map opts) {
        def rtn = [success: false]
        try {
            HttpApiClient client = opts.client ?: new HttpApiClient()
            client.networkProxy = authConfig.networkProxy
            def token = authConfig.token ?: getApiToken(authConfig, [client: client])?.token
            def apiPath = "/subscriptions/${authConfig.subscriberId}/resourceGroups/${opts.resourceGroup}/providers/Microsoft.RecoveryServices/vaults/${opts.vault}/backupProtectableItems"
            def apiVersion = '2016-12-01'
            def headers = buildHeaders(null, token, opts)
            HttpApiClient.RequestOptions requestOpts = new HttpApiClient.RequestOptions([headers:headers, queryParams: ['api-version': apiVersion, $filter: "backupManagementType eq 'AzureIaasVM'"]])

            def results = client.callJsonApi(authConfig.apiUrl, apiPath, null, null, requestOpts, 'GET')
            if(results.success && results.data) {
                rtn.results = results.data
                rtn.success = true
            }
        } catch (e) {
            log.error("listProtectableVms error: ${e}", e)
        }
        return rtn
    }

    static listProtectedVms(Map authConfig, Map opts) {
        def rtn = [success: false]
        try {
            HttpApiClient client = opts.client ?: new HttpApiClient()
            client.networkProxy = authConfig.networkProxy
            def token = authConfig.token ?: getApiToken(authConfig, [client: client])?.token
            def apiPath = "/subscriptions/${authConfig.subscriberId}/resourceGroups/${opts.resourceGroup}/providers/Microsoft.RecoveryServices/vaults/${opts.vault}/backupProtectedItems"
            def apiVersion = '2024-04-01'
            def headers = buildHeaders(null, token, opts)
            HttpApiClient.RequestOptions requestOpts = new HttpApiClient.RequestOptions([headers:headers, queryParams: ['api-version': apiVersion]])

            def results = client.callJsonApi(authConfig.apiUrl, apiPath, null, null, requestOpts, 'GET')
            if(results.success && results.data) {
                rtn.results = results.data
                rtn.success = true
            }
        } catch (e) {
            log.error("listProtectedVms error: ${e}", e)
        }
        return rtn
    }

    static enableProtection(Map authConfig, Map opts) {
        def rtn = [success: false]

        try {
            HttpApiClient client = opts.client ?: new HttpApiClient()
            client.networkProxy = authConfig.networkProxy
            def token = authConfig.token ?: getApiToken(authConfig, [client: client])?.token
            def apiPath = "/subscriptions/${authConfig.subscriberId}/resourceGroups/${opts.resourceGroup}/providers/Microsoft.RecoveryServices/vaults/${opts.vault}/backupFabrics/Azure/protectionContainers/${opts.containerName}/protectedItems/${opts.protectedItemName}"
            def apiVersion = '2024-04-01'
            def headers = buildHeaders(null, token, opts)

            def body = [
                properties: [
                    protectedItemType: 'Microsoft.Compute/virtualMachines',
                    sourceResourceId: opts.vmId,
                    policyId: opts.policyId
                ]
            ]
            HttpApiClient.RequestOptions requestOpts = new HttpApiClient.RequestOptions([headers:headers, queryParams: ['api-version': apiVersion], body: body])

            def results = client.callJsonApi(authConfig.apiUrl, apiPath, null, null, requestOpts, 'PUT')
            if(results.success) {
                rtn.results = results.headers?.Location
                rtn.statusCode = results.statusCode
                rtn.success = true
            } else {
                rtn.error = results.data?.error
            }
        } catch (e) {
            log.error("enableProtection error: ${e}", e)
        }
        return rtn
    }

    static triggerOnDemandBackup(Map authConfig, Map opts) {
        def rtn = [success: false]

        try {
            HttpApiClient client = opts.client ?: new HttpApiClient()
            client.networkProxy = authConfig.networkProxy
            def token = authConfig.token ?: getApiToken(authConfig, [client: client])?.token
            def apiPath = "/subscriptions/${authConfig.subscriberId}/resourceGroups/${opts.resourceGroup}/providers/Microsoft.RecoveryServices/vaults/${opts.vault}/backupFabrics/Azure/protectionContainers/${opts.containerName}/protectedItems/${opts.protectedItemName}/backup"
            def apiVersion = '2016-12-01'
            def headers = buildHeaders(null, token, opts)
            def body = [
                properties: [
                    objectType: "IaasVMBackupRequest"
                ]
            ]
            HttpApiClient.RequestOptions requestOpts = new HttpApiClient.RequestOptions([headers:headers, queryParams: ['api-version': apiVersion], body: body])

            def results = client.callJsonApi(authConfig.apiUrl, apiPath, null, null, requestOpts, 'POST')
            if(results.success) {
                rtn.results = results.headers?.'Azure-AsyncOperation'
                rtn.statusCode = results.statusCode
                rtn.success = true
            }
        } catch (e) {
            log.error("triggerOnDemandBackup error: ${e}", e)
        }
        return rtn
    }

    static removeProtection(Map authConfig, Map opts) {
        def rtn = [success: false]

        try {
            HttpApiClient client = opts.client ?: new HttpApiClient()
            client.networkProxy = authConfig.networkProxy
            def token = authConfig.token ?: getApiToken(authConfig, [client: client])?.token
            def apiPath = "/subscriptions/${authConfig.subscriberId}/resourceGroups/${opts.resourceGroup}/providers/Microsoft.RecoveryServices/vaults/${opts.vault}/backupFabrics/Azure/protectionContainers/${opts.containerName}/protectedItems/${opts.protectedItemName}"
            def apiVersion = '2024-04-01'
            def headers = buildHeaders(null, token, opts)
            def body = [
                properties: [
                    protectedItemType: 'Microsoft.Compute/virtualMachines',
                    sourceResourceId: opts.vmId,
                    protectionState: 'ProtectionStopped'
                ]
            ]

            HttpApiClient.RequestOptions requestOpts = new HttpApiClient.RequestOptions([headers:headers, queryParams: ['api-version': apiVersion], body: body])
            def results = client.callJsonApi(authConfig.apiUrl, apiPath, null, null, requestOpts, 'PUT')
            if(results.success) {
                rtn.results = results.headers?.Location
                rtn.statusCode = results.statusCode
                rtn.success = true
            }
        } catch (e) {
            log.error("removeProtection error: ${e}", e)
        }
        return rtn
    }

    static deleteBackup(Map authConfig, Map opts) {
        def rtn = [success: false]

        try {
            HttpApiClient client = opts.client ?: new HttpApiClient()
            client.networkProxy = authConfig.networkProxy
            def token = authConfig.token ?: getApiToken(authConfig, [client: client])?.token
            def apiPath = "/subscriptions/${authConfig.subscriberId}/resourceGroups/${opts.resourceGroup}/providers/Microsoft.RecoveryServices/vaults/${opts.vault}/backupFabrics/Azure/protectionContainers/${opts.containerName}/protectedItems/${opts.protectedItemName}"
            def apiVersion = '2019-05-13'
            def headers = buildHeaders(null, token, opts)
            HttpApiClient.RequestOptions requestOpts = new HttpApiClient.RequestOptions([headers:headers, queryParams: ['api-version': apiVersion]])

            def results = client.callJsonApi(authConfig.apiUrl, apiPath, null, null, requestOpts, 'DELETE')
            if(results.success) {
                rtn.results = results.headers?.Location
                rtn.statusCode = results.statusCode
                rtn.success = true
            }
        } catch (e) {
            log.error("deleteBackup error: ${e}", e)
        }
        return rtn
    }

    // if soft delete is enabled have to undo delete before vm can be protected again
    static undoDeleteBackup(Map authConfig, Map opts) {
        def rtn = [success: false]

        try {
            HttpApiClient client = opts.client ?: new HttpApiClient()
            client.networkProxy = authConfig.networkProxy
            def token = authConfig.token ?: getApiToken(authConfig, [client: client])?.token

            def apiPath = "/subscriptions/${authConfig.subscriberId}/resourceGroups/${opts.resourceGroup}/providers/Microsoft.RecoveryServices/vaults/${opts.vault}/backupFabrics/Azure/protectionContainers/${opts.containerName}/protectedItems/${opts.protectedItemName}"
            def apiVersion = '2019-05-13'
            def headers = buildHeaders(null, token, opts)
            def body = [
                properties: [
                    protectedItemType: 'Microsoft.Compute/virtualMachines',
                    sourceResourceId: opts.vmId,
                    protectionState: 'ProtectionStopped',
                    "isRehydrate": true
                ]
            ]
            HttpApiClient.RequestOptions requestOpts = new HttpApiClient.RequestOptions([headers:headers, queryParams: ['api-version': apiVersion], body: body])

            def results = client.callJsonApi(authConfig.apiUrl, apiPath, null, null, requestOpts, 'PUT')
            if(results.success) {
                rtn.results = results.headers?.Location
                rtn.statusCode = results.statusCode
                rtn.success = true
            }
        } catch (e) {
            log.error("undoDeleteBackup error: ${e}", e)
        }
        return rtn
    }

    static getBackupJob(Map authConfig, Map opts) {
        def rtn = [success: false]

        try {
            HttpApiClient client = opts.client ?: new HttpApiClient()
            client.networkProxy = authConfig.networkProxy
            def token = authConfig.token ?: getApiToken(authConfig, [client: client])?.token
            def apiPath = "/subscriptions/${authConfig.subscriberId}/resourceGroups/${opts.resourceGroup}/providers/Microsoft.RecoveryServices/vaults/${opts.vault}/backupjobs/${opts.jobId}"
            def apiVersion = '2019-05-13'
            def headers = buildHeaders(null, token, opts)
            HttpApiClient.RequestOptions requestOpts = new HttpApiClient.RequestOptions([headers:headers, queryParams: ['api-version': apiVersion]])

            def results = client.callJsonApi(authConfig.apiUrl, apiPath, null, null, requestOpts, 'GET')
            if(results.success) {
                rtn.results = results.data
                rtn.statusCode = results.statusCode
                rtn.success = true
            }
        } catch (e) {
            log.error("getBackupJob error: ${e}", e)
        }
        return rtn
    }

    static cancelBackupJob(Map authConfig, Map opts) {
        def rtn = [success: false]

        try {
            HttpApiClient client = opts.client ?: new HttpApiClient()
            client.networkProxy = authConfig.networkProxy
            def token = authConfig.token ?: getApiToken(authConfig, [client: client])?.token
            def apiPath = "/subscriptions/${authConfig.subscriberId}/resourceGroups/${opts.resourceGroup}/providers/Microsoft.RecoveryServices/vaults/${opts.vault}/backupjobs/${opts.jobId}/cancel"
            def apiVersion = '2024-04-01'
            def headers = buildHeaders(null, token, opts)
            HttpApiClient.RequestOptions requestOpts = new HttpApiClient.RequestOptions([headers:headers, queryParams: ['api-version': apiVersion]])

            def results = client.callJsonApi(authConfig.apiUrl, apiPath, null, null, requestOpts, 'POST')
            if(results.success) {
                rtn.results = results.headers?.Location
                rtn.statusCode = results.statusCode
                rtn.success = true
            }
        } catch (e) {
            log.error("cancelBackupJob error: ${e}", e)
        }
        return rtn
    }

    private getAzureProxy(Cloud cloud) {
        if(cloud.apiProxy) {
            def networkProxy = new NetworkProxy(
                    proxyHost: cloud.apiProxy?.proxyHost,
                    proxyPort: cloud.apiProxy?.proxyPort,
                    proxyUser:  cloud.apiProxy?.proxyUser,
                    proxyPassword:cloud.apiProxy?.proxyPassword,
                    proxyDomain: cloud.apiProxy?.proxyDomain,
                    proxyWorkstation: cloud.apiProxy?.proxyWorkstation
            )
            return networkProxy
        } else {
            return null
        }
    }
}
