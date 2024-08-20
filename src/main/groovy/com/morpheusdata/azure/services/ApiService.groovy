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
                resourceGroup:AzureBackupUtility.getAzureResourceGroup(cloud),
                username:AzureBackupUtility.getAzureClientId(cloud),
                password:AzureBackupUtility.getAzureClientSecret(cloud),
                networkProxy: getAzureProxy(cloud) ?: morpheusContext.services.setting.getGlobalNetworkProxy(),
                basePath:'/',
                cloudType: cloud.cloudType
        ]
        return rtn
    }

    static buildHeaders(Map headers, String token, Map opts) {
        headers = (headers ?: [:]) + ['Content-Type':'application/json;']
        if(token)
            headers['Authorization'] = (opts.authType ?: 'Bearer') + ' ' + token
        return headers
    }

    static getApiToken(Map authConfig) {
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
            HttpApiClient client = new HttpApiClient()
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
            HttpApiClient client = new HttpApiClient()
            client.networkProxy = authConfig.networkProxy
            def token = authConfig.token ?: getApiToken(authConfig)?.token
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
            HttpApiClient client = new HttpApiClient()
            client.networkProxy = authConfig.networkProxy
            def token = authConfig.token ?: getApiToken(authConfig)?.token
            def apiPath = '/subscriptions/' + authConfig.subscriberId + '/resourceGroups/' + opts.resourceGroup + '/providers/Microsoft.RecoveryServices/vaults'
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
            HttpApiClient client = new HttpApiClient()
            client.networkProxy = authConfig.networkProxy
            def token = authConfig.token ?: getApiToken(authConfig)?.token
            def apiPath = opts.vault.externalId + '/backupPolicies'
            def apiVersion = '2024-04-01'
            def headers = buildHeaders(null, token, opts)
            HttpApiClient.RequestOptions requestOpts = new HttpApiClient.RequestOptions([headers:headers, queryParams: ['api-version': apiVersion, $filter: "backupManagementType eq 'AzureIaasVM'"]])

            def results = client.callJsonApi(authConfig.apiUrl, apiPath, null, null, requestOpts, 'GET')
            if(results.success && results.data) {
                rtn.results = results.data
                rtn.success = true
            }
        } catch (e) {
            log.error("listVaults error: ${e}", e)
        }
        return rtn
    }

    static getAzureProxy(Cloud cloud) {
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
