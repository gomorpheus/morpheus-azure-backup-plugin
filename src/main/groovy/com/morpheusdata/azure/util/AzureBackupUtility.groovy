package com.morpheusdata.azure.util

import com.morpheusdata.model.BackupResult
import com.morpheusdata.model.Cloud
import groovy.util.logging.Slf4j
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

@Slf4j
class AzureBackupUtility {
    static CLOUD_TYPE_MAP = [
            'usgov': [
                    'ServiceManagementUrl': 'https://management.core.usgovcloudapi.net/',
                    'ResourceManagerUrl': 'https://management.usgovcloudapi.net',
                    'ActiveDirectoryAuthority': 'https://login.microsoftonline.us',
            ],
            'china': [
                    'ServiceManagementUrl': 'https://management.core.chinacloudapi.cn/',
                    'ResourceManagerUrl': 'https://management.chinacloudapi.cn',
                    'ActiveDirectoryAuthority': 'https://login.chinacloudapi.cn',
            ],
            'german': [
                    'ServiceManagementUrl': 'https://management.core.cloudapi.de/',
                    'ResourceManagerUrl': 'https://management.microsoftazure.de',
                    'ActiveDirectoryAuthority': 'https://login.microsoftonline.de',
            ],
            'global': [
                    'ServiceManagementUrl': 'https://management.core.windows.net/',
                    'ResourceManagerUrl': 'https://management.azure.com',
                    'ActiveDirectoryAuthority': 'https://login.microsoftonline.com',
            ]
    ]

    static getAzureCloudCloudType(Cloud cloud) {
        return cloud.getConfigProperty('cloudType') ?: 'global'
    }

    static getAzureSubscriberId(Cloud cloud) {
        return cloud.getConfigProperty('subscriberId')
    }

    static getAzureIdentityPath(Cloud cloud) {
        return '/' + cloud.getConfigProperty('tenantId') + '/oauth2/token'
    }

    static getAzureTenantId(Cloud cloud) {
        return cloud.getConfigProperty('tenantId')
    }

    static getAzureResourceGroup(Cloud cloud) {
        return cloud.getConfigProperty('resourceGroup')
    }

    static getAzureClientId(Cloud cloud) {
        def rtn = cloud.accountCredentialData?.username ?: cloud.getConfigProperty('clientId')
        if(!rtn) {
            throw new Exception('no azure client id specified')
        }
        return rtn
    }

    static getAzureClientSecret(Cloud cloud) {
        def rtn = cloud.accountCredentialData?.password ?: cloud.getConfigProperty('clientSecret')
        if(!rtn) {
            throw new Exception('no azure client secret specified')
        }
        return rtn
    }

    static cloudTypeLookup(Cloud cloud, key) {
        def cloudType = getAzureCloudCloudType(cloud)
        return CLOUD_TYPE_MAP[cloudType][key]
    }

    static getAzureManagementUrl(Cloud cloud) {
        if(cloud.cloudType.code == 'azure') {
            return cloudTypeLookup(cloud, 'ResourceManagerUrl')
        } else {
            return cloud.getConfigProperty('managementUrl')
        }
    }

    static getAzureIdentityUrl(Cloud cloud) {
        if(cloud.cloudType.code == 'azure') {
            return cloudTypeLookup(cloud, 'ActiveDirectoryAuthority')
        } else {
            return cloud.getConfigProperty('identityUrl')
        }
    }

    static getAzureIdentityResourceUrl(Cloud cloud) {
        if(cloud.cloudType.code == 'azure') {
            return cloudTypeLookup(cloud, 'ServiceManagementUrl')
        } else {
            return cloud.getConfigProperty('identityResourceUrl') ?: 'https://management.core.windows.net/'
        }
    }

    static getBackupStatus(backupSessionState) {
        log.debug("getBackupStatus: ${backupSessionState}")
        def status = BackupResult.Status.IN_PROGRESS.toString()
        if (backupSessionState == "Failed") {
            status = BackupResult.Status.FAILED.toString()
        } else if (backupSessionState == "Completed") {
            status = BackupResult.Status.SUCCEEDED.toString()
        } else if (backupSessionState == "CompletedWithWarnings") {
            status = BackupResult.Status.SUCCEEDED_WARNING.toString()
        } else if (backupSessionState == "Cancelling") {
            status = BackupResult.Status.CANCEL_REQUESTED.toString()
        } else if (backupSessionState == "Cancelled") {
            status = BackupResult.Status.CANCELLED.toString()
        }
        return status
    }

    static parseDate(dateTimeString) {
        def basicPattern = "yyyy-MM-dd'T'HH:mm:ss."
        def microDigits = dateTimeString.split("\\d{4}-\\d{2}-\\d{2}T\\d{2}:\\d{2}:\\d{2}\\.")[1].split("Z")[0].length()
        def pattern = basicPattern + "S".multiply(microDigits) + "Z"

        return LocalDateTime.parse(dateTimeString, DateTimeFormatter.ofPattern(pattern)).toDate()
    }
}
