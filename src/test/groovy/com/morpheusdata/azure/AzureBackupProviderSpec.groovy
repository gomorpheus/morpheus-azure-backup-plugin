package com.morpheusdata.azure

import com.morpheusdata.azure.sync.PolicySync
import spock.lang.Specification
import spock.lang.Shared
import spock.lang.Subject
import com.morpheusdata.core.MorpheusContext
import com.morpheusdata.azure.util.AzureBackupUtility
import com.morpheusdata.model.BackupProvider

class AzureBackupProviderSpec extends Specification {

    @Subject@Shared
    AzureBackupProvider provider
    @Shared
    AzureBackupUtility utility
    @Shared
    MorpheusContext context
    @Shared
    AzureBackupPlugin plugin
    @Shared
    BackupProvider backupProviderModel

    def setup() {
        plugin = Mock(AzureBackupPlugin)
        context = Mock(MorpheusContext)
        utility = Mock(AzureBackupUtility)
        backupProviderModel = Mock(BackupProvider)
        provider = new AzureBackupProvider(plugin, context)
    }

    def "parseCronExpression should return correct cron expression for SimpleSchedulePolicy frequency Daily"() {
        given:
        def policySync = new PolicySync(backupProviderModel, provider.apiService, plugin)
        def schedulePolicy = [schedulePolicyType: "SimpleSchedulePolicy", scheduleRunTimes: ["2023-10-10T10:00:00Z"], scheduleRunFrequency: "Daily"]

        when:
        def cronExpression = policySync.parseCronExpression(schedulePolicy)

        then:
        cronExpression == "0 10 * * *" // At 10:00 AM
    }

    def "parseCronExpression should return correct cron expression for SimpleSchedulePolicy frequency Weekly on Sundays"() {
        given:
        def policySync = new PolicySync(backupProviderModel, provider.apiService, plugin)
        def schedulePolicy = [schedulePolicyType: "SimpleSchedulePolicy", scheduleRunDays: ['Sunday'], scheduleRunTimes: ["2023-10-10T10:00:00Z"], scheduleRunFrequency: "Weekly"]

        when:
        def cronExpression = policySync.parseCronExpression(schedulePolicy)

        then:
        cronExpression == "0 10 * * 0" // At 10:00 AM, only on Sunday
    }

    def "parseCronExpression should return correct cron expression for SimpleSchedulePolicy frequency Weekly on Sundays and Wednesday"() {
        given:
        def policySync = new PolicySync(backupProviderModel, provider.apiService, plugin)
        def schedulePolicy = [schedulePolicyType: "SimpleSchedulePolicy", scheduleRunDays: ['Sunday', 'Wednesday'], scheduleRunTimes: ["2023-10-10T10:00:00Z"], scheduleRunFrequency: "Weekly"]

        when:
        def cronExpression = policySync.parseCronExpression(schedulePolicy)

        then:
        cronExpression == "0 10 * * 0,3" // At 10:00 AM, only on Sunday and Wednesday
    }

    def "parseCronExpression should return correct cron expression for SimpleSchedulePolicyV2 with hourly schedule"() {
        given:
        def policySync = new PolicySync(backupProviderModel, provider.apiService, plugin)
        def schedulePolicy = [schedulePolicyType: "SimpleSchedulePolicyV2", scheduleRunFrequency: "Hourly", hourlySchedule: [interval: 4, scheduleWindowStartTime: "2024-10-23T03:00:00Z", scheduleWindowDuration: 20]]

        when:
        def cronExpression = policySync.parseCronExpression(schedulePolicy)

        then:
        cronExpression == "0 3,7,11,15,19 * * *" //At 03:00 AM, 07:00 AM, 11:00 AM, 03:00 PM and 07:00 PM
    }

    def "parseCronExpression should return correct cron expression for SimpleSchedulePolicyV2 with daily schedule"() {
        given:
        def policySync = new PolicySync(backupProviderModel, provider.apiService, plugin)
        def schedulePolicy = [schedulePolicyType: "SimpleSchedulePolicyV2", scheduleRunFrequency: "Daily", dailySchedule: [scheduleRunTimes: ["2024-10-23T23:30:00Z"]]]

        when:
        def cronExpression = policySync.parseCronExpression(schedulePolicy)

        then:
        cronExpression == "30 23 * * *" //At 11:30 PM
    }

    def "parseCronExpression should return correct cron expression for SimpleSchedulePolicyV2 with weekly"() {
        given:
        def policySync = new PolicySync(backupProviderModel, provider.apiService, plugin)
        def schedulePolicy = [schedulePolicyType: "SimpleSchedulePolicyV2", scheduleRunFrequency: "Weekly", weeklySchedule: [scheduleRunDays: ['Sunday', 'Thursday'], scheduleRunTimes: ["2024-10-23T20:00:00Z"]]]

        when:
        def cronExpression = policySync.parseCronExpression(schedulePolicy)

        then:
        cronExpression == "0 20 * * 0,4" //At 08:00 PM, only on Sunday and Thursday
    }
}
