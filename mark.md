
1. nc -l -p 9999


'''
{ "ruleId": 1, "ruleState": "ACTIVE", "groupingKeyNames": ["paymentType"], "unique": [], "aggregateFieldName": "paymentAmount", "aggregatorFunctionType": "SUM","limitOperatorType": "GREATER","limit": 200, "windowMinutes": 1}{ "ruleId": 1, "ruleState": "ACTIVE", "groupingKeyNames": ["paymentType"], "defaultGroupingKeyNames": ["deviceIp", "srcIp", "dstIp", "srcPort", "dstPort", "eventType", "eventSubType", "logType"], "unique": [], "aggregateFieldName": "paymentAmount", "aggregatorFunctionType": "SUM","limitOperatorType": "GREATER","limit": 200, "windowMinutes": 1}
'''