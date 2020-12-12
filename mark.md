
1. nc -l -p 9999


'''
{ "ruleId": 1, "ruleState": "ACTIVE", "groupingKeyNames": ["paymentType"], "unique": [], "aggregateFieldName": "paymentAmount", "aggregatorFunctionType": "SUM","limitOperatorType": "GREATER","limit": 200, "windowMinutes": 1}

{ "ruleId": 1, "ruleState": "ACTIVE", "groupingKeyNames": ["paymentType"], "defaultGroupingKeyNames": ["deviceIp", "srcIp", "dstIp", "srcPort", "dstPort", "eventType", "eventSubType", "logType"], "unique": [], "aggregateFieldName": "paymentAmount", "aggregatorFunctionType": "SUM","limitOperatorType": "GREATER","limit": 200, "windowMinutes": 1}



{ "ruleId": 1, "ruleState": "ACTIVE", "groupingKeyNames": ["tag"], "unique": [], "aggregateFieldName": "paymentAmount", "aggregatorFunctionType": "tag","limitOperatorType": "GREATER","limit": 200, "windowMinutes": 1}

{ "ruleId": 2, "ruleState": "ACTIVE", "groupingKeyNames": ["tag"], "defaultGroupingKeyNames": ["deviceIp", "srcIp", "dstIp", "srcPort", "dstPort", "eventType", "eventSubType", "logType"], "unique": [], "aggregateFieldName": "COUNT_FLINK", "aggregatorFunctionType": "SUM","limitOperatorType": "GREATER","limit": 3, "windowMinutes": 1}
'''