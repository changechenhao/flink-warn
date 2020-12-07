
1. nc -l -p 9999


'''
{ "ruleId": 1, "ruleState": "ACTIVE", "groupingKeyNames": ["paymentType"], "unique": [], "aggregateFieldName": "paymentAmount", "aggregatorFunctionType": "SUM","limitOperatorType": "GREATER","limit": 200, "windowMinutes": 1}

{ "ruleId": 1, "ruleState": "ACTIVE", "groupingKeyNames": ["paymentType"], "defaultGroupingKeyNames": ["deviceIp", "srcIp", "dstIp", "srcPort", "dstPort", "eventType", "eventSubType", "logType"], "unique": [], "aggregateFieldName": "paymentAmount", "aggregatorFunctionType": "SUM","limitOperatorType": "GREATER","limit": 200, "windowMinutes": 1}



{ "ruleId": 1, "ruleState": "ACTIVE", "groupingKeyNames": ["tag"], "unique": [], "aggregateFieldName": "paymentAmount", "aggregatorFunctionType": "tag","limitOperatorType": "GREATER","limit": 200, "windowMinutes": 1}

{ "ruleId": 1, "warnName" : "IPS攻击", "levle": 1,"ruleState": "ACTIVE", "groupingKeyNames": [], "conditionList": [{"fieldName": "logType", "operatorType": "EQUAL_STR", "value": "ips"}], "defaultGroupingKeyNames": ["deviceIp", "srcIp", "dstIp", "srcPort", "dstPort", "eventType", "eventSubType", "logType", "srcCountry", "dstCountry", "srcProProvince", "dstProProvince", "srcCity", "dstCity"], "unique": [], "aggregateFieldName": "COUNT_FLINK", "aggregatorFunctionType": "SUM","limitOperatorType": "GREATER_EQUAL","limit": 3, "windowMinutes": 1}

{ "ruleId": 2, "warnName" : "病毒攻击", "levle": 1,"ruleState": "ACTIVE", "groupingKeyNames": [], "conditionList": [{"fieldName": "logType", "operatorType": "EQUAL_STR", "value": "av"}], "defaultGroupingKeyNames": ["deviceIp", "srcIp", "dstIp", "srcPort", "dstPort", "eventType", "eventSubType", "logType", "srcCountry", "dstCountry", "srcProProvince", "dstProProvince", "srcCity", "dstCity"], "unique": [], "aggregateFieldName": "COUNT_FLINK", "aggregatorFunctionType": "SUM","limitOperatorType": "GREATER_EQUAL","limit": 3, "windowMinutes": 1}

'''