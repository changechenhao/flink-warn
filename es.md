'''
PUT /warn
{
	"mappings":{
		"dynamic_templates": [
        {
          "strings_as_ip": {
            "match_mapping_type": "string",   
            "match": "*Ip",                        
            "mapping": {                                  
              "type": "ip"
            }
          }
        },
        {
          "strings_as_long": {
            "match_mapping_type": "string",   
            "match": "*Port",                        
            "mapping": {                                  
              "type": "long"
            }
          }
        },
        {
            "strings_as_keyword": {
                "mapping": {
                    "ignore_above": 1024,
                    "type": "keyword"
                },
                "match_mapping_type": "string"
            }
        }
      ]
	}
}
'''