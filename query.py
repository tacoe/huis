#!/usr/bin/env python
# coding=utf-8
import json
import logging
import time
import sys
import elasticsearch
import webbrowser

# onze eigen voorkeursscore van 0 tot 10 voor elk van de postcodegebieden in Amsterdam
# via de kaart op http://www.kaartenplattegrond.nl/uploads/1/3/3/4/13340474/s792817144540433370_p121_i2_w2409.gif
pclist = "pc1011: 4, pc1012: 2, pc1013: 7, pc1014: 5, pc1015: 6, pc1016: 6, pc1017: 6, pc1018: 8, pc1019: 5, pc1021: 2, pc1022: 1, pc1023: 1, pc1024: 0, pc1025: 0, pc1026: 0, pc1027: 0, pc1028: 0, pc1031: 1, pc1032: 0, pc1033: 0, pc1034: 0, pc1035: 0, pc1037: 0, pc1041: 0, pc1042: 0, pc1043: 1, pc1045: 0, pc1047: 0, pc1051: 10, pc1052: 7, pc1053: 8, pc1054: 7, pc1055: 3, pc1056: 4, pc1057: 4, pc1058: 4, pc1059: 4, pc1060: 0, pc1061: 0, pc1062: 0, pc1063: 0, pc1064: 0, pc1065: 0, pc1066: 0, pc1067: 0, pc1068: 0, pc1069: 0, pc1071: 8, pc1072: 9, pc1073: 9, pc1074: 9, pc1075: 7, pc1076: 4, pc1077: 5, pc1078: 3, pc1079: 4, pc1081: 0, pc1082: 0, pc1083: 0, pc1091: 6, pc1092: 5, pc1093: 4, pc1094: 3, pc1095: 2, pc1096: 3, pc1097: 5, pc1098: 6, pc1102: 0, pc1103: 0, pc1104: 0, pc1105: 0, pc1106: 0, pc1107: 0, pc1108: 0, pc1109: 0"

# environment initialization
reload(sys)
sys.setdefaultencoding('utf-8')
es = elasticsearch.Elasticsearch()

q1 = {
    "from": 0,
    "size": 100000,
    "fields": [
        "_source"
    ],
    "query" : {
        "filtered": {
            "query": {
                "match_all": {}
            },
            "filter": {
                "and": [
                {
                    "term" : {
                        "Soort-aanbod": "appartement"
                    }
                },
                {
                    "range": {
                        "Koopprijs" : {
                            "from" : 400000,
                            "to" : 600000
                        }
                    }
                },
                {
                    "range": {
                        "Woonoppervlakte" : {
                            "from" : 95,
                            "to" : 120
                        }
                    }
                }
                ]
            }
        }
    },
    "script_fields" : {
        "Meterprijs" : {
            "script" : """

            Math.round(doc['Koopprijs'].value / doc['Woonoppervlakte'].value)

            """
        },
        "Buurtscore" : {
            "script" : """
            map = [""" + pclist + """];
            idx = 'pc' + doc['Postcode'].value.substring(0,4);
            val = map.get(idx);
            if(val) return val; else return 0;
            """
        }
    },
    "sort": {
        "_script": {
            "script": """
            map = [""" + pclist + """];
            mp = doc['Koopprijs'].value / doc['Woonoppervlakte'].value;
            idx = 'pc' + doc['Postcode'].value.substring(0,4);
            bs = map.get(idx);
            if(!bs) bs = 0;

            // basisscore: buurt
            // bonus:
            // TODO - begane grond? +1
            // TODO - eigen grond? +1
            // TODO - balkon/dakterras/tuin? zo niet, -5
            // - aantal kamers = 3? dan +2

            val = bs;
            if(doc['AantalKamers'].value == 3) val += 2;

            return val;
            """,
            "type": "number",
            "order": "desc"
        }
    }
    ,
    "aggs" : {
        "price_stats" : { "stats" : { "field" : "Koopprijs" } } ,
        "surface_stats" : { "stats" : { "field" : "Woonoppervlakte" } } ,
        "price_histogram" : { "histogram" : { "field" : "Koopprijs", "interval": 10000 } },
        "room_histogram" : { "histogram" : { "field" : "AantalKamers", "interval": 1 } }
    },
}
#}

result = es.search(index='funda-current', body=q1)

print "\nTotal results: " + str(result["hits"]["total"]) + "\n"
#print json.dumps(result, indent=2, sort_keys=True)

avg_price = result["aggregations"]["price_stats"]["avg"]
avg_surface = result["aggregations"]["surface_stats"]["avg"]
price_per_m2 = avg_price / avg_surface
print "Gemiddelde koopprijs: € " + str(int(avg_price))
print "Gemiddeld oppervlak:    " + str(int(avg_surface)) + " m2"
print "Prijs per m2:         € " + str(int(price_per_m2)) + "/m2"

print "Price distribution:"
for bucket in result["aggregations"]["price_histogram"]["buckets"]:
    print str(bucket["key"]) + ": " + str(bucket["doc_count"])

print "Aanbod per aantal kamers:"
for bucket in result["aggregations"]["room_histogram"]["buckets"]:
    print str(bucket["key"]) + ": " + str(bucket["doc_count"])

c = 0
print "Adres ----------------------------- Prijs --- Opp -- Br - K - m2prs - URL -------------------------------------------------------------------------------"
for hit in result["hits"]["hits"]:
    print hit["_source"]["Adres"][:35].ljust(35, ".") + \
        " €" + str(hit["_source"]["Koopprijs"]).rjust(7) + "  " + \
        str(hit["_source"]["Woonoppervlakte"]).rjust(3) + " m2 " + \
        str(hit["fields"]["Buurtscore"][0]).rjust(3) + "  " + \
        str(hit["_source"]["AantalKamers"]) + "  " + \
        " €" + str(hit["fields"]["Meterprijs"][0]).rjust(5) + "  " + \
        hit["_source"]["URL"]
    #if c < 10: webbrowser.open_new_tab(hit["_source"]["URL"])
    #c = c + 1
