from datetime import datetime

DATABASE_COLUMNS = [
    "protocol","ip_request", "ip_response", "a_record", "timestamp_request", 
    "timestamp_response", "response_type", "country_request", 
    "asn_request", "prefix_request", "org_request", "country_response", 
    "asn_response", "prefix_response", "org_response", 
    "country_arecord", "asn_arecord", "prefix_arecord", "org_arecord"
]



def timestampTyper(val:str):
    try:
        return datetime.strptime(val,"%Y-%m-%d %H:%M:%S.%f")
    except:
        return None

def floatTyper(val:str):
    try:
        return float(val)
    except:
        return None

fieldtypers = {
    "timestamp_request":timestampTyper,
    "timestamp_response":timestampTyper,
    "asn_request":floatTyper,
    "asn_response":floatTyper,
    "asn_arecord":floatTyper
}