import hashlib
import json

import requests
import urllib3

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)



def handle(req: bytes):
    """handle a request to the function
    Args:
        req (str): request body
    """
    args = json.loads(req)
    url = f'https://{args["host"]}/cdmi/cdmi_objectid/{args["fileId"]}'
    headers = {"X-Auth-Token": args["accessToken"]}

    resp = requests.get(url, headers=headers, stream=True, verify=False)
    if resp.status_code != 200:
        return resp.text

    content_hash = hashlib.md5()
    chunks = resp.iter_content(chunk_size=134)
    chunk = next(chunks)

    run = int.from_bytes(chunk[64:67], byteorder='little')
    subrun = int.from_bytes(chunk[68:71], byteorder='little')
    telescope = int.from_bytes(chunk[72:75], byteorder='little')
    year = int.from_bytes(chunk[76:79], byteorder='little')
    month = int.from_bytes(chunk[80:83], byteorder='little')
    day = int.from_bytes(chunk[84:87], byteorder='little')
    datatype = chunk[90:91].decode('utf-8')
    format_ = chunk[92:100].split(b'\0',1)[0].decode('utf-8')
    source = chunk[101:133].split(b'\0',1)[0].decode('utf-8')

    return json.dumps({"xattrs": {"run" : run, "subrun" : subrun, "telescope": telescope, "year": year, "month": month, "day" : day, "datatype" : datatype, "format": format_, "source" : source}})
