import logging

import azure.functions as func
import requests
import json
def algorithm():
    HTTP_PROXY="http://proxy.apac.etn.com:8080"
    HTTPS_PROXY="http://proxy.apac.etn.com:8080"
    authorisation_token="V0VIVk1Lbm9Pc2tTZUg2RXdmb0NGRGtuVEZ4SlNYck86QXp1dXdmSkxwc2JKSG9LUw=="
    algorithm_id="derms__analytics__pv_inference"
    bas_url="https://api-qa.eaton.com/" 
    authentication_url = bas_url+"oauth/accesstoken?grant_type=client_credentials"
    get_algorithm_url = bas_url+"bas/v1/algorithm/list"


    def get_status(bas_url,execution_id, access_token,proxies):
        url = bas_url+"bas/v1/algorithm/"+algorithm_id+"/"+execution_id+"/state"
        payload = {}
        headers = {
        'Accept': 'application/json',
        'Authorization': 'Bearer '+access_token,
        'Ocp-Apim-Subscription-Key': '{{apiKey}}'
        }
        response = requests.request("GET", url, headers=headers, data=payload,proxies=proxies)
        print(response.text)

    #POST api to fetch the access token

    proxies = {'http':HTTP_PROXY,'https':HTTPS_PROXY}
    payload_authentication = {}
    headers_authentication = {
    'Authorization': 'Basic '+authorisation_token,
    'Content-Type': 'application/json',
    'Ocp-Apim-Subscription-Key': '{{apiKey}}'
    }
    response_authentication = requests.request("POST", authentication_url, headers=headers_authentication, data=payload_authentication, proxies=proxies)
    resp=response_authentication.json()
    access_token=resp['access_token']
    print("access token "+access_token)

    #get api to find the algorithm id

    payload_get_algo = {}
    headers_get_algo = {
    'Accept': 'application/json',
    'Authorization': 'Bearer '+ access_token,
    'Ocp-Apim-Subscription-Key': '{{apiKey}}'
    }
    response_get_algo = requests.request("GET", get_algorithm_url, headers=headers_get_algo, data=payload_get_algo, proxies=proxies)
    resp=response_get_algo.json()
    print("result from get api ")
    print(resp)


    #put api to find the execution id

    put_execute_url = bas_url+"bas/v1/algorithm/"+algorithm_id+"/trigger"
    put_execute_payload = json.dumps({
    "data_sources": [
        {
        "source_id": "pv-data",
        "connection": {
            "connection_str": "dermsstorageasadev",
            "container_sas_token": "?sv=2022-11-02&ss=bfqt&srt=sco&sp=rwdlacupytfx&se=2023-09-27T22:45:51Z&st=2023-07-07T14:45:51Z&spr=https,http&sig=dgZIMJzHMOMtMTWJ5zTlbjL9LTwT4K6B%2BIcU9unah64%3D",
            "container_path": "trend-messages-dev/trends/3abf7828-4592-477a-bd39-2919682e4f3e/bff3d25b-a888-51d4-ba19-988940f2e013/",
            "connection_type": "ADLS",
            "data_format": "csv"
        },
        "schema": {
            "t": "int64",
            "v": "float64",
            "avg": "float64",
            "max": "float64",
            "min": "float64",
            "AsaTimestamp": "timestamp[ns, tz=UTC]",
            "Path": "string"
        }
        },
        {
        "source_id": "solar-data",
        "connection": {
            "connection_str": "dermsstorageasadev",
            "container_sas_token": "?sv=2022-11-02&ss=bfqt&srt=sco&sp=rwdlacupytfx&se=2023-09-27T22:45:51Z&st=2023-07-07T14:45:51Z&spr=https,http&sig=dgZIMJzHMOMtMTWJ5zTlbjL9LTwT4K6B%2BIcU9unah64%3D",
            "container_path": "tmp/",
            "connection_type": "ADLS",
            "data_format": "csv"
        },
        "schema": {
            "startTime": "timestamp[s, tz=UTC]",
            "solarGHI": "float64",
            "temperature": "float64",
            "weatherCode": "int32",
            "windSpeed": "float32",
            "month": "int32"
        }
        }
    ],
    "notifications": {
        "types": [],
        "emails": []
    },
    "additional_configurations": {
        "device_ids": [
        {
            "id": "10140461",
            "longitude": 73.923524,
            "latitude": 18.519828
        },
        {
            "id": "10140462",
            "longitude": 73.923524,
            "latitude": 18.519828
        }
        ],
        "freq": "5min"
    }
    })
    put_execute_headers = {
    'Content-Type': 'application/json',
    'Authorization': 'Bearer '+ access_token,
    'Ocp-Apim-Subscription-Key': '{{apiKey}}'
    }

    response_execution_id = requests.request("PUT", put_execute_url, headers=put_execute_headers, data=put_execute_payload,proxies=proxies)
    resp=response_execution_id.json()
    execution_id=resp['execution_id']
    print("execution id "+execution_id)

    #get api to find the status

    get_status_url=url = bas_url+"bas/v1/algorithm/"+algorithm_id+"/"+execution_id+"/state"
    payload = {}
    headers = {
    'Accept': 'application/json',
    'Authorization': 'Bearer '+access_token,
    'Ocp-Apim-Subscription-Key': '{{apiKey}}'
    }
    response_get_status = requests.request("GET", get_status_url, headers=headers, data=payload,proxies=proxies)
    resp=response_get_status.json()
    completed = resp['completed']
    print(resp)

    while completed != "100%":
        try:
            response_get_status = requests.request("GET", get_status_url, headers=headers, data=payload,proxies=proxies)
            resp=response_get_status.json()
            completed = resp['completed']
            print(resp)
        except:
            break   
        
    #get api to find the results

    url = bas_url+"bas/v1/algorithm/"+algorithm_id+"/"+execution_id+"/insight"

    payload = {}
    headers = {
    'Accept': 'application/json',
    'Authorization': 'Bearer '+access_token,
    'Ocp-Apim-Subscription-Key': '{{apiKey}}'
    }

    response = requests.request("GET", url, headers=headers, data=payload,proxies=proxies)
    print(response.text)
    
    
def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Python HTTP trigger function processed a request.')

    name = req.params.get('name')
    if not name:
        try:
            req_body = req.get_json()
        except ValueError:
            pass
        else:
            name = req_body.get('name')

    if name:
       # algorithm()
        return func.HttpResponse(f"Hello, {name}. This HTTP triggered function executed successfully.")

        
    else:
        return func.HttpResponse(
             "This HTTP triggered function executed successfully. Pass a name in the query string or in the request body for a personalized response.",
             status_code=200
        )
