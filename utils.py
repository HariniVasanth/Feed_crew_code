import logging
import math
from typing import Any, Union

import requests
from requests.adapters import HTTPAdapter, Retry
from requests.auth import HTTPBasicAuth

# *********************************************************************
# LOGGING - set of log messages
# *********************************************************************

log = logging.getLogger(__name__)

# *********************************************************************
# SETUP - of API KEY,header 
# retry session , if error 
# *********************************************************************

PAGE_SIZE = 1000
RETRIES = 3

session = requests.Session()
session.headers['Accept'] = "application/json"

MAX_RETRY = 5
MAX_RETRY_FOR_SESSION = 5
BACK_OFF_FACTOR = 1
TIME_BETWEEN_RETRIES = 1000
ERROR_CODES = (400,401,405,500, 502, 503)

### Retry mechanism for server error ### https://stackoverflow.com/questions/23267409/how-to-implement-retry-mechanism-into-python-requests-library###
# {backoff factor} * (2 ** ({number of total retries} - 1))
retry_strategy = Retry(total=25, backoff_factor=1, status_forcelist=ERROR_CODES)
session.mount('https://', HTTPAdapter(max_retries=retry_strategy))

# *********************************************************************
# FUNCTIONS - 
# get login_jwt - get auth key & assign the requests to reponse using post method
# get_coa: get chart of accounts based on segment type 

# assign the requests to reponse using get method
# append the list to coa
# In case, if error occurs retry
# *********************************************************************

# Generate jwt
def get_jwt(url: str,key:str,scopes:str,session: requests.Session=session)-> str:
    """Returns a jwt for authentication to the iPaaS APIs

    Args:
        url (str): LOGIN_URL= https://api.dartmouth.edu/api/jwt
        key (str): API_KEY

    Returns:
        _type_: str
    """

    headers = {
     'Authorization':key                  
    }

    if scopes:
        url = url + '?scope=' + scopes
    else:
        url = url

    response = session.post(url=url, headers=headers)

    if response.ok:
        response_json = response.json()
        jwt = response_json['jwt']
    else:
        response_json = response.json()
        error = response_json['Failed to obtain a jwt']
        raise Exception(error)
        

    return jwt

# Get_coa: access all employees 
def get_coa(jwt: str, url: str,session: requests.Session=session )-> "Union[list[None],list[dict]]":
    """ returns all the employees from dart_api
    Args:
        jwt (str): in .env file 
        =GL_URL (str): https://api.dartmouth.edu/general_ledger
    Returns:
        _type_: str
    """
    
    headers: dict = {'Authorization': 'Bearer ' + jwt, 'Content-Type':'application/json'}
    page_number: int = 1
    
    coa= []
    
    response = session.get(url=url, headers=headers) 
    response_list = response.json()

    #use for loop until last page:
    for page in range(len(response_list)):
        log.debug(f"Starting with page number {page}")
        
        continuation_key = response.headers.get("x-request-id")
        
        if page_number == 1:
            GL_url = f"{url}"
        else:
            GL_url = f"{url}?continuation_key={continuation_key}&pagesize={PAGE_SIZE}&page={page_number}"     
           
        response = session.get(url=GL_url, headers=headers)

        response_json = response.json()

        coa+=response_json
        current_page_number: int = page
        
        log.debug(f"Ending on loop {current_page_number}")
        log.debug(f"Records returned, so far: {len(coa)}")
        page_number+=1      

    return coa 

