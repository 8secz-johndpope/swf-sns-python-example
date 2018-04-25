import boto3 
from botocore.exceptions import ClientError 
import uuid

DOMAIN_NAME = 'SWFSampleDomain'

swf = boto3.client('swf')

def get_uuid(): 
  return uuid.uuid4()

def init_domain():  
  domain = None
  
  # First, check to see if the domain already exists and is registered. 
  registered_domains = swf.list_domains(registrationStatus='REGISTERED')
  
  for d in registered_domains['domainInfos']: 
    if(d["name"] == DOMAIN_NAME): 
      print("Domain already exists.")
      domain = d
  
  if domain == None: 
    # Register the domain for one day. 
    swf.register_domain(name=DOMAIN_NAME, description="SWFSampleDomain Domain", workflowExecutionRetentionPeriodInDays="1")
    
  return DOMAIN_NAME