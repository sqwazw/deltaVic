# #
# # Cloud specific wrapper code for interacting with aws cloud environment and resources.
# #

# import os, json, logging, base64
# from collections import OrderedDict

# import boto3
# from botocore.exceptions import ClientError, WaiterError
# from botocore.config import Config
        
# from email.mime.text import MIMEText
# from email.mime.multipart import MIMEMultipart
# from email.mime.base import MIMEBase

# # from .fileUtils import FileObj
# from .dbTable import Supplies

# # logging.getLogger("botocore").setLevel(logging.WARNING)
# # logging.getLogger("s3transfer").setLevel(logging.WARNING)
# # logging.getLogger("boto3").setLevel(logging.WARNING)

# class ScrtMgr():

#     # Have one secret to rule them all? Or just branch the json with a secret to accommodate the many? 

#     SECRET_NAME = "yourSecret"
    
    
#     def __init__(self):
#         # Create a Secrets Manager client
#         session = boto3.session.Session()
#         self.client = session.client("secretsmanager")
        
#     def get_secret(self, name) -> json:
#         _response = self.client.get_secret_value(SecretId=name)
       
#         sStr = "SecretString"
#         sBin = "SecretBinary"
#         _secret = _response[sStr] if sStr in _response else base64.b64decode(_response[sBin])
#         return json.loads(_secret)

#     def recordSecret(self, name, value):
#         self.client.put_secret_value(SecretId=name, SecretString=value)

#     def test_secrets():
#         pass #TODO: Do it.

# class ScrtMgr2(dict):

#     # One secret to rule them all.
#     SECRET_NAME="hexadecimalYuck"
#     # sub-dicts:
#     #   -list
#     #   -of
#     #   -top
#     #   -level
#     #   -json
#     #   -nestings
    
#     def __init__(self):
#         # Create a Secrets Manager client
#         session = boto3.session.Session()
#         self.client = session.client("secretsmanager")
#         _secret = self.client.get_secret_value(SecretId=self.SECRET_NAME)
#         [self.update(dict) for dict in _secret]
        
#     def get(self, nesting):
#         try:
#             twigs = nesting.split('.')
#             _dicty = self
#             for twig in twigs:
#                 _dicty = _dicty[twig]
#             return _dicty
#         except Exception as ex:
#             raise Exception(f"Could not obtain secret subdict for nesting {nesting}")
    
#     def write(self):
#         logging.info(json.dumps(self))
#         self.client.put_secret_value(SecretId=self.SECRET_NAME, SecretString=json.dumps(self))

# class S3():

#     S3_BUCKET_KEY_DEPLOY = "deploy"
    
#     # def __init__(self, s3_bucket_name: str, aws_credential_profile: str = None, aws_access_key_id: str = None, aws_secret_access_key: str = None) -> None:
#     def __init__(self, target:object) -> None:

#         if isinstance(target, str): # it's a bucket name, we assume permissions exist
#             self.session = boto3.Session()
#             self.bucketName = target
#         elif isinstance(target, dict): # We are being passed a dict of standard structure
#             axsKey = target["aws_access_key_id"]
#             scrtAxsKey = target["aws_secret_access_key"]
#             self.bucketName = target["bucket_name"]
#             self.session = boto3.Session(axsKey, scrtAxsKey)

#         self.s3_resource = self.session.resource("s3")
#         self.s3_bucket = self.s3_resource.Bucket(self.bucketName)

#     # def create_key(self, file_name, root_directory):
#     #     s3_bucket_key = os.path.sep.join(file_name.split(os.path.sep)[len(root_directory.split(os.path.sep)):])
#     #     s3_bucket_key = s3_bucket_key.replace(os.path.sep, S3.S3_BUCKET_KEY_SEPERATOR)
#     #     return self.add_application_key(s3_bucket_key)

#     # def add_application_key(self, s3_bucket_key: str):
#     #     s3_bucket_key_parts = s3_bucket_key.split(S3.S3_BUCKET_KEY_SEPERATOR)
#     #     if s3_bucket_key_parts[0] != S3.S3_BUCKET_KEY_DEPLOY:
#     #         s3_bucket_key_parts.insert(0, S3.S3_BUCKET_KEY_DEPLOY)
#     #     return S3.S3_BUCKET_KEY_SEPERATOR.join(s3_bucket_key_parts)
    
#     def presignLink(self, s3_bucket_key):
#         try:
#             url = self.s3_bucket.meta.client.generate_presigned_url(
#                 ClientMethod='get_object',
#                 Params={'Bucket': self.bucketName, 'Key': s3_bucket_key},
#                 ExpiresIn=3600 # one hour in seconds, increase if needed
#             )
#         except ClientError as e:
#             logging.error(e)
#             return None
#         return url
    
#     def promiseLink(self, s3_bucket_key):
#         try:
#             url = self.s3_bucket.meta.client.generate_presigned_url(
#                 "put_object",
#                 Params={"Bucket": self.bucketName, 
#                         "Key":s3_bucket_key, 
#                         "ContentType":"application/octet-stream"},
#                         ExpiresIn=3600
#             )
#         except ClientError as e:
#             logging.error(e)
#             return None
#         return url
    
#     def upload_file(self, file_name:str, s3_bucket_key: str):
#         response = self.s3_bucket.upload_file(file_name , s3_bucket_key)

#     def download_file(self, s3_bucket_key: str, file_name:str):
#         self.s3_bucket.download_file(s3_bucket_key, file_name)

#     # def copyB2B(self, srcObj, tgtObj):
#     #     self.s3_bucket.copy(srcObj, tgtObj)

#     def get_file_name_list(self, s3_bucket_key_filter: str):

#         file_name_list = []
#         for object in self.s3_bucket.objects.filter(Prefix=s3_bucket_key_filter):
#             file_name_list.append(object.key)

#         return file_name_list
    
#     def listSupsForPrefix(self, fmtStr, type):
#         prefix = fmtStr % type
#         return self.listFilesForPrefix(prefix) if type == Supplies.INC else self.listDirsForPrefix(prefix)

#     def listDirsForPrefix(self, prefix):
#         #logging.info("{}--{}".format(self.s3_bucket.name, prefix))
#         response = self.s3_bucket.meta.client.list_objects_v2(Bucket=self.s3_bucket.name , Prefix=prefix, Delimiter='/')
#         #logging.info(response)
#         if 'CommonPrefixes' in response.keys():
#             return [obj['Prefix'][:-1] for obj in response['CommonPrefixes']] #[:-1] is to remove the trailing slash
#         return None
    
#     def listFilesForPrefix(self, prefix):
#         response = self.s3_bucket.meta.client.list_objects_v2(Bucket=self.s3_bucket.name , Prefix=prefix)
#         logging.info(f"listFilesForPrefix response: {response}")
#         if 'Contents' in response.keys():
#             objs = [obj['Key'] for obj in response['Contents']]
#             return [o for o in objs if not o.endswith('/')] # exclude the head folder if it is present at index zero
#         return None
    
#     # def dlSupplyFilesByPrefix(self, prefix, mntPath):
#     #     dlFiles = self.listFilesForPrefix(prefix)
#     #     # dlFiles = [d for d in dlFiles if any(name in d for name in ('air','thing'))]
#     #     logging.info(f"dlFiles:{dlFiles}")
#     #     files = []
#     #     for f in dlFiles:
#     #         logging.debug("%s**%s" % (f, os.path.basename(f)))
#     #         localPath = os.path.join(mntPath, os.path.basename(f))
#     #         self.download_file(f, localPath)
#     #         size = os.stat(localPath).st_size
#     #         files.append(FileObj(localPath, size))
        
#     #     return sorted(files, key=lambda f:f.size, reverse=True)
    
            
# class Emailer:
#     def __init__(self, recipients, subject, textBody, htmlBody):
#         self.sender = 'VicmapLoad<vicmap@delwp.vic.gov.au>'#Vicmap
#         self.recipients = recipients
#         self.subject = subject
#         self.textBody = textBody
#         self.htmlBody = htmlBody
        
#         self.bucket = None
#         self.attachments = [] # this is to the path within the bucket

#         self.charset='UTF8'

#         self.tmpDir = '/mnt/tmp'
#         try:
#             if not os.path.isdir(self.tmpDir):
#                 os.makedirs(self.tmpDir)
#         except Exception as ex:
#             logging.warning("Could not create temp dir in mounted efs, (probably isn't attached).")        
    
#     def attach(self, bucket:S3, filePath:str):
#         self.bucket = bucket
        
#         #copy the file locally, b64 encode the contents into a mimePart. 
#         localFile = '%s/%s' % (self.tmpDir, os.path.basename(filePath))
#         # logging.info("{} {}".format(filePath, localFile))
#         try:
#             self.bucket.download_file(filePath, localFile) # logs "ERROR - Not Found" if file is not there. Error is thrown.
#         except ClientError as ce:
#             if ce.response.code == 404:
#                 raise Exception(f"404: File not found in s3 bucket: {filePath}")
#             else:
#                 raise Exception(f"Something went wrong attaching the file from the s3 bucket: {filePath}")
        
#         self.attachments.append(localFile)

#     def send(self):
#         try:
#             client = boto3.client('ses')
#             logging.info('Email sending...')
#             response = self.sendSimple(client) if len(self.attachments) == 0 else self.sendRaw(client)
#             logging.info('Email sent successfully! Message ID: {}'.format(response['MessageId']))
#             return True
#         except ClientError as e:
#             logging.error(e.response['Error']['Message'])
#             logging.error(e.response)
#             logging.error(e)
#             # Change the result if something goes wrong.
#             return False

#     def sendSimple(self, sesClient):
#         return sesClient.send_email(
#             Source=self.sender,
#             Destination={'ToAddresses': self.recipients},
#             Message={
#                 'Subject': { 'Data': self.subject, 'Charset': self.charset },
#                 'Body': {
#                     'Text': { 'Data': self.textBody, 'Charset': self.charset },
#                     'Html': { 'Data': self.htmlBody, 'Charset': self.charset }
#                 }
#             }
#         )
    
#     def sendRaw(self, sesClient):
#         msg = MIMEMultipart('alternative')
#         msg.set_charset(self.charset)
#         msg['Subject'] = self.subject
#         msg['From'] = self.sender
#         msg['Reply-to'] = self.sender
#         msg['To'] = self.recipients

#         logging.info(self.attachments)
#         for localFile in self.attachments:
#             part = MIMEBase('application', "octet-stream")
#             logging.info("opening file...")
#             with open(localFile, 'rb') as fp:
#                 part.set_payload(base64.encodebytes(fp.read()).decode())
#             os.remove(localFile) # cleanup

#             part.add_header('Content-Transfer-Encoding', 'base64')
#             part.add_header('Content-Disposition', 'attachment; filename="%s"' % os.path.basename(localFile))
#             msg.attach(part)
        
#         msg.attach(MIMEText(self.textBody, 'plain'))
#         msg.attach(MIMEText(self.htmlBody, 'html')) # last one attached is the preferred.
        
#         return sesClient.send_raw_email(RawMessage={"Data" : msg.as_bytes()})
    
#     @staticmethod
#     def vMail(context, status, msg, err=None, errStack=None, errDsets=None):
#         acctDict = {'####':'dev','####':'uat','####':'prd'}
        
#         emlTos = ["person@place.com","person2@place.com"]
#         fnName = context.function_name
#         fnAcct = context.invoked_function_arn.split(":")[4] #ID by acctId
#         fnLogs = f'aws logs get-log-events --log-group-name {context.log_group_name} --log-stream-name {context.log_stream_name}  --query "events[].message"'
#         emlSubj = f'Hello-{acctDict[fnAcct]}-{status}'#- {msg}'
        
#         errText,stackText,dsetErrText = None,None,None
#         errHtml,stackHtml,dsetErrHtml = None,None,None
#         if err: 
#             errText = f"{chr(10)}{err}"
#             errHtml = f"<br>{err}"
#         if errStack: 
#             stackText = f"{chr(10)}{chr(10).join(errStack.split(chr(10)))}"
#             stackHtml = f"<br>{'<br>'.join(errStack.split(chr(10)))}"
#         if errDsets: 
#             dsetErrText = f"{chr(10)}{chr(10).join([err.report() for err in errDsets])}"
#             dsetErrHtml = f"<br><ul><li> {'</li><li>'.join([err.report() for err in errDsets])}</li></ul>"
#         emlText = f"{msg}{chr(10)}{errText or 'None'}{chr(10)}{stackText or 'None'}{chr(10)}{dsetErrText or 'None'}"
#         detailDict = OrderedDict([("AWS acct#", fnAcct),
#                                 ("Lambda Name", fnName),
#                                 ("Message",msg),
#                                 ("Error", errHtml or 'None'),
#                                 ("ErrStack", stackHtml or 'None'),
#                                 ("DsetErrs", dsetErrHtml or 'None'),
#                                 ("Logs CLI", fnLogs)])
#         detail = '<br>'.join(["<b>{}:</b> {}".format(key, val) for key,val in detailDict.items()])
#         emlHtml = '<html><head></head><body><h1>{}</h1><p>{}</p></body></html>'.format(msg, detail)
        
#         Emailer(emlTos, emlSubj, emlText, emlHtml).send()
        
# class Lambda():
#     def __init__(self, lambdaName:str, type:str, pDict:dict):
#         self.name = lambdaName
#         self.type = type # "Event"=async, "RequestResponse"=synchronously, "DryRun"=validate & test execution role
#         self.payload = json.dumps(pDict)

#     @staticmethod
#     def invokeAsync(fnName, event):
#         client = boto3.client('lambda')
#         response = client.invoke(FunctionName=fnName, InvocationType="Event", Payload=json.dumps(event))
#         logging.debug(response)
    
#     def invoke(self):
#         #botocore.config.Config tells the RequestResponse wait time and retries.
#         config = Config(read_timeout=1000, connect_timeout=1000, retries={'max_attempts': 0})
#         lambda_client = boto3.client("lambda", config=config)
#         logging.info(self.payload)
#         response = lambda_client.invoke(FunctionName=self.name, InvocationType=self.type, Payload=self.payload)
#         logging.debug("response: %s" % response)
        
#         status_code = response["StatusCode"]
#         payload = response["Payload"].read()
#         # logging.info(f"payload: {payload}")
#         pJson = json.loads(payload)
#         if "body" in pJson: # this is the body item being sent back by the executed lambda. Our payload.
#             pBody = json.loads(pJson["body"]) # depending on the return payload, this sometimes breaks, sometimes not.
#             if 'error' not in pBody:
#                 logging.info("HOORAY! Successfully ran lambda: %s." % self.name)

#             if status_code == 200:
#                 [logging.info("{}: {}".format(key, val)) for key,val in pBody.items()]
#             else:
#                 logging.error("STATUS CODE was not 200.")

#         elif "errorMessage" in pJson:
#             errMsg, errType, stackTrace = pJson["errorMessage"], pJson["errorType"], '\n'.join(pJson["stackTrace"])
#             logging.error(f"Error occurred in lambda.\nErrMsg: {errMsg}\nErrType:{errType}\nStackTrace:\n{stackTrace}")
#             return False
#         else:
#             logging.error("neither body nor errorMessage in jsonResponse: {}".format(pJson))
#             return False

#         return True

# class CloudFormer():
#     def __init__(self, stackName, template, tags, params):
#         self.session = boto3.Session()
#         self.cf = self.session.resource("cloudformation")
#         self.client = self.cf.meta.client
#         # self.client = boto3.client('cloudformation')

#         self.stackName = stackName
#         self.template = template
#         self.templateBody = None
#         self.tags = tags
#         self.params = params

#         self.status = None
#         self.stackLastRun = None
#         self.result = None
    
#     def __str__(self):
#         return "name:%s status:%s lastRun:%s" % (self.stackName, self.status, self.stackLastRun)

#     ###########################################################################
#     def deploy(self):
#         self.prepare()
#         try:
#             if not self.validate():
#                 raise Exception("CloudFormation template did not validate")
            
#             if self.status in ("CREATE_FAILED", "UPDATE_FAILED", "ROLLBACK_COMPLETE"):
#                 logging.info(f"Previous create/update failed. Deleting stack before recreate {self.stackName}")
#                 self.delete()
#                 self.setStatus() # reset the status
            
#             if self.status in ("CREATE_COMPLETE", "UPDATE_COMPLETE", "UPDATE_ROLLBACK_COMPLETE"):
#                 self.update()
#                 self.result = "updated" # if this is recieved by deploy script it will update the code next.
#             elif self.status in ("DELETE_COMPLETE", None):
#                 self.create()
#                 self.result = "created"
#             else:
#                 # UPDATE_ROLLBACK_FAILED, DELETE_IN_PROGRESS
#                 raise Exception(f"Unaccounted for cloud formation status encountered: {self.status}")
        
#         except (ClientError) as ce:
#             logging.error(f"Client Error during cloud formation: {self.template}\nMsg: {ce.response['Error']['Message']}")
        
#         #check
#         if not self.setStatus():
#             raise Exception(f"No status: {self.status}. No Stack?")
#         if "FAILED" in self.status:
#             logging.error(f"Cloud formation status ({self.status}) for  {self.stackName} ...")
#             self.result = "failed"
        
#         if not self.result: raise Exception(f"CloudFormer class ended deploy without a result! status: {self.status}")

#     def prepare(self):
#         self.stackLastRun = None
#         self.stack = self.cf.Stack(self.stackName)
#         if self.setStatus():
#             self.stackLastRun = self.stack.last_updated_time
#             if self.stackLastRun is None:
#                 self.stackLastRun = self.stack.creation_time

#         with open(self.template, "r") as file:
#             self.templateBody = file.read()

#     def setStatus(self):
#         try:
#             self.stack = self.cf.Stack(self.stackName) # refresh the stack status
#             self.status = self.stack.stack_status
#             logging.info(self.stack.stack_status)
#             return True
#         #except (ValidationError) as err:?
#         except ClientError as err:
#             self.status = None
#             resp = err.response['Error']
#             logging.warn("threw ClientError on stack_status(%s): %s" % (resp['Code'], resp['Message']))
#             return False
    
#     ###########################################################################
#     def validate(self):
#         logging.info("Validating Cloudformation {0} ...".format(self.template))
#         try:
#             response = self.client.validate_template(TemplateBody=self.templateBody)
#             # print(response)
#             return True
#         except ClientError as e:
#             logging.error("Template not valid: {0}".format(self.template))
#             logging.error(e)
#             return False

#     def delete(self):
#         logging.info("Deleting stack {0} ...".format(self.stackName))
#         self.client.delete_stack(StackName=self.stackName)
#         self.wait("stack_delete_complete")

#     def create(self):
#         logging.info("Creating stack {0} ...".format(self.stackName))
#         if 'iam' in self.stackName:
#             self.client.create_stack(StackName=self.stackName, TemplateBody=self.templateBody, Tags=self.tags, Parameters=self.params, OnFailure="DO_NOTHING", Capabilities=['CAPABILITY_NAMED_IAM'])
#         else:
#             self.client.create_stack(StackName=self.stackName, TemplateBody=self.templateBody, Tags=self.tags, Parameters=self.params, OnFailure="DO_NOTHING") #, Capabilities=['CAPABILITY_AUTO_EXPAND']
#         self.wait("stack_create_complete")

#     def update(self):
#         logging.info("Updating Stack {0} ...".format(self.stackName))
#         try:
#             if 'iam' in self.stackName:
#                 self.client.update_stack(StackName=self.stackName, TemplateBody=self.templateBody, Tags=self.tags, Parameters=self.params, Capabilities=['CAPABILITY_NAMED_IAM'])
#             else:
#                 self.client.update_stack(StackName=self.stackName, TemplateBody=self.templateBody, Tags=self.tags, Parameters=self.params)
#             self.wait("stack_update_complete")
#         except (ClientError) as ce:
#             if ce.response['Error']['Message'] == 'No updates are to be performed.':
#                 logging.info("No changes")
#             else:
#                 raise Exception(ce)

#     def wait(self, status):
#         try:
#             waiter = self.client.get_waiter(status)
#             waiter.wait(StackName=self.stackName, WaiterConfig={'Delay': 10, 'MaxAttempts': 30})
#         except (WaiterError) as e:
#             logging.error("Error updating WaiterError: {0}".format(self.template))
#             logging.error(e)

# class StateMachine():
#     def __init__(self):
#         self.client = boto3.client("stepfunctions")
#         self.stepFns = ScrtMgr().get_secret(ScrtMgr.SECRET_NAME_STEP_FNS)
    
#     def invoke(self, sm_name:str, event:dict={}):
#         if sm_name not in self.stepFns:
#             raise Exception("Unknown state machine requested: {}".format(sm_name))
#         # if "COMPLETE" not in event:
#         event.update({"COMPLETE":False})

#         inPyld = json.dumps(event, default=str)
#         response = self.client.start_execution(stateMachineArn=self.stepFns[sm_name], input=inPyld) # name=sm_name, traceHeader='string'
#         # logging.info(f"response: {response}")
#         # status_code = response["ResponseMetadata"]["HTTPStatusCode"]
#         return response['executionArn'] # True if response else False

#     def checkArn(self, sm_name:str, execArn:str):
#         #https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/stepfunctions.html#SFN.Client.list_executions
#         if sm_name not in self.stepFns:
#             raise Exception("Unknown state machine requested: {}".format(sm_name))
        
#         response = self.client.list_executions(stateMachineArn=self.stepFns[sm_name])
#         arr = [sfn for sfn in  response['executions'] if sfn['executionArn']==execArn]
#         return arr[0]['status'] if arr else None

#     def running(self):
#         runners = {}
#         # arn:aws:states:ap-southeast-2:040147941756:execution:vml-sm-fme-worker:226f0182-16b6-48fb-b92b-ee098f64c10a
#         # stateMachineArn='arn:aws:states:ap-southeast-2:*:execution:vml-*:*', 
#         logging.info(self.client.list_executions(statusFilter='RUNNING'))
#         for sfn in self.stepFns:
#             response = self.client.list_executions(stateMachineArn=self.stepFns[sfn], statusFilter='RUNNING')
#             execs = []
#             [execs.append(exec) for exec in response['executions']]
#             runners.update({sfn:execs})
#         return runners

#     def isRunning(self, sm_name:str, subMachine:str=None, sup:str=None):
#         if sm_name not in self.stepFns:
#             raise Exception("Unknown state machine requested: {}".format(sm_name))
        
#         _execs = []
#         response = self.client.list_executions(stateMachineArn=self.stepFns[sm_name], statusFilter='RUNNING')
#         # execs = response['executions']
#         for sfn in response['executions']:
#             _execs.append(sfn)
#         logging.debug(f"{len(_execs)} sfns of {sm_name} are running")
        
#         _sfns = set()
#         checkers = {}
#         if subMachine: checkers.update({'target':subMachine})
#         if sup: checkers.update({'sup':sup})
#         if not checkers:
#             _sfns = [sfn['executionArn'] for sfn in _execs]
#         else:
#             # test each returned machine to see if the input subMachine/sup matches
#             for sfn in _execs:
#                 rsp = self.client.describe_execution(executionArn=sfn['executionArn'])
#                 input = json.loads(rsp['input'])
#                 inputBody = input['body'] # all my events have a body now. ww, 16/10/22.
#                 match = True
#                 for attr,val in checkers.items():
#                     if attr in inputBody:
#                         if inputBody[attr] != val: match = False
#                         else: logging.debug(f"{inputBody[attr]} matched {val} in {attr}")
#                     else:
#                         match = False
#                 if match:
#                     _sfns.add(sfn['executionArn'])
#             logging.debug(f"{len(_sfns)} running sfns match {checkers}")
        
#         return _sfns
    
# if __name__ == '__main__':
#     # #test emailer
#     # eml = Emailer('person@place.com', 'test', 'texty test', '<html><head></head><body><p>Hello,<br>htmly test.</p></body></html>')
#     # eml.attach(S3('vicmap-load-int'), 'supply/VOTS/spi_incr_20200725_01.gz')
#     # eml.send()

#     #test cloudformation
#     cFormer = CloudFormer("<cf-handle-name>", "<yaml-path-to-template>", {}, {})
#     cFormer.prepare()
#     print(cFormer)
#     #cFormer.deploy()
