import logging

from .utils_db import DB
from .utils_api import ApiUtils
from .dbTable import LyrReg
from .utils import Supplies

class Setup():
  def __init__(self, configgy, stage):
    self.configgy = configgy # needed for the .write(). self.cfg.meta.write()??
    self.cfg = configgy.cfg[stage]
    self.db = None
    self.dbSchemas = None

  def status(self, stage):
    logging.info(f"testing config file has all required keys for '{stage}' instance: {self.preReqs()}")
    logging.info(f"dbCnxn: {self.cfg['dbUser']}:{self.cfg['dbPswd']}@{self.cfg['dbHost']}:{self.cfg['dbPort']}/{self.cfg['dbName']}")
    logging.info(f"Email: {self.cfg['email']}")
    logging.info(f"ClientId: {self.cfg['client_id']}")
    logging.info(f"ApiKey: {self.cfg['api_key']}")
    logging.info(f"deltaVic endpoint: {self.cfg['baseUrl']}")

    # for key, val in self.cfg.items():
    #   logging.info(f"{key}: {val}")#, self.cfg['key'])
  
  def core(self):
    self.db = DB(self.cfg['dbHost'], self.cfg['dbPort'], self.cfg['dbName'], self.cfg['dbUser'], self.cfg['dbPswd'])
    sqlUnretired = "update vm_meta.layer_registry set active=false where identity like 'vlat%' or identity like 'vtt%'"
    self.db.execute(sqlUnretired)
    sqlNotMisc = "update vm_meta.layer_registry set active=false where sup='MISC' and not "
    sqlNotMisc += "(" + ' or '.join(f"identity like '{sch}%'" for sch in ['vmadmin','vmreftab','vmfeat']) + ")"
    logging.info(sqlNotMisc)
    # self.db.execute(sqlNotMisc)

  def run(self):
    self.preReqs()
    # test connection - exception will be thrown if it is bad
    self.db = DB(self.cfg['dbHost'], self.cfg['dbPort'], self.cfg['dbName'], self.cfg['dbUser'], self.cfg['dbPswd'])
    self.checkDb_Meta()
    self.checkApiClient()
    self.seedDsets()
    self.createVicmapSchemas()

  def preReqs(self):
    # test vars from config that should exist
    reqCfgVars = ['dbHost','dbPort','dbPort','dbName',
                  'dbUser','dbPswd', 'dbClientPath',
                  'baseUrl','email']
    for var in reqCfgVars:
      if not (val := self.cfg.get(var)):
        raise Exception(f"No {var} in config.ini")
      if not val:
        raise Exception(f"No value for {var} in config.ini")
    return True
    
  def checkDb_Meta(self):
    # check db is spatial
    try:
      self.db.execute("SELECT PostGIS_version()")
    except Exception as ex:
      logging.warning("PostGis has not been installed. Please install it as the database superuser.")
      # try: # issue, you have to be a superuser to install postgis.
      #   self.db.execute("CREATE EXTENSION PostGIS")
      # except Exception as ex:
      #   logging.error("Could not install PostGis on DB")
      #   raise ex
        
    # get all schemas present
    self.dbSchemas = self.db.getSchemas()
    # check db has vm_meta schema, if not create and add layer_registry
    if 'vm_meta' not in self.dbSchemas:
      self.db.createSch('vm_meta')
    if 'layer_registry' not in self.db.getTables('vm_meta'):
      with open('layer_registry.sql', 'r') as file:
        createStmt = file.read()
        # logging.info(f"file: {createStmt}")
        self.db.execute(createStmt)
    if 'vm_delta' not in self.dbSchemas:
      self.db.createSch('vm_delta')
    
  def checkApiClient(self):
    # if the client_id is null, we need to create it
    if not self.cfg.get('client_id'):
      logging.info("Submitting email address to obtain a client_id")
      api = ApiUtils(self.cfg['baseUrl'], None, None)
      rsp = api.post("register", {"email":self.cfg['email']})
      logging.info("Updating config file with client_id...")
      self.cfg['client_id'] = rsp['client_id']
      self.configgy.write()#self.cfg)
      raise Exception(f"No client_id was yet registered, please check your inbox for {self.cfg['email']}")
    
    if not self.cfg.get("api_key"):
      logging.info("Submitting client_id to obtain an api-key")
      api = ApiUtils(self.cfg['baseUrl'], None, self.cfg['client_id'])
      rsp = api.post("register", {"email":self.cfg['email']})
      logging.info("Updating config file with api_key...")
      self.cfg['api_key'] = rsp['api_key']
      self.configgy.write()

  def seedDsets(self):
    # get full list of datasets
    api = ApiUtils(self.cfg['baseUrl'], self.cfg['api_key'], self.cfg['client_id'])
    rsp = api.post("data", {})
    dsets = [LyrReg(d) for d in rsp['datasets']]
    logging.info(f"Retrieved {len(rsp)}")
    
    # insert the records that don't exist yet
    _existingKeys = [d.identity for d in self.db.getRecSet(LyrReg)]
    for d in dsets:
      if d.identity not in _existingKeys:
        d.sup_ver=-1 # new record gets a negative supply-id so it matches on the latest seed.
        d.sup_type=Supplies.FULL # seed is full.
        self.db.execute(*d.insSql())

  def createVicmapSchemas(self):
    # check db has all vicmap schemas
    _dsets = self.db.getRecSet(LyrReg)
    _schemas = set([d.identity.split('.')[0] for d in _dsets])
    logging.info(_schemas)
    [self.db.createSch(sch) for sch in _schemas if sch not in self.dbSchemas]
    
    