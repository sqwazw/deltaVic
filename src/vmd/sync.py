import os, logging, traceback
from dataman import LyrReg, ApiUtils, PGClient, Supplies

###########################################################################

class Synccer():
  def __init__(self, cfg, db):
    self.cfg = cfg
    self.db = db
    # self.lyrs = []
    self.tables = []
    self.views = []
  
  def assess(self):
    self.resolve() # updates out of date datasets to 'QUEUED'
    _layers = [ll for ll in self.db.getRecSet(LyrReg) if ll.active and not ll.err and ll.status != 'COMPLETE']
    self.tables = [ll for ll in _layers if ll.relation=='table']
    self.views = [ll for ll in _layers if ll.relation=='view']
    logging.info(f'To Process: {len(self.tables)} table and {len(self.views)} views')
  
  def resolve(self):
    _local, _remote = {}, {}
    # Note, only compare those datasets present locally as active and in a complete state and not in err.
    [_local.update({d.schTbl:d}) for d in self.db.getRecSet(LyrReg) if d.active and d.status==LyrReg.COMPLETE and not d.err]
    [_remote.update({ll.schTbl:ll}) for ll in self.getVicmap()]
    logging.info(f"Retrieved layers: {len(_local)} locally, {len(_remote)} from vicmap")

    for name,lyr in _local.items():
      if not (_vmLyr := _remote.get(name)):
        logging.warning(f"No version of {name} existed in the remote datasets")
        continue
      if lyr.sup_ver != _vmLyr.sup_ver:
        logging.info(f"{lyr.sup_ver}, {_vmLyr.sup_ver}")
        self.db.execute(*lyr.upStatusSql(LyrReg.QUEUED))

  def getVicmap(self):#seedDsets(self):
    # get full list of datasets
    api = ApiUtils(self.cfg['baseUrl'], self.cfg['api_key'], self.cfg['client_id'])
    rsp = api.post('data', {})
    return [LyrReg(d) for d in rsp['datasets']]
    
  def run(self):
    try:
      if self.tables: #process table based on status
        [Sync(self.db, self.cfg, tbl).process() for tbl in self.tables]
      else:
        if self.views: # process views based on status
          [Sync(self.db, self.cfg, vw).process() for vw in self.views]
    except Exception as ex:
      _msg = "Something went wrong in the Synccer"
      logging.error(_msg)
      raise Exception(_msg)
    
###########################################################################

class Sync():
  def __init__(self, db, cfg, lyr):
    self.db = db
    self.cfg = cfg
    self.lyr = lyr
    self.delta = None

  def process(self):
    while self.lyr.status != 'COMPLETE' and not self.lyr.err:
      try:
        logging.info(F"status: {self.lyr.status.lower()}")
        getattr(self, self.lyr.status.lower())()
      except Exception as ex:
        logging.error(str(ex))
        logging.error(traceback.format_exc())
        self.db.execute(*self.lyr.upExtraSql({"error":str(ex)}))
        self.db.execute(*self.lyr.setErr())

  def queued(self):
    logging.info(F"q-ing {self.lyr.schTbl}")
    # get the next dump file from data endpoint
    api = ApiUtils(self.cfg['baseUrl'], self.cfg['api_key'], self.cfg['client_id'])
    _rsp = api.post("data", {"dset":self.lyr.schTbl,"sup_ver":self.lyr.sup_ver})
    if _next := _rsp.get("next"):
      self.db.execute(*self.lyr.upExtraSql({}))
      self.db.execute(*self.lyr.upExtraSql(_next))
      self.db.execute(*self.lyr.upStatusSql(LyrReg.RESTORE))
    else:
      raise Exception("Tried to load data but data endpoint says there is no next")

  def restore(self):
    logging.info(F"restore-ing {self.lyr.schTbl}")
    if not os.path.exists("temp"):
      os.makedirs("temp")
    # logging.info(f"extradata: {self.lyr.extradata}")
    self.delta = self.lyr.extradata['filename'].replace('.dmp','')
    fPath = f"temp/{self.lyr.extradata['filename']}"
    ApiUtils.download_file(self.lyr.extradata['s3_url'], fPath)
    # restore the file - full loads go straight to each vicmap schema, incs go to the vm_delta schema.
    PGClient(self.db, self.cfg['dbClientPath']).restore_file(fPath)
    # self.db.execute(*self.lyr.enQueue(self.lyr.extradata['sup_ver'], self.lyr.extradata['sup_type']))
    self.db.execute(*self.lyr.upStatusSql(LyrReg.QUEUED))
    
    if self.lyr.extradata['sup_type'] == Supplies.FULL:
      self.db.execute(*self.lyr.upStatusSql(LyrReg.RECONCILE))
    else:
      self.db.execute(*self.lyr.upStatusSql(LyrReg.DELETE))
    
  def delete(self):
    # apply the deletes -> add()
    logging.debug(f"Deleting rows for {self.lyr.schTbl}")
    sqlDel = (f"delete from {self.lyr.schTbl} where {self.lyr.pkey} in"
              f" (select {self.lyr.pkey} from {self.delta} where operation='DELETE')")
    self.db.execute(sqlDel)
    self.db.execute(*self.lyr.upStatusSql(LyrReg.ADD))

  def add(self):
    # delete the adds in case of a rerun, then add the adds -> reconcile()
    logging.debug(f"Adding rows for {self.lyr.schTbl}")
    #pre-add-del -> cleans up before a rerun. Can just do whole inc table in the DELETE above.
    sqlDel = (f"delete from {self.lyr.schTbl} where {self.lyr.pkey} in"
              f" (select {self.lyr.pkey} from {self.delta} where operation='INSERT')")
    self.db.execute(sqlDel)
    #adds
    logging.info(f"Adding rows for {self.dset.tbl}")
    colsCsv = ",".join(self.db.getAllCols(self.lyr.schTbl)) # Get Column names as csv for sql.
    sqlAdd = (f"insert into {self.lyr.schTbl}"
              f" (select {colsCsv} from {self.delta} where operation='INSERT')")
    self.db.execute(sqlAdd)
    self.db.analVac(self.lyr.schTbl) # clean up since dels and adds have completed.
    self.db.execute(*self.lyr.upStatusSql(LyrReg.RECONCILE))

  def reconcile(self):
    logging.info(F"reconcile-ing {self.lyr.schTbl}")
    # reconcile the row count and check sum -> signOff()?
    # warning: if table is malformed and missing ufi_created col, it will fail here and not insert stats, which will throw errs when assembling datasets.
    # warning 2, if a datasets does not have a pkey specified it will also failed - need to update the control schema prior to anyone seeding.
    try: 
      supCount, supChkSum = self.lyr.extradata['row_count'], self.lyr.extradata['check_sum'], 
      maxUfiDate, maxUfi, tblCount, tblChkSum = self.db.getTblStats(self.lyr.schTbl, Supplies.meta(self.lyr.sup).ufiCreateCol, self.lyr.pkey) # , state_query=self.obj.pidUpSql()
    except Exception as ex:
      logging.warning(f"{self.lyr.schTbl} did not return stats: {str(ex)}")
      raise Exception("Problem getting stats: {str(ex)}")
    
    recStr = ""
    recStr += f" count(sup:{supCount}!=vml:{tblCount})-diff({supCount-tblCount})" if tblCount!=supCount else ""
    recStr += f" chkSum(sup:{supChkSum}!=vml:{tblChkSum})-diff({supChkSum-tblChkSum})" if tblChkSum!=supChkSum else ""
    if recStr:
      raise Exception(f"Supply misreconciled: {recStr}")
    # self.db.execute(*self.lyr.upStatsSql(maxUfiDate, maxUfi, tblCount, tblChkSum))
    
    self.db.execute(*self.lyr.upSupSql(self.lyr.extradata['sup_ver'], self.lyr.extradata['sup_type']))
    self.db.execute(*self.lyr.upStatusSql(LyrReg.COMPLETE))
   
  # def signOff(self):
  #   # clean up the temp file and the temp view
  #   # status->COMPLETE or err=true
  #   pass
