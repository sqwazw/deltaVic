import os, logging, traceback
from datetime import datetime

from dataman import LyrReg, ApiUtils, PGClient, Supplies, FU

###########################################################################

class Synccer():
  def __init__(self, cfg, db):
    self.cfg = cfg
    self.db = db
    # self.lyrs = []
    self.tables = []
    self.views = []
  
  def unWait(self):
    self.db.execute(LyrReg.unWaitUpSql())

  def assess(self):
    self.resolve() # updates out of date datasets to 'QUEUED'
    _layers = [ll for ll in self.db.getRecSet(LyrReg) if ll.active and not ll.err and ll.status not in (LyrReg.COMPLETE,LyrReg.WAIT)]
    self.tables = [ll for ll in _layers if ll.relation=='table']
    # self.tables = [tt for tt in self.tables if tt.identity.startswith('vmreftab')]#[0:1] # use to dither candidates.
    self.views = [ll for ll in _layers if ll.relation=='view']
    logging.info(f'To Process: {len(self.tables)} table and {len(self.views)} views')
    return len(self.tables) + len(self.views)
  
  def resolve(self):
    _local, _remote = {}, {}
    # Note, only compare those datasets present locally as active and in a complete state and not in err.
    [_local.update({d.identity:d}) for d in self.db.getRecSet(LyrReg) if d.active and d.status==LyrReg.COMPLETE and not d.err]
    [_remote.update({ll.identity:ll}) for ll in self.getVicmap()]
    logging.info(f"Retrieved layers: {len(_local)} locally, {len(_remote)} from vicmap_master")

    for name,lyr in _local.items():
      if not (_vmLyr := _remote.get(name)):
        logging.warning(f"No version of {name} existed in the remote datasets") # auto delete in qa at end?
        continue
      if lyr.sup_ver != _vmLyr.sup_ver:
        # logging.debug(f"{lyr.sup_ver}, {_vmLyr.sup_ver}")
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

  def process(self):
    while self.lyr.status not in (LyrReg.COMPLETE,LyrReg.WAIT) and not self.lyr.err:
      try:
        logging.debug(F"status: {self.lyr.status.lower()}")
        getattr(self, self.lyr.status.lower())()
      except Exception as ex:
        # logging.error(str(ex))
        logging.error(traceback.format_exc())
        self.db.execute(*self.lyr.upExtraSql({"error":str(ex)}))
        self.db.execute(*self.lyr.setErr())

  def queued(self):
    logging.info(F"q-ing {self.lyr.identity} -- current({self.lyr.sup}:{self.lyr.sup_ver}:{self.lyr.sup_type})")
    # get the next dump file from data endpoint
    api = ApiUtils(self.cfg['baseUrl'], self.cfg['api_key'], self.cfg['client_id'])
    _rsp = api.post("data", {"dset":self.lyr.identity,"sup_ver":self.lyr.sup_ver})
    if not (_next := _rsp.get("next")):
      if self.lyr.sup_ver == _rsp.get("sup_ver"): # have the latest already
        logging.warn("Requested data load but endpoint says max(sup_ver) is current")
        self.db.execute(*self.lyr.upStatusSql(LyrReg.COMPLETE))
      else:
        logging.warn("Requested data load but endpoint says next ready only half ready yet")
        self.db.execute(*self.lyr.upStatusSql(LyrReg.WAIT))
      return
    
    self.db.execute(*self.lyr.upExtraSql({})) # clear the field.
    logging.debug(F"next: {_next}")
    self.db.execute(*self.lyr.upExtraSql(_next))

    _supDate = datetime.fromisoformat(_next['sup_date']) if _next['sup_date'] else None # datetime.now()# 
    logging.info(F" --> next({_next['sup_ver']}:{_next['sup_type']}):{_supDate}")
    self.db.execute(*self.lyr.upSupSql(_next['sup_ver'], _next['sup_type'], _supDate))

    self.db.execute(*self.lyr.upStatusSql(LyrReg.RESTORE))

  def restore(self):
    logging.debug(F" -> restore-ing {self.lyr.identity}")
    if not os.path.exists("temp"):
      os.makedirs("temp")
    # logging.info(f"extradata: {self.lyr.extradata}")
    fPath = f"temp/{self.lyr.extradata['filename']}"
    ApiUtils.download_file(self.lyr.extradata['s3_url'], fPath)
    # restore the file - full loads go straight to each vicmap schema, incs go to the vm_delta schema.
    # logging.debug(f"restore version: {PGClient(self.db, self.cfg['dbClientPath']).get_restore_version()}") # test pg connection.
    PGClient(self.db, self.cfg['dbClientPath']).restore_file(fPath)
    self.db.execute(*self.lyr.upStatusSql(LyrReg.QUEUED))
    
    if self.lyr.sup_type == Supplies.FULL:
      self.db.execute(*self.lyr.upStatusSql(LyrReg.RECONCILE))
    else:
      self.db.execute(*self.lyr.upStatusSql(LyrReg.DELETE))
    
  def delete(self):
    # apply the deletes -> add()
    logging.debug(f" -> Deleting rows for {self.lyr.identity}")
    _delta = self.lyr.extradata['filename'].replace('.dmp','')
    sqlDel = (f"delete from {self.lyr.identity} where {self.lyr.pkey} in"
              f" (select {self.lyr.pkey} from {_delta} where operation='DELETE')")
    self.db.execute(sqlDel)
    self.db.execute(*self.lyr.upStatusSql(LyrReg.ADD))

  def add(self):
    # delete the adds in case of a rerun, (idempotency), then add the adds again -> reconcile()
    logging.debug(f" -> Adding rows for {self.lyr.identity}")
    #pre-add-del -> cleans up before a rerun. Can just do whole inc table in the DELETE above.
    _delta = self.lyr.extradata['filename'].replace('.dmp','')
    sqlDel = (f"delete from {self.lyr.identity} where {self.lyr.pkey} in"
              f" (select {self.lyr.pkey} from {_delta} where operation='INSERT')")
    self.db.execute(sqlDel)
    #adds
    logging.debug(f"Adding rows for {self.lyr.identity}")
    colsCsv = ",".join(self.db.getAllCols(self.lyr.identity)) # Get Column names as csv for sql.
    sqlAdd = (f"insert into {self.lyr.identity}"
              f" (select {colsCsv} from {_delta} where operation='INSERT')")
    self.db.execute(sqlAdd)
    self.db.analVac(self.lyr.identity) # clean up since dels and adds have completed.
    self.db.execute(*self.lyr.upStatusSql(LyrReg.RECONCILE))

  def reconcile(self):
    logging.debug(F" -> reconcile-ing {self.lyr.identity}")
    # reconcile the row count and check sum -> signOff()?
    # warning: if table is malformed and missing ufi_created col, it will fail here and not insert stats, which will throw errs when assembling datasets.
    # warning 2, if a datasets does not have a pkey specified it will also failed - need to update the control schema prior to anyone seeding.
    try: 
      supCount, supChkSum = self.lyr.extradata['row_count'], self.lyr.extradata['check_sum']
      # logging.debug(f"{supCount} {supChkSum}")
      maxUfiDate, maxUfi, tblCount, tblChkSum = self.db.getTblStats(self.lyr.identity, Supplies.meta(self.lyr.sup).ufiCreateCol, self.lyr.pkey) # , state_query=self.obj.pidUpSql()
      # logging.debug(f"{tblCount} {tblChkSum}")
      
    except Exception as ex:
      logging.warning(f"{self.lyr.identity} did not return stats: {str(ex)}")
      raise Exception(f"Problem getting stats: {str(ex)}")
    
    recStr = ""
    recStr += f" count(remote:{supCount}!=local:{tblCount})-diff({supCount-tblCount})" if tblCount!=supCount else ""
    recStr += f" chkSum(remote:{supChkSum}!=local:{tblChkSum})-diff({supChkSum-tblChkSum})" if tblChkSum!=supChkSum else ""
    if recStr:
      raise Exception(f"Supply misreconciled: {recStr}")
    # self.db.execute(*self.lyr.upStatsSql(maxUfiDate, maxUfi, tblCount, tblChkSum))
    
    # self.db.execute(*self.lyr.upSupSql(self.lyr.extradata['sup_ver'], self.lyr.extradata['sup_type'], self.lyr.extradata['sup_date']))
    self.db.execute(*self.lyr.upStatusSql(LyrReg.CLEAN))
   
  def clean(self):
    # clean up the temp file and the temp view
    logging.debug(F" -> clean-ing {self.lyr.identity}")
    if self.lyr.sup_type == Supplies.INC:
      self.db.dropTable(self.lyr.extradata['filename'].replace('.dmp',''))
    FU.remove(f"temp/{self.lyr.extradata['filename']}")
    # status->COMPLETE or err=true
    self.db.execute(*self.lyr.upStatusSql(LyrReg.COMPLETE))
