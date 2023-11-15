import json, logging
from datetime import datetime

class DBTable():
  QUEUED="QUEUED"
  RESTORE="RESTORE"
  DELETE="DELETE"
  ADD="ADD"
  RECONCILE="RECONCILE"
  COMPLETE="COMPLETE"

  def __init__(self, row):
    [setattr(self, col,val) for col,val in zip(self.cols, row)]
  
  def insSql(self):
    _upDict = {}
    [_upDict.update({col:val}) for col,val in zip(self.cols, self.asList()) if col not in ('edit_date')]
    _cols = _upDict.keys()
    sqlStr =  f"INSERT into {self.name} ({','.join(_cols)}) VALUES ({('%s,'*len(_cols))[0:-1]})"
    sqlParams = list(_upDict.values())
    # logging.info(f"sqlStr:{sqlStr}")
    # logging.info(f"sqlParams:{sqlParams}")
    return sqlStr, sqlParams

  def setErr(self, errState=True):
    self.err = errState
    sqlStr = "update {} set err=%s where schTbl=%s".format(self.name)
    sqlParams = (self.err, self.schTbl)
    return sqlStr, sqlParams
  
  def asList(self):
    # return [getattr(self, col) for col in self.cols]
    listy = [getattr(self, col) for col in self.cols]
    listy = [json.dumps(ll) if isinstance(ll, dict) else ll for ll in listy] #json columns need to be strings on pg insert.
    return listy
  
  def enQueue(self, supVer, supType):
    sqlStr = "update {} set status=%s, sup_ver=%s, sup_type=%s, err=%s where schTbl=%s".format(self.name)
    sqlParams = (self.QUEUED, supVer, supType, False, self.schTbl)
    return sqlStr, sqlParams

  def upStatsSql(self, maxUfiDate, maxUfi, tblCount, tblChkSum):
    sqlStr = "update {} set max_create=%s, max_ufi=%s, row_count=%s, check_sum=%s where schTbl=%s".format(self.name)
    sqlParams = (maxUfiDate, maxUfi, tblCount, tblChkSum, self.schTbl)
    return sqlStr, sqlParams

  def upSupSql(self, supVer, supType):
    """ update supply info """
    self.sup_ver, self.sup_type = supVer, supType
    sqlStr = "UPDATE {} SET sup_ver=%s, sup_type=%s WHERE schTbl=%s".format(self.name)
    sqlParams = (supVer, supType, self.schTbl)
    return sqlStr, sqlParams
  
  def upStatusSql(self, status):
    self.status = status
    sqlStr = "UPDATE {} SET status=%s WHERE schTbl=%s".format(self.name)
    sqlParams = (status, self.schTbl)
    return sqlStr, sqlParams

  def upExtraSql(self, dicty=None):
    self.extradata = self.extradata or {} # set as empty dict if None
    if isinstance(self.extradata, str): # dodgy fix?
      logging.warn("Extradata was a string, fixing")
      self.extradata = json.loads(self.extradata)
    # update with the new values or clear if None.
    self.extradata.update(dicty) if dicty else self.extradata.clear()
    
    sqlStr = "UPDATE {} SET extradata=%s where schTbl=%s".format(self.name)
    sqlParams = (json.dumps(self.extradata), self.schTbl)
    return sqlStr, sqlParams
  
  @classmethod
  def listmaker(cls):
    return f"select {','.join(cls.cols)} from {cls.name} order by schTbl"

class LyrReg(DBTable):
  name='vm_delta.layer_registry'
  # cols=['sch','tbl','relation','active','md_uuid','md_altname','geom_type','geom_dim',
  #   'pkey','anzlic_id','sup','sup_ver','sup_type','sup_tbl','extradata','ufi','ufi_created','status','err','sql_where']#,'foi','static'
  cols=['schTbl','active','relation','geom_type','pkey','status','err','sup','sup_ver','sup_date','sup_type','md_uuid','extradata','edit_date']
  # cols = ['sch','tbl']
  def __init__(self, lyrObj):
    if type(lyrObj) == tuple:
      super().__init__(lyrObj)
    elif type(lyrObj) == dict:
      sup_date = datetime.fromisoformat(lyrObj['sup_date'])
      newRow = [lyrObj['schTbl'],True,lyrObj['relation'],lyrObj['geom_type'],lyrObj['pkey'],LyrReg.QUEUED,False,lyrObj['sup'],lyrObj['sup_ver'],sup_date,None,None,None,None]
      # logging.info(f"initting new row: {newRow}")
      super().__init__(newRow)
      # logging.info(self.sup_ver)
    else:
      raise Exception(f"Layer Object was not in an expected format")
  
  def __str__(self):
    return f"{self.schTbl}-{self.sup}-{self.sup_ver}-{self.status}"

# class Datasets():
#   def __init__(self, db):
#     _dsets = db.getRecSet(LyrReg.listmaker())
#   def __str__(self):
#     return f"{self.sch}.{self.tbl} {self.status}"


