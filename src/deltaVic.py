import sys, os, logging, traceback
from datetime import datetime, timedelta

from dataman import DB, FU, Logger, Configgy, LyrReg
from vmd import Setup, Synccer

rootLog = Logger().get()
# rootLog.level = logging.DEBUG
# NOTSET=0, DEBUG=10, INFO=20, WARN=30, ERROR=40, CRITICAL=50

class vmdelta():
  STAGE='dev'

  def __init__(self, vargs):
    self.action=vargs[0] if len(vargs) > 0 else "sync" # default
    self.task  =vargs[1] if len(vargs) > 1 else False
    self.thing =vargs[2] if len(vargs) > 2 else False
    logging.debug("running deltaVic...")
    self.configgy = Configgy("config.ini")
    self.cfg = self.configgy.cfg[self.STAGE]
    # logging.info(f"setting log level to {int(self.cfg['log_level'])}")
    rootLog.level = int(self.cfg['log_level'])
    # logging.info(rootLog.level) # not working??

  def run(self):
    match self.action:
      case "setup":
        Setup(self.configgy, self.STAGE).run()
      case "sync":
        _db = DB(self.cfg['dbHost'], self.cfg['dbPort'], self.cfg['dbName'], self.cfg['dbUser'], self.cfg['dbPswd'])
        synccer = Synccer(self.cfg, _db)
        synccer.unWait() # queue any leftover jobs from last time.
        while(synccer.assess()):
          synccer.run()
          # break
      case "status":
        Setup(self.configgy, self.STAGE).status(self.STAGE)
      case "core":
        # set only core vicmap layers
        Setup(self.configgy, self.STAGE).core()
      # case "upload":
      #   self.upload(self.thing)
      case "clean":
        if self.task == "db": # analyse and vaccuume the tables
          _db = DB(self.cfg['dbHost'], self.cfg['dbPort'], self.cfg['dbName'], self.cfg['dbUser'], self.cfg['dbPswd'])
          logging.info("Analysing and Vaccuuming...")
          for dset in _db.getRecSet(LyrReg):
            if _db.table_exists(dset.identity):
              # logging.info(f"...{dset.identity}")
              _db.analVac(dset.identity)
        elif self.task == "files": # remove any leftover files in temp
          for dir, subdir, files in os.walk('temp'):
            [FU.remove(f"{dir}{os.sep}{ff}") for ff in files]
        else:
          logging.info("task was not specified")
      case "scorch":
        # remove all datasets and empty the layer_registry table
        _db = DB(self.cfg['dbHost'], self.cfg['dbPort'], self.cfg['dbName'], self.cfg['dbUser'], self.cfg['dbPswd'])
        for dset in _db.getRecSet(LyrReg):
          if dset.relation == 'table': _db.dropTable(dset.identity)
          # if dset.relation == 'view': _db.dropView(dset.identity)
        _db.truncate("vm_delta.layer_registry")
      case "_":
        logging.info("action was not specified") # default to sync?
    
  # def upload(self):
  #   pass


def main():
  startTime = datetime.now()
  # logging.info(f"Start Time: {startTime}")
  
  try:
    vmd = vmdelta(sys.argv[1:])
    vmd.run()
  except Exception as ex:
    logging.info(f"Exception: {str(ex)}")
    logging.info(traceback.format_exc())
  
  endTime = datetime.now()
  # logging.info(f"End Time: {endTime}")
  duration = (endTime - startTime).total_seconds()
  logging.info(f"Duration: {str(timedelta(seconds=duration)).split('.')[0]}")

if __name__ == "__main__":
  main()