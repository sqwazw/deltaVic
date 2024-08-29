import sys, os, logging, traceback
from datetime import datetime

from dataman import DB, FU, Logger, Configgy, LyrReg
from vmd import Setup, Synccer

rootLog = Logger().get()
# rootLog.level = logging.DEBUG

class vmdelta():
  STAGE='prd'

  def __init__(self, vargs):
    self.action=vargs[0] if len(vargs) > 0 else "sync" # default
    self.task  =vargs[1] if len(vargs) > 1 else False
    self.thing =vargs[2] if len(vargs) > 2 else False
    logging.info("initting vmdelta")
    self.configgy = Configgy("config.ini")
    self.cfg = self.configgy.cfg[self.STAGE]
    rootLog.level = int(self.cfg['log_level'])

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
      # case "upload":
      #   self.upload(self.thing)
      case "clean":
        if self.task == "db": # analyse and vaccuume the tables
          _db = DB(self.cfg['dbHost'], self.cfg['dbPort'], self.cfg['dbName'], self.cfg['dbUser'], self.cfg['dbPswd'])
          logging.info("Analysing and Vaccuuming...")
          for dset in _db.getRecSet(LyrReg):
            logging.info(f"...{dset.identity}")
            _db.analVac(dset.identity)
        elif self.task == "files": # remove any leftover files in temp
          for dir, subdir, files in os.walk('temp'):
            [FU.remove(f"{dir}{os.sep}{ff}") for ff in files]
        else:
          logging.info("task was not specified")
      case "_":
        logging.info("action was not specified")
    
  # def upload(self):
  #   pass


def main():
  startTime = datetime.now()
  logging.info(f"Start Time: {startTime}")
  
  try:
    vmd = vmdelta(sys.argv[1:])
    vmd.run()
  except Exception as ex:
    logging.info(f"Exception: {str(ex)}")
    logging.info(traceback.format_exc())
  
  endTime = datetime.now()
  logging.info(f"End Time: {endTime}")
  logging.info(f"Time taken: {(endTime - startTime).total_seconds()}")

if __name__ == "__main__":
  main()