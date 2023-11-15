import sys, logging, traceback
from datetime import datetime

from dataman import DB, Logger, Configgy
from vmd import Setup, Synccer

rootLog = Logger().get()

class vmdelta():
  def __init__(self, vargs):
    self.action=vargs[0] if len(vargs) > 0 else "sync" # default
    self.task  =vargs[1] if len(vargs) > 1 else False
    self.thing =vargs[2] if len(vargs) > 2 else False
    logging.info("initting vmdelta")
    self.configgy = Configgy("config.ini")
    self.cfg = self.configgy.cfg['dev']
  
  def run(self):
    match self.action:
      case "setup":
        Setup(self.configgy).run()
      case "sync":
        _db = DB(self.cfg['dbHost'], self.cfg['dbPort'], self.cfg['dbName'], self.cfg['dbUser'], self.cfg['dbPswd'])
        synccer = Synccer(self.cfg, _db)
        synccer.assess()
        synccer.run()
      # case "upload":
      #   self.upload(self.thing)
    
  # def uploadData(self):
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