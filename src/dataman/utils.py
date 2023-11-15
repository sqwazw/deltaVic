import os, logging, configparser, io, zipfile, shutil
from subprocess import Popen, PIPE
from glob import glob

class FileUtils():
  def __init__(self):
    pass

  @staticmethod
  def findFilesInRoot(root, fileName):
    findRegEx = "{}/**/{}".format(root, fileName) # regEx to find file.
    logging.debug(findRegEx)
    fileArr = glob(findRegEx, recursive=True) # py3.10 will allow root_dir as an arg here. https://docs.python.org/3.10/library/glob.html
    if not len(fileArr):
      logging.warning("Cannot find file(s) {} in root {}".format(fileName, root))
      return None
    else:
      return fileArr
  
  @staticmethod
  def sizeof_fmt(num, suffix="B"):
    for unit in ["", "Ki", "Mi", "Gi", "Ti", "Pi", "Ei", "Zi"]:
      if abs(num) < 1024.0:
        return f"{num:3.1f}{unit}{suffix}"
      num /= 1024.0
    return f"{num:.1f}Yi{suffix}"
  
  @staticmethod
  def createDir(pathy):
    if not os.path.exists(pathy):
      os.makedirs(pathy)
    return pathy
  
  @staticmethod
  def remove(path):
    logging.debug(f"removing path {path}")
    if os.path.isfile(path): os.remove(path)
    elif os.path.isdir(path): shutil.rmtree(path)
    else: logging.warn(f"{path} is not a file or a dir!! It may not exist")
  
  @staticmethod
  #create_dir create a directory with the name of the archive to place the contents of the archive into.
  def extract(vml_bucket, srcZipPath:str, tgt:str):
    logging.info("Extracting: {0} to {1}".format(srcZipPath, tgt))
    file_stream = io.BytesIO()
    vml_bucket.download_fileobj(srcZipPath, file_stream)
    zf = zipfile.ZipFile(file_stream)
    zf.extractall(tgt)
    # return tgt

  # @staticmethod
  # def runProc(procArgs: list):
  #   logging.info(f"Running process {procArgs}")
  #   p = Popen(procArgs, stdout=PIPE, stderr=PIPE)
  #   stdout, stderr = p.communicate()

  @staticmethod
  def run_sub(params:list):
    _msgStr, _errStr = "",""
    logging.debug(f"Sending Command: {' '.join(params)}")
    # logging.debug("FU.LD_LIBRARY_PATH: " + os.environ["LD_LIBRARY_PATH"])
    logging.debug("FU.PATH: " + os.environ["PATH"])
    
    # ["chmod","-R","a+x",dir] -- set execute recursive on dir
    # ["chmod","-R","a+rw",dir] -- set read/write recursive on dir
    # ["chmod","-R","a+r",dir] -- set read recursive on dir
    # the following will not pick up things that are on the path, hence the exceptions.
    if params[0] not in ['uname','ps','scp','env','chmod','pg_restore','pg_dump'] and not os.path.exists(params[0]):
      raise Exception(f"Path {params[0]} does not exist")
    
    proc = Popen(params, stderr=PIPE, stdout=PIPE)
    stdout, stderr = proc.communicate()
    
    if _msgStr := stdout.decode():
      logging.info(f"Command Response:{_msgStr}")
    if _errStr := stderr.decode():
      logging.error(f"Command Response Error:{_errStr}")
      raise Exception(_errStr)
    
    return _msgStr
  
class Logger():
  @staticmethod
  def get():
    log_formatter = logging.Formatter('[%(levelname)s] - %(message)s')
    _rootLog = logging.getLogger()
    _rootLog.level = logging.INFO
    try: # on aws the getLOgger command get a handler automatically
      hdlr = _rootLog.handlers[0]
      hdlr.setFormatter(log_formatter)
    except IndexError: # otherwise we have to make the handler ourselves
      # log_formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
      console_handler = logging.StreamHandler()
      console_handler.setFormatter(log_formatter)#'%(asctime)s - %(name)s - %(levelname)s - %(message)s'
      _rootLog.addHandler(console_handler)

    logging.getLogger("boto").setLevel(logging.CRITICAL)
    logging.getLogger("botocore").setLevel(logging.CRITICAL)
    logging.getLogger("urllib3").setLevel(logging.CRITICAL)
    logging.getLogger('s3transfer').setLevel(logging.CRITICAL)

    return _rootLog
  
class Configgy():
  def __init__(self, cfgFile):
    self.cfgFile = 'config.ini'
    self.cfg = configparser.ConfigParser()
    self.cfg.optionxform = str # keep it case sensitive
    self.cfg.read(self.cfgFile)
  def write(self):#, cfg):
    with open(self.cfgFile, 'w') as newCfg:
      self.cfg.write(newCfg)
  
class Test():
  def __init__(self):
    logging.info("initting test")
  def cfg(self, cfg):
    logging.info(self.cfg['email'])
  def db(self, db):
    result = db.item("select count(*) from eisedit.mapindex_100d")
    logging.info(f"result: {result} *")
    result = db.row("select count(*), sum(ufi) from eisedit.mapindex_100d")
    logging.info(f"result: {result} *")
    result = db.rows("select * from eisedit.mapindex_100d")
    rows = '\r'.join([str(r) for r in result])
    logging.info(f" count:{len(result)}\r result: {rows} *")
  def api(self, api):
    api.getDatasets()

class Supplies(dict):
  # load types
  FULL = "full" # seed
  INC = "inc" # incremental

  @staticmethod
  def meta(supId):
    supsDict = {
      Supply_Lat.supply_id:Supply_Lat,
      Supply_Topo.supply_id:Supply_Topo,
      Supply_Misc.supply_id:Supply_Misc
    }
    return supsDict[supId]
  
class Supply_Lat():
  supply_id = 'VLAT'
  fams = ['VLAT_VLAT','VLAT_VM']
  supplyPrefix = "vlat" # supply prefix in the jacobs bucket
  # database_schema = "lat"
  supplySchema = "latsupply"
  loadSchema = "latload"
  trackingTable = "lat_supply_table_instance"
  # schema_list_product = ["vmadd", "vmadmin", "vmcltenure", "vmprop"]
  ufiCreateCol = "ufi_created"
  ufiRetireCol = "ufi_retired"
  pfiRetireCol = "pfi_retired"

class Supply_Topo():
  supply_id = 'VTT'
  fams = ['VTT_VTT','VTT_VM']
  supplyPrefix = "topo" # supply prefix in the jacobs bucket
  # database_schema = "topo"
  supplySchema = "toposupply"
  loadSchema = "topoload"
  trackingTable = "topo_supply_table_instance"
  # schema_list_product = ["vmelev", "vmfeat", "vmhydro", "vmindex", "vmtrans", "vmveg"]
  ufiCreateCol = "create_date_ufi"
  ufiRetireCol = "retire_date_ufi"
  pfiRetireCol = "retire_date_pfi"

class Supply_Misc():
  supply_id = 'MISC'
  fams = ['MISC']
  supplyPrefix = None
  # database_schema = "misc"
  supplySchema = "miscsupply"
  loadSchema = "misc"
  trackingTable = None # "vmdd.instance_registry"
  ufiCreateCol = "ufi_created"
  ufiRetireCol = "ufi_retired"
  pfiRetireCol = "pfi_retired"

