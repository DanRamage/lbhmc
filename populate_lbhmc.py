import sys
sys.path.append("../../commonfiles/python")
import logging.config
import optparse
import ConfigParser
import json
from xlrd import open_workbook,cellname
from datetime import datetime, timedelta
import requests
from sqlalchemy import exc
from multiprocessing import Process, Queue, current_process, Event
#from multi_proc_logging import listener_process

from xeniaSQLAlchemy import xeniaAlchemy, multi_obs, platform
from unitsConversion import uomconversionFunctions

class mp_data_save_worker(Process):
  def __init__(self, **kwargs):
    Process.__init__(self)

    self.logger = logging.getLogger(type(self).__name__)
    self.input_queue = kwargs['input_queue']
    self.db_user = kwargs.get('db_user')
    self.db_pwd = kwargs.get('db_password')
    self.db_host = kwargs.get('db_host')
    self.db_name = kwargs.get('db_name')
    self.db_conn_type = kwargs.get('db_connectionstring')

  def run(self):
    try:
      logger = logging.getLogger(type(self).__name__)
      logger.debug("%s starting process" % (current_process().name))
      process_data = True

      db = xeniaAlchemy()
      if (db.connectDB(self.db_conn_type, self.db_user, self.db_pwd, self.db_host, self.db_name, False) == True):
        if (logger):
          logger.info("Succesfully connect to DB: %s at %s" % (self.db_name, self.db_host))
      else:
        logger.error("Unable to connect to DB: %s at %s. Terminating script." % (self.db_name, self.db_host))
        process_data = False
      if process_data:
        rec_count = 0
        for db_rec in iter(self.input_queue.get, 'STOP'):
          try:
            db.addRec(db_rec, True)
            val = ""
            if (db_rec.m_value != None):
              val = "%f" % (db_rec.m_value)
            logger.debug(
              "Committing record Sensor: %d Datetime: %s Value: %s" % (db_rec.sensor_id, db_rec.m_date, val))
            # Trying to add record that already exists.
          except exc.IntegrityError, e:
            db.session.rollback()
          except Exception, e:
            db.session.rollback()
            logger.exception(e)
        rec_count += 1

      logger.info("%s thread exiting." % (current_process().name))

      db.disconnect()

    except Exception as e:
      logger.exception(e)
    return

class obs_map:
  def __init__(self):
    self.__target_obs = None
    self.__target_uom = None
    self.__source_obs = None
    self.__source_uom = None
    self.__source_index = None
    self.__s_order = 1
    self.__sensor_id = None
    self.__m_type_id = None

  @property
  def target_obs(self):
    return self.__target_obs

  @target_obs.setter
  def target_obs(self, target_obs):
    self.__target_obs = target_obs

  @property
  def target_uom(self):
    return self.__target_uom

  @target_uom.setter
  def target_uom(self, target_uom):
    self.__target_uom = target_uom

  @property
  def source_obs(self):
    return self.__source_obs

  @source_obs.setter
  def source_obs(self, source_obs):
    self.__source_obs = source_obs

  @property
  def source_uom(self):
    return self.__source_uom

  @source_uom.setter
  def source_uom(self, source_uom):
    self.__source_uom = source_uom

  @property
  def s_order(self):
    return self.__s_order

  @s_order.setter
  def s_order(self, s_order):
    self.__s_order = s_order

  @property
  def source_index(self):
    return self.__source_index

  @source_index.setter
  def source_index(self, source_index):
    self.__source_index = source_index

  @property
  def sensor_id(self):
    return self.__sensor_id

  @sensor_id.setter
  def source_index(self, sensor_id):
    self.__sensor_id = sensor_id

  @property
  def m_type_id(self):
    return self.__m_type_id

  @m_type_id.setter
  def m_type_id(self, m_type_id):
    self.__m_type_id = m_type_id

class json_obs_map:
  def __init__(self):
    self.logger = logging.getLogger(type(self).__name__)
    self.obs = []

  def load_json_mapping(self, file_name):
    try:
      with open(file_name, "r") as obs_json:
        obs_json = json.load(obs_json)
        for obs in obs_json:
          xenia_obs = obs_map()
          xenia_obs.target_obs = obs['target_obs']
          if obs['target_uom'] is not None:
            xenia_obs.target_uom = obs['target_uom']
          xenia_obs.source_obs = obs['header_column']
          if obs['source_uom'] is not None:
            xenia_obs.source_uom = obs['source_uom']
          if obs['s_order'] is not None:
            xenia_obs.s_order = obs['s_order']
          self.obs.append(xenia_obs)
    except Exception as e:
      self.logger.exception(e)
      raise

  def build_db_mappings(self, **kwargs):
    db = xeniaAlchemy()
    if (db.connectDB(kwargs['db_connectionstring'],
                     kwargs['db_user'],
                      kwargs['db_password'],
                      kwargs['db_host'],
                      kwargs['db_name'],
                     False) == True):
      self.logger.info("Succesfully connect to DB: %s at %s" % (kwargs['db_name'], kwargs['db_host']))
    else:
      self.logger.error("Unable to connect to DB: %s at %s. Terminating script." % (kwargs['db_name'], kwargs['db_host']))

    entry_date = datetime.now()
    for obs_rec in self.obs:
      if obs_rec.target_obs != 'm_date':
        self.logger.debug("Platform: %s checking sensor exists %s(%s) s_order: %d" % (kwargs['platform_handle'],
                                                                                      obs_rec.target_obs,
                                                                                      obs_rec.target_uom,
                                                                                      obs_rec.s_order))
        sensor_id = db.sensorExists(obs_rec.target_obs, obs_rec.target_uom, kwargs['platform_handle'], obs_rec.s_order)
        if sensor_id is None:
          self.logger.debug("Sensor does not exist, adding")
          platform_id = db.platformExists(kwargs['platform_handle'])
          sensor_id = db.newSensor(entry_date.strftime('%Y-%m-%d %H:%M:%S'),
                                   obs_rec.target_obs,
                                   obs_rec.target_uom,
                                   platform_id,
                                   1,
                                   0,
                                   obs_rec.s_order,
                                   None,
                                   False)
        obs_rec.sensor_id = sensor_id
        m_type_id = db.mTypeExists(obs_rec.target_obs, obs_rec.target_uom)
        obs_rec.m_type_id = m_type_id
    db.disconnect()
  def get_date_field(self):
    for obs in self.obs:
      if obs.target_obs == 'm_date':
        return obs

  def get_rec_from_source_name(self, name):
    for obs in self.obs:
      if obs.source_obs == name:
        return obs

  def __iter__(self):
    for obs_rec in self.obs:
      yield obs_rec

def get_data(**kwargs):
  logger = logging.getLogger(__name__)
  base_url = kwargs['base_url']
  try:
    request_params = {
      'action': 'Excel',
      'siteId': kwargs['site_id'],
      'sensorId': kwargs['sensor_list'],
      'startDate': kwargs['start_time'],
      'endDate': kwargs['end_time'],
      'displayType': 'StationSensor',
      'predefFlag': 'true',
      'enddateFlag': 'false',
      'now': kwargs['now_time']
    }
    payload_str = "&".join("%s=%s" % (k, v) for k, v in request_params.items())
    logger.debug("Request: %s params: %s" % (base_url, payload_str))
    req = requests.get(base_url, params=payload_str)
    process_records = False
    if req.status_code == 200:
      logger.debug("Request successful, saving to file: %s" % (kwargs['dest_file']))
      with open(kwargs['dest_file'], 'wb') as f:
        for chunk in req.iter_content(1024):
          f.write(chunk)
        return True
    else:
      logger.error("Request failed.")
  except Exception as e:
    logger.exception(e)
  return False

def process_file(**kwargs):
  logger = logging.getLogger(__name__)
  obs_mapping = kwargs['obs_map']
  input_queue = kwargs['input_queue']
  db_obj = kwargs['db_obj']
  uom_converter = kwargs['units_conversion']

  logger.debug("Opening file: %s" % (kwargs['dest_file']))
  wb = open_workbook(filename=kwargs['dest_file'])
  sheet = wb.sheet_by_index(0)
  #Get platform info for lat/long
  plat_rec = db_obj.session.query(platform) \
    .filter(platform.platform_handle == kwargs['platform_handle']) \
    .one()

  row_entry_date = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
  for row_index in range(sheet.nrows):
    try:
      if row_index > 0:
        # HEader row, add the column index so we can lookup the obs in the worksheet.
        if row_index == 1:
          for col_index in range(sheet.ncols):
            field_name = sheet.cell(row_index, col_index).value
            obs_rec = obs_mapping.get_rec_from_source_name(field_name)
            if obs_rec is not None:
              obs_rec.source_index = col_index
        else:
          #Build the database records.
          m_date_rec = obs_mapping.get_date_field()
          for obs_rec in obs_mapping:
            #Skip the date, not a true obs.
            if obs_rec.target_obs != 'm_date':
              try:
                m_date = sheet.cell(row_index, m_date_rec.source_index).value
                value = float(sheet.cell(row_index, obs_rec.source_index).value)
                if obs_rec.target_uom != obs_rec.source_uom:
                  value = uom_converter.measurementConvert(value, obs_rec.source_uom, obs_rec.target_uom)
                db_rec = multi_obs(row_entry_date=row_entry_date,
                                        platform_handle=kwargs['platform_handle'],
                                        sensor_id=(obs_rec.sensor_id),
                                        m_type_id=(obs_rec.m_type_id),
                                        m_date=m_date,
                                        m_lon=plat_rec.fixed_longitude,
                                        m_lat=plat_rec.fixed_latitude,
                                        m_value=value
                                        )

                logger.debug("%s Queueing m_date: %s obs(%d): %s(%s): %f" %\
                                  (kwargs['platform_handle'],
                                   db_rec.m_date,
                                   db_rec.sensor_id,
                                   obs_rec.target_obs,
                                   obs_rec.target_uom,
                                   db_rec.m_value))
                input_queue.put(db_rec)
              except ValueError as e:
                logger.error("%s m_date: %s obs(%d): %s(%s) no value" %\
                             (kwargs['platform_handle'],
                              m_date,
                              obs_rec.sensor_id,
                              obs_rec.target_obs,
                              obs_rec.target_uom
                              ))
    except Exception as e:
      logger.exception(e)
  return

def main():
  logger = None
  try:
    parser = optparse.OptionParser()
    parser.add_option("-c", "--ConfigFile", dest="configFile",
                      help="Configuration file")
    (options, args) = parser.parse_args()

    configFile = ConfigParser.RawConfigParser()
    configFile.read(options.configFile)

    logFile = configFile.get('logging', 'configfile')

    logging.config.fileConfig(logFile)
    logger = logging.getLogger("data_ingestion_logger")
    logger.info('Log file opened')

    try:
      # Get the list of organizations we want to process. These are the keys to the [APP] sections on the ini file we
      # then use to pull specific processing directives from.
      station_count = configFile.getint('settings', 'station_count')
      base_url = configFile.get('settings', 'base_url')
      units_conversion_file = configFile.get('settings', 'units_conversion')
      db_user = configFile.get('database', 'user')
      db_password = configFile.get('database', 'password')
      db_host = configFile.get('database', 'host')
      db_name = configFile.get('database', 'name')
      db_connectionstring = configFile.get('database', 'connectionstring')
    except ConfigParser.Error as e:
      logger.exception(e)
    else:
      try:
        uom_converter = uomconversionFunctions(units_conversion_file)
        db = xeniaAlchemy()
        if (db.connectDB(db_connectionstring, db_user, db_password, db_host, db_name, False) == True):
          if (logger):
            logger.info("Succesfully connect to DB: %s at %s" % (db_name, db_host))
        else:
          logger.error("Unable to connect to DB: %s at %s. Terminating script." % (db_name, db_host))

        now_time = datetime.now()
        now_str = now_time.strftime('%a %b %d %Y %H:%M:%S GMT-0400 (EDT)')
        start_time = (now_time - timedelta(hours=1)).strftime('%Y-%m-%d %H:00:00')
        #Sutron does not do < end time, it's <= end time so we want to handle that
        #by making the minutes 00:59:00
        end_time = (datetime.strptime(start_time, '%Y-%m-%d %H:00:00') + timedelta(minutes=59)).strftime('%Y-%m-%d %H:%M:%S')

        input_queue = Queue()
        db_record_saver = mp_data_save_worker(db_user=db_user,
                                              db_password=db_password,
                                              db_host=db_host,
                                              db_name=db_name,
                                              db_connectionstring=db_connectionstring,
                                              input_queue=input_queue)
        db_record_saver.start()

        for station_num in range(station_count):
          if (logger):
            logger.info("Processing: %s" % (station_num+1))
          section = "station_%d" % (station_num+1)
          try:
            json_obs_file = configFile.get(section, "obs_mapping_file")
            site_id = configFile.get(section, 'site_id')
            sensor_list = configFile.get(section, 'sensor_ids')
            platform_handle = configFile.get(section, 'platform_handle')
            file_name = configFile.get(section, 'file_name')

            obs_mapping = json_obs_map()
            obs_mapping.load_json_mapping(json_obs_file)
            obs_mapping.build_db_mappings(platform_handle=platform_handle,
                                          db_user=db_user,
                                          db_password=db_password,
                                          db_host=db_host,
                                          db_name=db_name,
                                          db_connectionstring=db_connectionstring)
            if get_data(base_url=base_url,
                     now_time=now_str,
                     start_time=start_time,
                     end_time=end_time,
                     dest_file=file_name,
                     site_id=site_id,
                     sensor_list=sensor_list):
              process_file(obs_map=obs_mapping,
                           dest_file=file_name,
                           platform_handle=platform_handle,
                           input_queue=input_queue,
                           db_obj=db,
                           units_conversion=uom_converter)

          except(ConfigParser.Error, Exception) as e:
            logger.exception(e)
      except Exception as e:
        logger.exception(e)

      input_queue.put('STOP')
      db.disconnect()
    logger.info('Log file closing.')

  except Exception as e:
    logger.exception(e)

if __name__ == '__main__':
  main()
