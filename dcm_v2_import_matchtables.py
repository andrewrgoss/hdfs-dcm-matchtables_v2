#!/usr/bin/python
__author__ = 'agoss'

import argparse
from datetime import datetime, timedelta
import ftplib
import gzip
import os
import shutil
from string import find
import subprocess

import gen_utils
import yaml

class struct:
    def __init__(self, **entries): 
        self.__dict__.update(entries)

def find_between(s, first, last):
    try:
        start = s.index(first) + len(first)
        end = s.index(last, start)
        return s[start:end]
    except ValueError:
        return ""

def main():
    global cfg
    global placement_rule_applied
    with open("config.yml", 'r') as ymlfile:
        cfg = yaml.safe_load(ymlfile) # find and read config file
    cfg = struct(**cfg)

    file_date = datetime.strftime(datetime.strptime(datetime.strftime(datetime.now() - timedelta(1), '%Y-%m-%d'), '%Y-%m-%d'), '%Y%m%d') # format date for filename (defaults to yesterday)
    loc = cfg.dcm_v2_import_matchtables['dataDir'] # local parent directory to download files
    hdfs = cfg.dcm_v2_import_matchtables['hdfsDir'] # parent HDFS directory to upload files

    # get list of all files from Google Cloud Storage
    print 'Logging in to GCS...'
    gc_ls = subprocess.Popen(['gsutil', 'ls', cfg.dcm_v2_import_matchtables['gcs_bucket_mask']], stdout=subprocess.PIPE)
    ftp_list = gc_ls.communicate()[0].split('\n')

    # currently match_tables are daily files
    ftp_acts = [file for file in ftp_list if file.find(cfg.dcm_v2_import_matchtables['dcm_file_type']) >= 0 and file.split('_')[-4] == file_date and file[-2:] == cfg.dcm_v2_import_matchtables['dcm_file_extension']]

    # map path (aka file type/directory name) to list of files
    paths = {cfg.dcm_v2_import_matchtables['hdfs_file_type']: ftp_acts}

    for path in paths.keys():
        new_files = [file for file in paths[path]] # get all new files
        file_count = len(new_files)
        print str(file_count) + ' new files found'

        for file in new_files: # loop through new files

            print 'downloading ' + file
            match_table = find_between(file, cfg.dcm_v2_import_matchtables['dcm_file_mask'], '_' + file_date)
            loc_path = loc + path + '/' + match_table
            hdfs_path = hdfs + path

            for existing_file in os.listdir(loc_path): # clear out existing file on vm (to be replaced with new file)
                os.remove(loc_path + '/' + existing_file)
            subprocess.call(['gsutil', 'cp', file, loc_path])  # download file to vm
            subprocess.call(['hadoop', 'fs', '-rm', hdfs_path + '/' + match_table + '/*'])  # clear out existing file on hdfs (to be replaced with new file)
            subprocess.call(['hadoop', 'fs', '-put', '-f', loc_path + '/', hdfs_path + '/'])  # put to hdfs

            print file + ' processed'

        print path + ' files finished'

    print '\n**********DONE**********\n'

if __name__ == '__main__':
  try:
    main()
  except:
    gen_utils.error_logging('main() handler exception:', str(os.path.basename(__file__)))
    raise