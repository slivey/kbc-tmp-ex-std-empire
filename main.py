from __future__ import print_function

"__author__ = 'Jeff Huth'"
"__credits__ = 'GreatVines 2017, Twitter: @drinkdata'"

"""
Python 3 environment (unicode script fixes in place)
"""

from ftplib import FTP
import csv
import zipfile
import gzip
import io
import os
import sys
import requests
import logging
import re

import pip
pip.main(['install', '--disable-pip-version-check',
          '--no-cache-dir', 'dateparser'])
pip.main(['install', '--disable-pip-version-check', '--no-cache-dir', 'pygelf'])
pip.main(['install', '--disable-pip-version-check', '--no-cache-dir', 'tinys3'])


from keboola import docker
from pygelf import GelfTcpHandler
import pandas as pd
import tinys3
from datetime import datetime


# Environment setup
abspath = os.path.abspath(__file__)
script_path = os.path.dirname(abspath)
os.chdir(script_path)

# Logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt="%Y-%m-%d %H:%M:%S"
)
logger = logging.getLogger()
fields = {"_some": {"structured": "data"}}
logger.addHandler(GelfTcpHandler(
    host=os.getenv('KBC_LOGGER_ADDR'),
    port=os.getenv('KBC_LOGGER_PORT'),
    version="1.0",
    debug=False, **fields
))
# removes the initial stdout logging
logger.removeHandler(logger.handlers[0])

# initialize application
logging.info("Initializing Docker. CWD: " + os.getcwd())

# server docker
cfg = docker.Config('/data/')

# windows dev docker
# cfg = docker.Config(os.path.relpath('data'))

# Access the supplied parameters
logging.info("Getting config params.")
params = cfg.get_parameters()
ftp_host = cfg.get_parameters()["ftp_host"]
ftp_user = cfg.get_parameters()["ftp_user"]
ftp_password = cfg.get_parameters()["#ftp_password"]
ftp_dir = cfg.get_parameters()["ftp_dir"]
zip_pattern = cfg.get_parameters()["zip_pattern"]
zip_regex = cfg.get_parameters()["zip_regex"]
file_regex = cfg.get_parameters()["file_regex"]
s3_client = cfg.get_parameters()["s3_client"]
s3_secret = cfg.get_parameters()["#s3_secret"]
s3_bucket = cfg.get_parameters()["s3_bucket"]
s3_folder = cfg.get_parameters()["s3_folder"]


if __name__ == "__main__":
    """
    Main execution script.
    """

    # ftp connect
    logging.info("Connecting to ftp: " + ftp_host)
    ftp = FTP(ftp_host)
    ftp.login(ftp_user, ftp_password)
    ftp.set_pasv(True)
    ftp.cwd(ftp_dir)

    # s3 client init
    try:
        conn = tinys3.Connection(s3_client, s3_secret, tls=True)
        logging.info("Successfully connected." + str(conn))
    except Exception as a:
        logging.error("Could not connect. Exit!")
        sys.exit(1)

    logging.info("Getting list of remote ftp zip files: " + zip_pattern)
    for z in ftp.nlst(zip_pattern):  # Get list of zip files on remote FTP
        zname = z
        zsize = ftp.size(z)
        zmodified = datetime.strptime(ftp.sendcmd(
            'MDTM ' + z)[4:18], "%Y%m%d%H%M%S").strftime("%Y-%m-%d %H:%M:%S")
        zdttm = str(re.search(zip_regex, z).group(2))

        # Download zip file locally
        logging.info("Downloading ftp zip file: " + z)
        ftp.retrbinary(
            "RETR " + z, open(os.path.join("data/in/files", z), 'wb').write)
        zf = zipfile.ZipFile(os.path.join("data/in/files", z))

        for f in zf.namelist():  # Get list of text files in zip file
            ftype = str(re.search(file_regex, f).group(1)).replace(
                ' ', '').replace('-', '').replace('_', '')
            gz_file = ftype + '.N' + zdttm + '.gz'

            logging.info("Reading in text file: " + f)
            df = pd.read_csv(filepath_or_buffer=zf.open(f),  # read in text file to data frame
                             sep='\t',
                             quoting=csv.QUOTE_ALL,
                             quotechar='"',
                             doublequote=True,
                             low_memory=False,
                             encoding='ISO-8859-1')

            logging.info("Writing out gzip csv file: " + gz_file)
            content = df.to_csv(path_or_buf=os.path.join("data/out/files", gz_file),  # write out gzip csv
                                sep=',',
                                header=True,
                                index=False,
                                quoting=csv.QUOTE_ALL,
                                quotechar='"',
                                doublequote=True,
                                line_terminator='\n',
                                encoding='utf-8',
                                compression='gzip')

            # upload gzip csv to s3
            s3_file_path = s3_folder + '/' + gz_file

            logging.info("Opening file for upload: " + gz_file)
            fgz = open(os.path.join("data/out/files", gz_file), 'rb')

            logging.info("Uploading file to s3: " + gz_file)
            conn.upload(s3_file_path, fgz, s3_bucket)

            logging.info("Removing local file: " + gz_file)
            os.remove(os.path.join("data/out/files", gz_file))

        # upload zip to s3
        s3_file_path = s3_folder + '/' + z

        logging.info("Opening zip file for upload: " + z)
        fz = open(os.path.join("data/in/files", z), 'rb')

        logging.info("Uploading zip file to s3: " + z)
        conn.upload(s3_file_path, fz, s3_bucket)

        logging.info("Removing local zip file: " + z)
        os.remove(os.path.join("data/in/files", z))

        logging.info("Deleting remote ftp file: " + z)
        ftp.delete(z)  # remove the zip file from ftp

        logging.info("Closing ftp.")
        ftp.close()

        logging.info("Script completed.")
