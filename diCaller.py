################################################################################
#COMMENTS

#General:

#This module calls dataInterface class methods to accomplish one or more of
#below features. This script should be on the same path as dataInterface.py and diConfig.ini.
#This is a boilerplate caller program. That is, based on what parameters are set
#in diConfig.ini, not all methods below may get executed though all of them
#are getting called below.

#Features:

# - extract from Oracle and load to S3
# - load files that are already spooled by Oracle to S3
# - back up loaded files to another S3 location
# - back up loaded files to a local location
# - recursively back up entire files in a folder to an S3 folder (Eg: EC2 or local files to S3)
# - All parameters neded to accomplish above tasks and to determine which of the
#   above tasks to perform are provided by you in diConfig.ini

#Usage:

#python <module_that_implements_dateInterface.py> <section-name-in-diConfig.ini>
#Eg: python diCaller.py BIOSYENT.PROD

#Python:

#This code was tested in Python 2.7 and 3.x
#These features are disabled for AIX: password encryption, cx_Oracle pkg, rich formatting based on csv pkg
#If Python complains about any of the imported modules (boto3 for example),
#run on command line:
#pip install <moodule name>
#Eg: pip install boto3

#Author: Raj Samuel
################################################################################

import dataInterface as di
import sys

def main():
    
    if len(sys.argv) > 1:
        config_section = sys.argv[1]
    else:
        config_section = 'PYTHON.TEST'
    a = di.dataInterface(config_section)
    a.decryptToken(a._oracle_password_token)
    a.connectToOracleDB()
    a.extractOracleToFile()
    a.decryptToken(a._aws_secret_access_key_token)
    a.connectToS3()
    a.backupS3Objects()    
    a.writeObjectsToS3()
    a.backupLocalFiles()
    a.writeLocalFolderToS3Folder()

if __name__ == '__main__':
    main()


