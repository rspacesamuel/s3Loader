#####################################################################################################
#COMMENTS:

#General:

#Class file for S3 load. See feature list below.
#Developers can either write code to call methods of this class file (if they want customizations)
#or just call diCaller.py which calls methods of this class.
#Most customizations can be done through diConfig.ini.
#For anyone scouting the code, member variables begin with _. Eg: _path_delim
#All other variables begin with alphabet. Eg: path_delim. No global variables.


#Features:

# - extract from Oracle and load to S3
# - load files that are already spooled by Oracle to S3
# - back up loaded files to another S3 location
# - back up loaded files to a local location
# - recursively back up entire files in a folder to an S3 folder (Eg: EC2 or local files to S3)
# - All parameters neded to accomplish above tasks and to determine which of the
#   above tasks to perform are provided by you in diConfig.ini


# Usage:

#       python <module_that_implements_dateInterface.py> <SECTION_NAME_from_diConfig.ini>
#Eg:    python diCaller.py BIOSYENT.DEV


#Python:

#This code was tested in Python 2.7 and 3.x
#These features are disabled for AIX: password encryption, cx_Oracle pkg, rich formatting based on csv pkg
#If Python complains about any of the imported modules (boto3 for example),
#run on command line:
#pip install <moodule name>
#Eg: pip install boto3


#Author: Raj Samuel
#To Do: SNS notification, CloudFormation


#Revision history:

####################################################################################################

import boto3
import configparser
import os
from subprocess import Popen, PIPE
import re
import csv
import gzip
import shutil
from pathlib import Path
import datetime
import logging
import sys
import platform

if platform.system() != 'AIX':
    from cryptography.fernet import Fernet
    import cx_Oracle as cxoracle


class dataInterface:

    def __init__(self,config_section):

        #Platform detection
        self._os = platform.system()
        if self._os == 'Windows':
            self._path_delim = '\\'
        else:
            self._path_delim = '/'
        self._curr_local_dir = os.path.dirname(__file__)
        if sys.version_info[0] < 3:
            self._python_major_ver = 2
        else:
            if sys.version_info[0] == 3:
                self._python_major_ver = 3
        
        
        #Initialize the time execution began
        self._curr_year = str(datetime.datetime.now().year)
        self._curr_month = str(datetime.datetime.now().month)
        self._curr_day = str(datetime.datetime.now().day)
        self._curr_hr = str(datetime.datetime.now().hour)
        self._curr_min = str(datetime.datetime.now().minute)

        #Initialize configparser to read diConfig.ini
        self._config = configparser.ConfigParser()
        config_file_name = self._curr_local_dir + self._path_delim + 'diConfig.ini'
        self._config.read(config_file_name)

        #Read key to encrypt/decrypt passwords. Key was randomly generated using Fernet.generate_key() and saved in _key_file_name
        self._key_file_name = self._config.get(config_section,'key_file_name')
        key_file = open(self._key_file_name, 'rb')
        self._key = key_file.read()
        key_file.close()

        #If the calling program was diEncryptor, skip the rest of constructor and the destructor. Just need to run encryptData() method.
        if sys.argv[0] != "diEncryptor.py":
        
            #Initialize logging file
            self._log_file_dir = self._config.get(config_section,'log_file_dir')
            #If log location is not set in diConfig.ini, set it to current directory
            if self._log_file_dir.strip() == '' or self._path_delim not in self._log_file_dir:
                self._log_file_dir = self._curr_local_dir
            else:
                #If there is a slash at the end, remove it
                if self._log_file_dir[-1] == self._path_delim:
                    self._log_file_dir = self._log_file_dir[:-1]
            self._log_file_name = self._log_file_dir + self._path_delim + sys.argv[0].split(self._path_delim)[-1] + '.' + config_section + '.' + self._curr_year + '.' + self._curr_month + '.' + self._curr_day + '.' + self._curr_hr + '.' + self._curr_min + '.log'
            logging.basicConfig(filename=self._log_file_name, level=logging.DEBUG, format='%(asctime)s\t%(levelname)s\t%(message)s')
            if self._log_file_dir == self._curr_local_dir:
                logging.info('Writing log to current directory. Specify \"log_file_dir\" in diConfig.ini to change log directory')
            logging.info('Initializing from diConfig.ini. Reading section \"%s\".', config_section)

            #Placeholder for all decrypted passwords (Oracle, AWS etc.)
            self._decrypted_token = ''
            
            #Initialize s3 parameters and S3 (connection) object
            self._s3_region_name = self._config.get(config_section,'s3_region_name')
            self._s3_bucket_name = self._config.get(config_section,'s3_bucket_name')
            self._s3_storage_class = self._config.get(config_section,'s3_storage_class')
            self._aws_access_key_id = self._config.get(config_section,'aws_access_key_id')
            self._aws_secret_access_key_token = self._config.get(config_section,'aws_secret_access_key')
            if self._config.get(config_section,'s3_automatic_multipart_upload').lower() == 'true':
                self._s3_automatic_multipart_upload = True
            else:
                self._s3_automatic_multipart_upload = False
            if self._config.get(config_section,'s3_file_compress').lower() == 'true':
                self._s3_file_compress = True
            else:
                self._s3_file_compress = False
            if self._config.get(config_section,'s3_backup').lower() == 'true':
                self._s3_backup = True
            else:
                self._s3_backup = False
                
            self._s3_backup_bucket_name = self._config.get(config_section,'s3_backup_bucket_name')
            self._s3_backup_basefolder_name = self._config.get(config_section,'s3_backup_basefolder_name')
            self._s3_backup_storage_class = self._config.get(config_section,'s3_backup_storage_class')
            self._s3 = None

            #Initialize Oracle parameters and oracle object
            self._oracle_user_name = self._config.get(config_section,'oracle_user_name')
            self._oracle_password_token = self._config.get(config_section,'oracle_password')
            self._oracle_service_name = self._config.get(config_section,'oracle_service_name')
            self._oracle = None
            self._column_names = ""
            if self._config.get(config_section,'oracle_sqlplus_connection').lower() == 'true':
                self._oracle_sqlplus_connection = True
            else:
                self._oracle_sqlplus_connection = False
            
            #Initialize dictionary variable to hold Oracle SQL statements
            if self._config.get(config_section,'oracle_spooling').lower() == 'false':
                self._sql_stmts_dict = {}
                for name,value in self._config.items(config_section):
                    if re.match('sql_stmt_[0-9]+',name):
                        self._sql_stmts_dict[name] = value

            #Initialize variables for file format specifiers
            if self._oracle_sqlplus_connection == False:
                self._file_fmt_delim = self._config.get(config_section,'outputfile_format_delimiter').strip()
                self._file_fmt_quote = self._config.get(config_section,'outputfile_format_quote').strip()
                self._file_fmt_escape = self._config.get(config_section,'outputfile_format_escapechar').strip()
            if self._config.get(config_section,'outpufile_format_header').lower() == 'true':
                self._file_fmt_header = True
            else:
                self._file_fmt_header = False

            #Initialize Oracle's output filenames into a dictionary. This is also S3's input.
            self._sql_output_file_dict = {}
            
            #Is there a data file (spool) already created for Python? No:
            if self._config.get(config_section,'oracle_spooling').lower() == 'false':
                self._oracle_spooling = False
                logging.info('oracle_spooling = false. Program will not look for a data file, but will look for SQL statements to be run in diConfig.ini')
                
                for name,value in self._config.items(config_section):
                    if re.match('outputfile_of_sql_stmt_[0-9]+',name):
                        self._sql_output_file_dict[name] = value
                        #and delete .gz files from last run
                        last_run_file = Path(value+'.gz')
                        if last_run_file.is_file():
                            try:
                                os.remove(value+'.gz')
                            except OSError as ose:
                                logging.warning(ose)
                                #not raising this since it's not critical
                                
            #Yes, there is spool file:
            else:
                self._oracle_spooling = True
                logging.info('oracle_spooling = true. Program will look for a data file(s), and will not run any SQL statements from diConfig.ini')

                for name,value in self._config.items(config_section):
                    if re.match('spooled_outputfile_from_oracle_[0-9]+',name):
                        self._sql_output_file_dict[name] = value

                              
            #Initialize S3 folder names.
            self._s3_folder_dict = {}
            for name,value in self._config.items(config_section):
                if re.match('s3_folder_name_[0-9]+',name):
                    self._s3_folder_dict[name] = value

            #Initialize local backup variables.
            if self._config.get(config_section,'local_backup').lower() == 'true':
                self._local_backup = True
                self._local_backup_basefolder_name = self._config.get(config_section,'local_backup_basefolder_name')
            else:
                self._local_backup = False

            #Initialize folder2folder copy variables.
            if self._config.get(config_section,'folder2folder_copy').lower() == 'true':
                self._folder2folder_copy = True
                self._folder2folder_source_folder = self._config.get(config_section,'folder2folder_source_folder')
                self._folder2folder_target_s3_basefolder = self._config.get(config_section,'folder2folder_target_s3_basefolder')
            else:
                self._folder2folder_copy = False
                

            #Assertions for diConfig.ini parameters
            self.checkForInvalidConfig()
            logging.info('Initialization done.')
        

    def __del__(self):
        #In the very end compress log file and delete original. If calling program was diEncryptor, there is no log file, ignore this destructor.
        if sys.argv[0] != "diEncryptor.py":
            self.gzCompressFile(self._log_file_name)
            logging.shutdown()
            try:
                os.remove(self._log_file_name)
            except OSError as ose:
                logging.warning(ose)
                #Printing to console because this error won't make it to log file. And not raising it since it's not critical.
                print("Error deleting unzipped log file..", ose)
            
 
    def checkForInvalidConfig(self):
        #Exit program and do not attempt to load to S3 if these validations fail
        try:
            if self._os == 'AIX':
                assert self._oracle_sqlplus_connection is True, "Terminating. Platform is AIX so oracle_sqlplus_connection = true must be set in diConfig.ini"
            assert self._path_delim not in self._s3_bucket_name and '/' not in self._s3_bucket_name, "Terminating. Unexpected character found in S3 bucket name \"%s\" in diConfig.ini" % self._s3_bucket_name
            assert self._path_delim not in self._s3_backup_bucket_name and '/' not in self._s3_backup_bucket_name , "Terminating. Unexpected character found in S3 bucket name \"%s\" in diConfig.ini" % self._s3_backup_bucket_name
            assert self._s3_backup_basefolder_name[-1] != '/', "Terminating. S3 backup folder name \"%s\" ends with unexpected / in diConfig.ini" % self._s3_backup_basefolder_name
            assert self._path_delim in self._key_file_name, "Terminating. Full path required for key file \"%s\" in diConfig.ini" % self._key_file_name
            if self._local_backup == True:
                assert self._path_delim in self._local_backup_basefolder_name, "Terminating. Review path given for local backup base folder \"%s\" in diConfig.ini" % self._local_backup_basefolder_name
            if self._folder2folder_copy == True:
                assert self._path_delim in self._folder2folder_source_folder, "Terminating. Review path given for the source of folder2folder copy: \"%s\" in diConfig.ini" % self._folder2folder_source_folder
                assert self._folder2folder_target_s3_basefolder[-1] != '/', "Terminating. S3 folder name \"%s\" for folder2folder copy ends with unexpected / in diConfig.ini" % self._folder2folder_target_s3_basefolder
      
            #Loop through S3 folder names. Same folder name can be the target for more than one file.
            for folder_var,folder_name in self._s3_folder_dict.items():
                found = False
                assert folder_name[-1] != '/' "Terminating. S3 folder name \"%s\" ends with unexpected / in diConfig.ini" % folder_name
                folder_number = folder_var.split('_')[3].strip()
                #Loop through output file names (spool or Python generated)
                for file_var, file_name in self._sql_output_file_dict.items():
                    assert self._path_delim in file_name, "Terminating. Either wrong path delimiter was given or full path was not given in variable \"%s\" in diConfig.ini" % file_var
                    file_number = file_var.split('_')[4].strip()
                    if folder_number == file_number:
                        if self._oracle_spooling == False:
                            #Loop through SQL statements when spooling is turned OFF
                            for sql_var, sql_stmt in self._sql_stmts_dict.items():
                                stmt_number = sql_var.split('_')[2].strip()
                                if file_number == stmt_number:
                                    found = True
                                    break
                            assert found is True, "Terminating. File \"%s\" does not have a matching \"sql_stmt_%s\" to load from. Please review diConfig.ini." % (file_var, file_number)
                        else:
                            #Spooling is ON. Having the folder number (target) and spool file number (source) match is enough.
                            found = True
                assert found is True, "Terminating. S3 folder \"%s\" does not have a matching data file parameter to load from. Please review diConfig.ini." % folder_var

        except AssertionError as ae:
            logging.warning(ae.args[0])
            raise
            sys.exit(1)

            
    def encryptData(self, data):
	#Encrypt passwords or other data using key from keyfile.
        #Pass the data/password that needs to be encrypted.
        #Run diEncryptor.py to encrypt passwords with this method.
        if self._os == 'AIX':
            return
        f = Fernet(self._key)
        encrypted_bytes = f.encrypt(str.encode(data))
        encrypted_string = encrypted_bytes.decode()
        return encrypted_string

        
    def decryptToken(self, token):
        #Decrypt encrypted passwords (tokens) in diConfig.ini using key from keyfile.
        #This must be called before connecting to S3 or DB.
        #Pass the encrypted token in diConfig.ini as parameter. Returns decrypted string.
        if self._os == 'AIX':
            return
        f = Fernet(self._key)
        decrypted_bytes = f.decrypt(str.encode(token))
        decrypted_string = decrypted_bytes.decode()
        self._decrypted_token = decrypted_string

            
    def gzCompressFile(self, filename_with_path):
        #Compress file using gzip. Compressed file will be created in the same path with .gz appended to file name
        #Compression is recommended on S3, because Bezos charges for bytes.        
        with open(filename_with_path, 'rb') as unzippd:
            with gzip.open(filename_with_path+'.gz', 'wb') as zippd:
                shutil.copyfileobj(unzippd,zippd)

                
    def stripFilenameFromPath(self,filename_with_path):
        #Strip local path from file name to make up S3 object name aka S3 key.
        assert self._path_delim in filename_with_path, "File path has unrecognized slash or missing path. Check diConfig.ini. Expected %r ." % self._path_delim
        filename_without_path = filename_with_path.split(self._path_delim)[-1]
        return filename_without_path        

        
    def connectToS3(self):
        #Start a session that stores AWS credentials.
        #On AIX the password in diConfig.ini is in plain text, so use the token variable as is. On all other systems token is encrypted in diConfig.ini, so use the decrypted version of token.
        if self._os == 'AIX':
            decrypted_token = self._aws_secret_access_key_token
        else:
            decrypted_token = self._decrypted_token

        try:
            logging.info("Connecting to S3.. in Region: %s using Access Key ID: %s", self._s3_region_name, self._aws_access_key_id)
            aws_session = boto3.Session(aws_access_key_id=self._aws_access_key_id, aws_secret_access_key=decrypted_token, region_name=self._s3_region_name)
            #Create a resource object from session for S3
            self._s3 = aws_session.resource('s3')
        except:
            logging.warning("Failed to connect to AWS %s region using Access Key ID %s. Please review diConfig.ini.",self._s3_region_name,self._aws_access_key_id)
            raise
            sys.exit(1)


    def writeOneObjectToS3(self,s3_folder='NOTHING',s3_file='NOTHING'):
        #Write a single object to S3
        if s3_folder == 'NOTHING' and s_file == 'NOTHING':
            #No arguments were passed. The caller intends to write just one object to S3.
            s3_folder = self._s3_folder_dict['s3_folder_name_1']
            s3_file = self._sql_output_file_dict['outputfile_of_sql_stmt_1']
        else:
            if s3_file == 'NOTHING':
                #Only one argument is passed. This is a rare situation but if it happens Python's default behavior
                #is to assign it to the first positional parameter of the function (s3_folder in this case).
                s3_file = self._sql_output_file_dict['outputfile_of_sql_stmt_1']
        
        #If S3 compression is enabled, compress the file
        if self._s3_file_compress == True:
            logging.info("Compression is enabled. Data files will be compressed to gzip format.")
            self.gzCompressFile(s3_file)
            gzfile_extn = '.gz'
                
        else:
            gzfile_extn = ''
        no_path_filename = self.stripFilenameFromPath(s3_file)
        s3_key = s3_folder + '/' + no_path_filename + gzfile_extn
        
        #Moment of truth..
        try:            
            if self._s3_automatic_multipart_upload == True and os.path.getsize(s3_file+gzfile_extn) > 100000000:
                #Start multipart upload if the config variable is set and if the file size > 100MB
                logging.info("s3_automatic_multipart_upload = true and file size > 100MB. Starting multipart upload to S3.. in Bucket: %s, Key: %s, using input file: %s",self._s3_bucket_name, s3_key, s3_file+gzfile_extn)            
                #Using AWS defaults for TransferConfig() by not specifying any params. Current defaults according to boto3 documentation is:
                #TransferConfig(multipart_threshold=8388608, max_concurrency=10, multipart_chunksize=8388608, num_download_attempts=5, max_io_queue=100, io_chunksize=262144, use_threads=True)
                multipart_config = boto3.s3.transfer.TransferConfig()
                #Below upload_file() allows a callback function that can be used to display/log the progress of individual file uploads. Nice to have, but not used here.
                self._s3.Object(self._s3_bucket_name,s3_key).upload_file(Filename=s3_file+gzfile_extn, ExtraArgs={'StorageClass':self._s3_storage_class}, Config=multipart_config)
            else:
                #Start a normal load without splitting the data file
                logging.info("s3_automatic_multipart_upload = false. Writing to S3 .. in Bucket: %s, Key: %s, using input file: %s",self._s3_bucket_name, s3_key, s3_file+gzfile_extn)            
                self._s3.Object(self._s3_bucket_name,s3_key).put(Body=open(s3_file+gzfile_extn,'rb'), StorageClass=self._s3_storage_class)
        except AttributeError as ae:
            logging.warning("Failed writing to S3.. in Bucket: %s, Key: %s, using input file: %s",self._s3_bucket_name, s3_key, s3_file+gzfile_extn)
            logging.warning(ae)
            raise

        try:
            #Delete the compressed local copy that was loaded to S3. Don't do anything if compression is disabled, retaining original data files.
            if self._s3_file_compress == True:
                os.remove(s3_file+gzfile_extn)
        except OSError as ose:
            logging.warning(ose)
            #Not raising this since it's not critical


    def writeObjectsToS3(self):
        #Wrapper function for writeOneObjectToS3. This will write one or more objects.
        for name,folder_name in self._s3_folder_dict.items():
            folder_number = name.split('_')[3].strip()
            for varname, file_name in self._sql_output_file_dict.items():
                file_number = varname.split('_')[4].strip()
                if folder_number == file_number:
                    self.writeOneObjectToS3(folder_name,file_name)
                    break

    def writeLocalFolderToS3Folder(self):
        #Copies the entire contents of a local folder to S3 folder by calling writeOneObjectToS3()
        #As of writing this, AWS API for folder-to-folder copy is only available in Java/C# and not in Python. Hence the custom logic below.
        #If source folder ends with path delimiter, remove ending delimiter
        if self._folder2folder_copy == False:
            return
        if self._folder2folder_source_folder[-1] == self._path_delim:
            self._folder2folder_source_folder = self._folder2folder_source_folder[:-1]        
        source_folder_without_path = self._folder2folder_source_folder.split(self._path_delim)[-1]
        data_month_bkp_folder = self._folder2folder_target_s3_basefolder + '/' + self._curr_year + '/' + self._curr_month + '/' + self._curr_day
        if self._folder2folder_copy == True:
            for curr_path, subfolders, files_in_curr_path in os.walk(self._folder2folder_source_folder):
                if len(files_in_curr_path) == 0:
                    continue
                else:
                    for each_file in files_in_curr_path:
                        #The first replace() gets directory tree "under" source folder by erasing the tree above it. Second replace() makes sure we have S3 path delimiter (/)
                        curr_folder = source_folder_without_path + curr_path.replace(self._folder2folder_source_folder,'').replace(self._path_delim,'/')
                        logging.info("Writing to S3.. in Bucket: %s, Key: %s, using input file: %s",self._s3_bucket_name, data_month_bkp_folder+'/'+curr_folder+'/'+each_file, curr_path+self._path_delim+each_file)
                        try:
                            self.writeOneObjectToS3(data_month_bkp_folder+'/'+curr_folder, curr_path+self._path_delim+each_file)
                        except AttributeError as ae:
                            logging.warning("Failed writing to S3.. in Bucket: %s, Key: %s, using input file: %s",self._s3_bucket_name, data_month_bkp_folder+'/'+curr_folder+'/'+each_file, curr_path+self._path_delim+each_file)
                            logging.warning(ae)
                            raise

    def backupS3Objects(self):
        #Backup S3 objects to another S3 location, for example before they get overwritten.
        if self._s3_backup == False:
            logging.info("s3_backup = false. Won't back up data files into S3.")
            return
        if self._s3_file_compress == True:
            gzfile_extn = '.gz'
        else:
            gzfile_extn = ''

        data_month_bkp_folder = self._s3_backup_basefolder_name + '/' + self._curr_year + '/' + self._curr_month + '/' + self._curr_day
        for name,folder_name in self._s3_folder_dict.items():
            folder_number = name.split('_')[3].strip()
            for varname, file_name in self._sql_output_file_dict.items():
                file_number = varname.split('_')[4].strip()
                if folder_number == file_number:
                    no_path_filename = self.stripFilenameFromPath(file_name)

                    #source:
                    s3_source_key = folder_name + '/' + no_path_filename + gzfile_extn
                    s3_source = {'Bucket' : self._s3_bucket_name,
                                 'Key' : s3_source_key
                                }
                    #target key:
                    s3_target_key = data_month_bkp_folder + '/' + folder_name + '/' + no_path_filename.split('.')[0] + '.' + self._curr_year + '.' + self._curr_month + '.' + self._curr_day + '.' + no_path_filename.split('.')[-1] + gzfile_extn
                    
                    #copy object to target aka back up
                    #this is the only place a botocore client call is placed instead of resource call (primarily because:
                    #code is more readable and resource call for copy_object doesn't seem to have a StorageClass feature yet)
                    #First check if the object to be backed up exists. This takes care of first time runs of new files.
                    try:
                        bucket_listing_dict = self._s3.meta.client.list_objects_v2(Bucket=self._s3_bucket_name,Prefix=s3_source_key)
                    except:
                        logging.warning("Failed to access S3")
                        raise
                    if bucket_listing_dict.get('KeyCount') > 0:
                        logging.info("Backing up S3 Key: %s in Bucket: %s to target S3 Key: %s in backup bucket: %s. Backed up key will be assigned Storage Class: %s",s3_source['Key'], s3_source['Bucket'], s3_target_key, self._s3_backup_bucket_name, self._s3_backup_storage_class)
                        self._s3.meta.client.copy_object(Bucket=self._s3_backup_bucket_name, CopySource=s3_source, Key=s3_target_key, StorageClass=self._s3_backup_storage_class)
                    

    def backupLocalFiles(self):
        #Backup local data files to another location on that local server (usually Oracle server)
        #Do not call this function before writing to S3 because compression is expected to have happened by now(check compression comments below)
        if self._local_backup == False:
            logging.info("local_backup = false. Won't back up on local server.")
            return
        if self._local_backup_basefolder_name[-1] != self._path_delim:
            self._local_backup_basefolder_name += self._path_delim
        data_month_bkp_folder = self._local_backup_basefolder_name + self._curr_year + self._path_delim + self._curr_month + self._path_delim + self._curr_day + self._path_delim
        try:
            os.makedirs(data_month_bkp_folder)
            logging.info("Local backup folder \"%s\" created.", data_month_bkp_folder)
        except OSError as ose:
            logging.warning("Local backup folder \"%s\" already exists or unable to create. Attempting to back up here..", data_month_bkp_folder)
        
        #Compress data files if they are not compressed already, then back up. It's usually already compressed by the time we get here.
        for varname, file_name in self._sql_output_file_dict.items():
            filename_without_path = self.stripFilenameFromPath(file_name)
            try:
                #Compress (compress only if below variable is set to false. The assumption is, when set to True compression already happened in writeOneObjectToS3())
                if self._s3_file_compress == False:
                    self.gzCompressFile(file_name)
                #Back up
                bkp_src = file_name + ".gz"
                bkp_tgt = data_month_bkp_folder + filename_without_path + ".gz"
                shutil.copyfile(bkp_src,bkp_tgt)
                #Delete compressed copy after backup
                os.remove(bkp_src)
                logging.info("Local backup successful. Source: %s Target: %s", bkp_src, bkp_tgt)
            except OSError as ose:
                logging.warning(ose)
                logging.info("One of these failed: Data file compression or Backing up compressed file locally or Deleting of backed up files from source. Source: %s Target: %s", bkp_src, bkp_tgt)
                logging.info("Continuing without terminating.")
            
        
    def connectToOracleDB(self):
        #If data files are already spooled don't connect to Oracle DB
        if self._oracle_spooling == True:
            logging.info("oracle_spooling = true. Won't attempt to connect to Oracle DB. Data files are expected to be available.")
            return
        #On AIX the password is in diConfig.ini in plain text, so use the token variable as is. On all other systems token is encrypted in diConfig.ini, so use the decrypted version of token.
        if self._os == 'AIX':
            decrypted_token = self._oracle_password_token
        else:
            decrypted_token = self._decrypted_token
        
        try:
            if self._oracle_sqlplus_connection == True:
                #Below Popen() is commented out because Popen() is being called for each SQL separately in the extract method instead of once below to avoid pipe malfunction. Return now.
                return                
                #self._oracle = Popen(['sqlplus', '-S', self._oracle_user_name+'/'+decrypted_token+'@'+self._oracle_service_name], stdin=PIPE, stdout=PIPE, stderr=PIPE, universal_newlines=True)

            else: 
                logging.info("oracle_sqlplus_connection = false. Connecting to Oracle via InstantClient. cx_Oracle pkg will be used to query data.")
                self._oracle = cxoracle.connect(self._oracle_user_name+'/'+decrypted_token+'@'+self._oracle_service_name)
            logging.info("Connection to Oracle successful.")
        except:
            logging.warning("Failed to connect to Oracle using %s/****@%s. Please review diConfig.ini.",self._oracle_user_name,self._oracle_service_name)
            raise
            sys.exit(1)

            
    def formatSQLforSQLPlus(self, sql_stmt):
        #This method is called from extractOracleToFile()
        #Tilde(~) separated no-quotes rows is the only option for SQLPlus currently (can be changed).
        
        #Strip leading/trailing whitespace from SQL
        sql_stmt = sql_stmt.strip()
        #Add semicolon at the end
        if sql_stmt[-1] != ';':
            sql_stmt+=';'
        #Split SQL into SELECT and FROM clauses
        pattern = r'(^[Ss][Ee][Ll][Ee][Cc][Tt] .* [Ff][Rr][Oo][Mm])'
        split_sql_stmt = re.split(pattern,sql_stmt)
        if len(split_sql_stmt) == 1:
            logging.warning("Terminating. Expected SQL statments in the form \"SELECT col1,col2.. FROM..\", but found something else: %s .", sql_stmt)
            raise
            sys.exit(1)
        select_clause = split_sql_stmt[1]
        from_clause = split_sql_stmt[2]
	#Below column names will be used as a header in output file:
        self._column_names = select_clause.replace("SELECT","",1).replace("FROM","",1).replace(",", "~").strip()                        
        #Replace commas with ||'~'|| for SQLPlus   
        formatted_select_clause = select_clause.replace(",", "|| \'~\' ||")
        #linesize to avoid line break at col 81, numwidth to avoid exp format, pagesize to avoid col headers every few rows, feedback off to avoid N rows selected msg.
        full_sql = "set linesize 32767;\nset numwidth 38;\nset pagesize 0;\nset underline off;\nset feedback off;\n" + formatted_select_clause + " " + from_clause
        if self._python_major_ver == 3:
            #final_full_sql = full_sql.encode() #commenting out leaves this version check useless, but leaving it as is for now.
            final_full_sql = full_sql
        else:
            final_full_sql = full_sql
        return final_full_sql
        
        
    def extractOracleToFile(self):
        #Run SQLs in diConfig.ini and write resultset to output file (mentioned in diConfig.ini)
        #But if data files are already spooled don't bother
        if self._oracle_spooling == True:
            return
        if self._oracle_sqlplus_connection == True:
            
            #Extract data via SQLPlus            
            for name, sql_stmt in self._sql_stmts_dict.items():
                #For each SQL statement, get the corresponding _N output file. (That is, for sql_stmt_number_1 get outputfile_of_sql_stmt_number_1, and so on..)
                stmt_number = name.split('_')[2].strip()
                formatted_SQL = self.formatSQLforSQLPlus(sql_stmt)
                for varname, filename in self._sql_output_file_dict.items():
                    outputfile_number = varname.split('_')[4].strip()
                    if  outputfile_number == stmt_number:
                        logging.info("oracle_sqlplus_connection = true. Connecting to Oracle via SQLPlus. cx_Oracle pkg won't be used.")
                        try:
                            #Call Popen() to create a connection process for each sql_stmt. Calling this just once in ConnectToOracleDB() for all SQLs was messy.
                            self._oracle = Popen(['sqlplus', '-S', self._oracle_user_name+'/'+self._oracle_password_token+'@'+self._oracle_service_name], stdin=PIPE, stdout=PIPE, stderr=PIPE, universal_newlines=True)
                            self._oracle.stdin.write(formatted_SQL)
                            stdout, stderr = self._oracle.communicate()
                            with open(filename, 'w') as output_file: 
                                line = ""
                                current_line_is_header = True
                                #Write header
                                if self._file_fmt_header == True:
                                    output_file.write(self._column_names+"\n")
                                #for char in stdout.decode("utf-8","ignore"):  #this is commented out, but may be needed in Python 3! (maybe not - tested ok in Python 3)
                                for char in stdout:
                                    line+=char
                                    if char == '\n':
                                        output_file.write(line)
                                        line = ""
                            logging.info("Successfully wrote Oracle data to %s", filename)
                            break
                        
                        except (OSError, ValueError) as ora_err:
                            logging.warning("Failed to query Oracle or write to %s", filename)
                            logging.warning(ora_err)
                            logging.warning(stderr)
                            raise
                            sys.exit(1)
                            
        else:
            #Extract data via cx_Oracle and InstantClient           
            for name, sql_stmt in self._sql_stmts_dict.items():
                #For each SQL statement, get the corresponding _N output file. (That is, for sql_stmt_number_1 get outputfile_of_sql_stmt_number_1, and so on..)
                stmt_number = name.split('_')[2].strip()
                for varname, filename in self._sql_output_file_dict.items():
                    outputfile_number = varname.split('_')[4].strip()
                    if  outputfile_number == stmt_number:
                        try:
                            cursor = self._oracle.cursor()
                            cursor.execute(sql_stmt)
                            file = open(filename,'w',newline='') #rewrite this using "with"
                            csvout = csv.writer(file, delimiter=self._file_fmt_delim, quoting=eval('csv.'+self._file_fmt_quote), escapechar=self._file_fmt_escape)
                            #Write header
                            if self._file_fmt_header == True:
                                column_names = [item[0] for item in cursor.description]
                                csvout.writerow(column_names)
                            csvout.writerows(cursor)
                            file.close()
                            logging.info("Successfully wrote Oracle data to %s", filename)
                            break
                        except OSError as ose:
                            logging.warning("Failed to query Oracle or write to %s", filename)
                            logging.warning(ose)
                            raise
                            sys.exit(1)
            self._oracle.close()


