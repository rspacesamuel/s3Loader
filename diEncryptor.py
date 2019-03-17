###############################################################################
#COMMENTS
#Calls dataInterface.encryptData() method to encrypt a password or other data.
#This script should be on the same path as dataInterface.py and diConfig.ini
#Usage:
#python diEncryptor.py

#Author: Raj Samuel
###############################################################################

import dataInterface as di

def main():
    e = di.dataInterface('PYTHON.TEST')
    print('Warning! What you type will be echoed on screen.\nCopy-paste encrypted text into diConfig.ini.')
    secret = input('\nType password or data to be encrypted. Press Enter after typing:')
    print('Encypted text below:')
    print(e.encryptData(secret))

if __name__ == '__main__':
    main()


