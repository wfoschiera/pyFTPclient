# coding=utf-8
__author__ = 'Roman Podlinov'
# -*- coding: utf-8 -*-


import threading
import logging
import ftplib
import socket
import time

##################################################
#import progressbar
#available here: https://code.google.com/p/python-progressbar/
from progressbar import Bar, ETA, \
    FileTransferSpeed, Percentage, \
    ProgressBar, RotatingMarker
    
#global variable to start download progress bar
file_size = 0

##################################################

def setInterval(interval, times = -1):
    # This will be the actual decorator,
    # with fixed interval and times parameter
    def outer_wrap(function):
        # This will be the function to be
        # called
        def wrap(*args, **kwargs):
            stop = threading.Event()

            # This is another function to be executed
            # in a different thread to simulate setInterval
            def inner_wrap():
                i = 0
                while i != times and not stop.isSet():
                    stop.wait(interval)
                    function(*args, **kwargs)
                    i += 1

            t = threading.Timer(0, inner_wrap)
            t.daemon = True
            t.start()
            return stop
        return wrap
    return outer_wrap

#I put some arguments to make possible set ftp folder and local folder
class PyFTPclient:
    def __init__(self, host, FTPfolder, port, login, passwd, LocalFolder, monitor_interval = 30):
        self.host = host
        self.FTPfolder = FTPfolder
        self.port = port
        self.login = login
        self.passwd = passwd
        self.LocalFolder = LocalFolder
        self.monitor_interval = monitor_interval
        self.ptr = None
        self.max_attempts = 15
        self.waiting = True

    def DownloadFile(self, dst_filename, LocalFolder, local_filename = None):
        res = ''
        if local_filename is None:
            local_filename = dst_filename
        with open(self.LocalFolder + local_filename, 'w+b') as f:
            print self.LocalFolder + local_filename
            self.ptr = f.tell()
            
            @setInterval(self.monitor_interval)
            def monitor():
                if not self.waiting:
                    i = f.tell()
                    try:                    
                        if self.ptr < i:
                            logging.debug("%d  -  %0.1f Kb/s" % (i, (i-self.ptr)/(1024*self.monitor_interval)))
                            self.ptr = i
                        else:
                            ftp.close()
                            #break ???
                    except IOError:
                        pass

            def connect():
                ftp.connect(self.host, self.port)
                ftp.login(self.login, self.passwd)
                ftp.cwd(self.FTPfolder)
                # optimize socket params for download task
                ftp.sock.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)
                ftp.sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_KEEPINTVL, 75)
                ftp.sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_KEEPIDLE, 60)

            ftp = ftplib.FTP()
            ftp.set_debuglevel(2)
            ftp.set_pasv(True)

            connect()
            ftp.voidcmd('TYPE I')
            dst_filesize = ftp.size(dst_filename)

            mon = monitor()
            
############################################################
            widgets = ['Test: ', Percentage(), ' ', Bar(marker=RotatingMarker()),
               ' ', ETA(), ' ', FileTransferSpeed(),' ']
            pbar = ProgressBar(widgets=widgets, maxval=dst_filesize).start()

            def callback(data):
                global file_size
                file_size += len(data)
                pbar.update(file_size)
                f.write(data)
                #print 'my callback function'
                #print file_size

############################################################            
            
            while dst_filesize > f.tell():
                try:
                    connect()
                    self.waiting = False
                    # retrive file from position where we were disconnected
                    res = ftp.retrbinary('RETR %s' % dst_filename, callback = callback) if f.tell() == 0 else \
                              ftp.retrbinary('RETR %s' %(dst_filename), callback = callback, rest=f.tell())
                
                except:
                    self.max_attempts -= 1
                    if self.max_attempts == 0:
                        mon.set()
                        logging.exception('')
                        raise
                    self.waiting = True
                    logging.info('waiting 30 sec...')
                    time.sleep(30)
                    logging.info('reconnect')

            mon.set() #stop monitor
            ftp.close()
            
            if not res.startswith('226 Transfer complete'):
                logging.error('Downloaded file {0} is not full.'.format(dst_filename))
                # os.remove(local_filename)
                return None
                
                
            return 1


def run(host, FTPfolder, port, login, passwd, LocalFolder, files):
    obj = PyFTPclient(host, FTPfolder, port, login, passwd, LocalFolder)
    obj.DownloadFile(files, LocalFolder)
    #Logging doesn't work for me, so I disable it
    #logging.basicConfig(filename='/media/fox/Data/TestsYield/log/download.log',format='%(asctime)s %(levelname)s: %(message)s',level=logging.DEBUG)
    #logging.basicConfig(format='%(asctime)s %(levelname)s: %(message)s', level=logging.level)


#uncomment to run the script. 
#example
#in your script:
#import pyftpclientProgressBar.py as pyftp
#pyftp.run ('FTP SERVER', 'FTP FOLDER', port, username, passwd, local folder to save, filename)
#pyftp.run ('ftp1.cptec.inpe.br', 'modelos/io/tempo/regional/Eta05km/CNEN/ultimo/', 21, 'anonymous', 'anonymous', '/media/Data/temp/', '1404241272_50')



#keep this to remember the original file.
if __name__ == "__main__":
    #        logging.basicConfig(filename='/var/log/dreamly.log',format='%(asctime)s %(levelname)s: %(message)s',level=logging.DEBUG)
    logging.basicConfig(format='%(asctime)s %(levelname)s: %(message)s', level=cfg.logging.level)
    pyftp.run ('ftp1.cptec.inpe.br', 'modelos/io/tempo/regional/Eta05km/CNEN/ultimo/', 21, 'anonymous', 'anonymous', '/media/Data/temp/', '1404241272_50')


