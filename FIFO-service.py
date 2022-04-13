
import can
from can.interfaces import socketcan
import json
import time
import logging
from logging.handlers import RotatingFileHandler

import threading
import os
import sys
import struct

#global status

##### INITIALIZATION ############

FIFO_PATH = "/temp/sybok/bledata"
#confFilename = "FIFO-service-conf.json"
channel = "can0"

#################################


def validChecker(statusLock, bufferLock):
    '''Thread function called every 1 sec. It checks if the status buff of every message is set to 1 or 0 and insert 0xFF where data are invalid'''
    global status
    global buffer
    logging.info("Thread start")

    while True:

        with statusLock:
            logging.info("STATUSLOCK -> ACQUIRED")
            #if False in status:
            for i in range(len(status)):
                if status[i] == False:
                    #with bufferLock:
                    #logging.info("BUFFERLOCK -> ACQUIRED")
                    buffer[i] = format(1, '1>{}b'.format(len(buffer[i])))
                    logging.debug("Invalidation data:  {}".format(buffer[i]))
                    #logging.info("BUFFERLOCK -> RELEASED")
                else:
                    status[i] = False
            logging.debug("Status: {}".format(status))
        logging.info("STATUSLOCK -> RELEASED")

        time.sleep(1.0)



def canManager(statusLock, bufferLock, filters):
    '''Treah function that takes the can message and fill the buffer ready to be written in the pipe'''
    global status
    global buffer
    logging.info("Thread start")

    with can.interfaces.socketcan.SocketcanBus(channel="can0", receive_own_messages=False, fd=False, filters=filters) as bus:
        logging.info("SocketCAN open on channel: {}".format(channel))
        while True:
            msg = bus.recv()
            logging.debug("Msg received:\t{}\t{}".format(msg.arbitration_id, msg.data))

            for pgn in filters_dict.keys():
                if pgn in format(msg.arbitration_id, '0>16X'):

                    unpack_data = format(struct.unpack('<Q', msg.data)[0], '0>16X')
                    logging.debug("unpack data  <{}>".format(unpack_data))

                    data = unpack_data[filters_dict[pgn]["byte"][0]<<1:filters_dict[pgn]["byte"][1]<<1]
                    logging.debug("data byte  <{}>".format(data))

                    size = len(data)<<2
                    data = format(int(data, base=16), '0>{}b'.format(size))[filters_dict[pgn]["fromBit"]:filters_dict[pgn]["toBit"]]
                    logging.debug("data bin:\tsize <{}>\tdata <{}>".format(size,data))

                    with statusLock:
                        logging.info("STATUSLOCK -> ACQUIRED")
                        
                        buffer[filters_dict[pgn]["start"]] = data
                        logging.debug("buffer updated:   {}".format(buffer))
                        
                        status[filters_dict[pgn]["start"]] = True
                        logging.debug("status:  {}".format(status))
                    logging.info("STATUSLOCK -> RELEASED")

            


def buffToSend():
    '''Thread function that writes the buff in the pipe'''

    global buffer
    logging.info("Thread start") 
    

    while True:          
        try:
            with open(FIFO_PATH, 'w') as pipein:    
                buff = ""
                for data in buffer:
                    buff += format(int(data, base=2), '0>{}X'.format(len(data)>>2))
                logging.info("Buffer ready to send: <{}>".format(buff))
                buff += '\n'
        
           
                pipein.write(buff)
                logging.info("Buffer <{}> written in pipe {}".format(buff,pipein))
                #pipein.write("CIAO\n")
        except BrokenPipeError:
            logging.error("Broken pipe {}".format(pipein))
        
        #pipein.write("CIAO\n")
        time.sleep(0.2)



def confExtract(conffilename):
    """Open and extract all the CAN filters"""
    try:
        with open(conffilename, 'r') as f:
            filters_dict = json.load(f)
    except FileNotFoundError as e:
        logging.error("Configuration file not found with name:{}".format(conffilename))
        raise(e)

    logging.info("Configuration file extracted successfully")

    return filters_dict



        
if __name__ == '__main__':

    if len(sys.argv) > 1:
        db_level = sys.argv[1]
    else:
        db_level = 'ERROR'


    handler = RotatingFileHandler("fifoservice.log", mode='a', maxBytes=1000000, backupCount=1)
    logging.basicConfig(format='%(asctime)s - %(funcName)s - %(message)s', level=db_level, handlers=[handler])

    conffilename = "FIFO-service-conf.json"
    filters_dict = confExtract(conffilename)
    filters = filters_dict["filters"]
    status = filters_dict["status"]
    buffer = filters_dict["buffer"]
    '''    
    if os.path.exists(FIFO_PATH):
        os.unlink(FIFO_PATH)
    '''

    if not os.path.exists(FIFO_PATH):
        os.mkfifo(FIFO_PATH)



    statusLock = threading.Lock()
    bufferLock = threading.Lock()

    with open(FIFO_PATH,'w') as pipe:
        logging.info("Pipe open: <{}>".format(pipe))
        thread1 = threading.Thread(target=canManager, args=(statusLock, bufferLock, filters,), daemon=True)
        thread1.start()
        thread2 = threading.Thread(target=buffToSend, daemon=True)
        thread2.start()
        thread3 = threading.Thread(target=validChecker, args=(statusLock, bufferLock,), daemon=True)
        thread3.start()    
    
        thread1.join()
        logging.error("Thread1 <canManager> exit")
        thread2.join()
        logging.error("Thread2 <buffToSend> exit")
        thread3.join()
        logging.error("Thread3 <validChecker> exit")


