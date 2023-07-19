#!/usr/bin/env python3

# License MIT License
# 
# Copyright (c) 2021-2023 Michael Leon ( in service of the
#   Data Services Group (DSG) as well as AQPI at the
#   Earth Systems Research Laboratory (ESRL) of
#   National Oceanic Atmospheric Administration (NOAA) )
# 
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
# 
# The above copyright notice and this permission notice shall be included in all
# copies or substantial portions of the Software.
# 
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
# SOFTWARE.

'''
Description
###########
:synopsis: A script developed to begin to unravel the udp packets broadcast from a noaaport satellite dish receiving a NOAA SBN data feed.
:usage: ``./noaaport_reader.py <channel>`` # where <channel> is a number from 1 to 10

Dependencies
############
python v3.6.8+ standard module libraries

NOAAPort Channel Specifications
###############################
>>> 
   Channel	Name			ID	Multicast IP	Port	PID
   1		NMC/NWSTG		5	224.0.1.1	1201	101
   8		EXP (Experimental)	11	224.0.1.8	1208	106
   9		GRW (GOES-R West)	12	224.0.1.9	1209	107
   10		GRE (GOES-R East)	13	224.0.1.10	1210	108

======= ===== == ============ ==== ===  ================================================
Channel Name  ID Multicast IP Port PID  Full Name
======= ===== == ============ ==== ===  ================================================
1       NMC   5  224.0.1.1    1201 101  NCEP / NWSTG
2       GOES     224.0.1.2    1202 102  GOES / NESDIS
3       NMC2     224.0.1.3    1203 103  NCEP / NWSTG2
4       NOPT     224.0.1.4    1204 104  Optional Data - OCONUS Imagery / Model
5       NPP      224.0.1.5    1205 105  National Polar-Orbiting Partnership / POLARSAT
6       ???      224.0.1.6    1206 ???  National Blend of Models
8       EXP   11 224.0.1.8    1208 106  Experimental
9       GRW   12 224.0.1.9    1209 107  GOES-R Series West
10      GRE   13 224.0.1.10   1210 108  GOES-R Series East
1       NWWS     224.1.1.1    1201 201  Weather Wire
======= ===== == ============ ==== ===  ================================================

Sphinxdocs notes:
#################

#. Add path to directory containing scripts in source/conf.py
     sys.path.insert(0, os.path.abspath('software/auto_python'))
#. Add your python script(s) to this directory.
#. Run the below to auto-generate pages (before running 'make html')
     sphinx-apidoc -f -o source/software/auto_python/ source/software/auto_python/
#. Add link to new auto generated modules(.rst) to scripts/python/index.rst listing

Code
####
''' 

desc = "udp socket reader for reading noaaport data"

import socket
import struct
import threading
import signal
import sys
import os
from collections import namedtuple
from datetime import datetime, timezone
import time
import argparse

class noaaport_reader():
  '''This is a class that defines the data structures and communication defaults used for
  listening to and reading a NOAAPort data stream.\n
  Constructor method - defines channels, sockets, data structures and metadata used to read a noaaport stream
  
  Define the _struct(ure)s and bytes that we expect to receive - to unpack the byte_range\n
  Use namedtuples to define _meta(data) for expanding each header into its varables
  
  sbn_message = '!' # the data portion of the packet\n
  frame = flh + sbn + psh + wmo + ccb + sbn_message) # sometimes, depending on psh_start_struct
  '''

  def __init__(self, channel: int):
    self.channels = list(range(1,12))
    '''expected noaaport channels of streaming udp data'''
    self.set_channel(channel)
    self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM, socket.IPPROTO_IP) # define our socket
    self.sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1) # and its options

    self.data_dir = os.path.join('/data/temp/', self.channel)
    if not os.path.isdir(self.data_dir): os.makedirs(self.data_dir)

    self.flh_struct = struct.Struct('!8B L 2H')
    '''16 bytes for the Frame Level Header # unsigned char[8], long, unsigned short[2]'''
    self.flh_meta = namedtuple('FrameLevelHeader', ' '.join([
      'HDLC_address',    # 
      'HDLC_control',    # 
      'SBN_version',     # version of the sbn protocol?
      'SBN_control',     # 
      'SBN_command',     # Product format data transfer (5: timing sync frame)
      'SBN_data_stream', # noaaport channel ID?
      'SBN_source',      # (text, graphics, goesE?)
      'SBN_destination', # 
      'SBN_sequence_number', # the way we track packets, 1 at a time 
      'SBN_run',         #  
      'SBN_checksum',
    ]))
    '''the Frame Level Header metadata as a namedtuple\n
    One relevant variable, the noaaport channel ID uses these values (need to verify/correct)::

         1 GOES East
         2 GOES West
         3 Reserved
         4 Non-GOES Imagery/DCP
         5 NCEP/NWSTG
         6 Reserved
         7 Reserved '''

    self.sbn_struct = struct.Struct('!2B 4H 2B L')
    '''16 bytes for the Satellite Broadcast Network Header'''
    self.sbn_meta = namedtuple('SatelliteBroadcastNetworkHeader', ' '.join([
      'PDHVN',
      'Transfer_type', # 16:compressed, 4:end, 1:start
      'Header_length',
      'Block_number',
      'Data_block_offset',
      'Data_block_size',
      'Records_per_block',
      'Blocks_per_record',
      'Product_sequence_number',
    ]))
    '''the Satellite Broadcast Network Header (aka the Product-definition header) metadata as a namedtuple\n
    A variable to key off of, the Transfer_type uses these values::

      * Transfer-type bit-mask:          # from github:unidata/noaaport
      *   0x01  Start-of-product frame
      *   0x02  Product transfer. Set in Start-of-product frame
      *   0x04  End-of-product frame
      *   0x08  Product error
      *   0x10  Data-block is zlib(3) compressed
      *   0x20  Product abort
      *   0x40  ???
      #define PDH_START_PRODUCT          1 # from pdinrs-src.tar.gz
      #define PDH_TRANSFER_IN_PROGRESS   2
      #define PDH_LAST_PACKET            4
      #define PDH_PRODUCT_ERROR          8
      #define PDH_PRODUCT_ABORT         32
      #define PDH_OPTION_HEADER_INC     64
      #define CCB_SIZE_BYTE_SHIFT        2
      #define HE_SIZE                 8192  '''

    self.psh_start_struct  = struct.Struct('!2B H')
    '''The first few bytes of the Product Specification Header'''
    self.psh_start_meta = namedtuple('ProductSpecHeader_Start', ' '.join([
      'field_num',
      'field_type',
      'psh_size', # if > 0, we have a psh
    ]))
    '''the *start* of the Product Specification Header metadata as a namedtuple'''

    self.psh_struct = struct.Struct('!2B H 2B 2H 2B 3H 2B 3L 2H')
    '''36 bytes for the Product Specification Header'''
    self.psh_meta = namedtuple('ProductSpecHeader', ' '.join([
      'field_num',
      'field_type',
      'psh_size',
      'psh_ver',
      'psh_flag',
      'awips_size',
      'bytes_per_rec',
      'my_type',
      'category',
      'prod_code',
      'num_frags',
      'next_headoff',
      'reserved',
      'source',
      'segnum',
      'recv_time',
      'send_time',
      'currRunIOd',
      'origRunId',
    ]))
    '''the Product Specification Header metadata as a namedtuple'''

    self.wmo_struct = struct.Struct('!11s 5s 6s 8s 3s 7s')
    '''42 bytes for the WMO Header (alphanumeric?)'''
    self.wmo_meta = namedtuple('WMOHeader', ' '.join([
      # wmo_header, rstation, wmo_id, station, ddhhmm[:6], channel, Ymd, wmo_size
      'wmo_header', # the full WMO header string provided
      'rstation',   # originating/sending/receiving station? 
      'wmo_id',     # 
      'station',    # obs station format is 'KKKK'
      'wmo_time',   # format is 'ddhhmm', using current year and month
      'wmo_awips',  # user defined (eg: channel)
      'wmo_ymd',    # more human readable wmo_time # injected by this script
      'size',       # the number of bytes used for the wmo header (+'\r\r\n') # injected by this script
    ]))
    '''the World Meteorological Organization Header (aka, the "COMMS" header) metadata as a namedtuple'''

  def set_channel(self, channel : int) -> 'sets the channel, ip and port':
    '''Sets the channel we will be listening on (there are ten available: 1-10).'''
    self.channel = channel
    if channel == '11': # Weather radio announcements
      self.ip = "224.1.1.1"
      self.port = 1201
    else:
      self.ip = "224.0.1." + channel # eg: 224.0.1.4
      self.port = 1200 + int(channel) # eg: 1204

  def stop_listening(self):
    '''Stop listening to an open channel / socket'''
    print('closing socket we were listening on.. %s:%d' % (self.ip, self.port))
    self.sock.shutdown
    self.sock.close()

  def handler(self, signum, frame):
    '''Set up signal handling - to gracefully exit on any catchable signal'''
    signame = signal.Signals(signum).name
    print(f'Signal handler called with signal {signame} ({signum})')
    self.stop_listening()
    os._exit(0) # 

  def connect_to_socket(self):
    '''Set up options for, bind to, and return a socket to listen on'''
    self.sock.bind(('', self.port)) # using the channel/port we are listening on, bind to the socket
    mreq = struct.pack("sl", socket.inet_aton(self.ip), socket.INADDR_ANY) # define our memory request # "=4sl"?
    # sock.setsockopt(socket.IPPROTO_IP, socket.IP_ADD_MEMBERSHIP, socket.inet_aton(MCAST_GRP) + socket.inet_aton(host)) # another way to listen?
    self.sock.setsockopt(socket.IPPROTO_IP, socket.IP_ADD_MEMBERSHIP, mreq) # set our options and listen

    # set up signal handling
    catchable_sigs = set(signal.Signals) - {signal.SIGKILL, signal.SIGSTOP}
    for sig in catchable_sigs:
      signal.signal(sig, self.handler)
  
  def read_frame_header(self, flh_raw: bytes, expected_sbn_seq: int) -> 'flh: namedtuple, expected_sbn_seq: int' :
      '''Given a raw set of bytes (expects the first 16 bytes of a noaaport packet) and an expected sbn sequence,
      read the Frame Level Header and calculate the next expected sbn sequence. \n
      This and other noaaport headers are unpacked into their expected _struct(ures), and assigned to their
      defined _meta(data) variables.\n
      Track the expected_sbn_sequence, make sure it increments by 1 (not sure where we're joining in the sbn sequencing)\n
      Produce a warning if an sbn packet arrives out of sequence.'''
      flh = self.flh_meta(*self.flh_struct.unpack(flh_raw)) # unpack the frame level header
      if expected_sbn_seq != int(flh.SBN_sequence_number):
        s_print("   Received an SBN message/packet out of sequence! Expected %s, got %s" % (expected_sbn_seq, flh.SBN_sequence_number))
      expected_sbn_seq = int(flh.SBN_sequence_number) + 1 # we expect the next packet to increment by 1
      if args.verbose >= 2: s_print(' ', flh)
      if args.verbose >= 1: s_print(f'    flh.SBN_sequence_number: {flh.SBN_sequence_number}')
      return flh, expected_sbn_seq

  def read_sbn_header(self, sbn_raw : bytes, product_seq : int) -> 'sbn: namedtuple, product_seq: int' :
      '''Given a raw set of bytes (expects bytes 16:32 of a noaaport packet), read the Satellite Broadcast Network Header\n
      Track the current product_sequence, assemble packets for the same product, make sure the new product increments by 1'''
      sbn = self.sbn_meta(*self.sbn_struct.unpack(sbn_raw)) # unpack the satellite broadcast network header
      if args.verbose >= 2: s_print(' ', sbn)
      if args.verbose >= 1: s_print(f'    Block_number: {sbn.Block_number}, Data_block_size: {sbn.Data_block_size} \
                Header_length: {sbn.Header_length}, Transfer_type: {sbn.Transfer_type}, Product_sequence_number: {sbn.Product_sequence_number}')
      # if product_seq == sbn.Product_sequence_number: # compare previous packet to current - same product?
      #   if sbn.Transfer_type == 2 and sbn.Header_length == 16 and sbn.Block_number > 0:
      #     s_print("    Continuation packet - appending to existing product...")
      #     if sbn.Data_block_size == 4000:
      #       s_print("    Sandwich packet - product requires previous and future packets to assemble")
      #   if sbn.Transfer_type == 6:
      #     s_print("    Last packet needed to assemble the product. Need to complete processing and cleanup.")
      # if (sbn.Product_sequence_number != product_seq + 1) : # if next product is not the previous, it's seq should increment by 1
      #   s_print("    received a product out of sequence! Last product was %s, just got %s" % (product_seq, sbn.Product_sequence_number))
      product_seq = sbn.Product_sequence_number # set product_sequence to last received packet value
      return sbn, product_seq 

  def read_psh_header_start(self, psh_start_raw : bytes) -> 'psh_start: namedtuple' :
      '''Given a raw set of bytes (expects bytes 32:36 of a noaaport  packet), read the *start* of the Product Specification Header'''
      psh_start = self.psh_start_meta(*self.psh_start_struct.unpack(psh_start_raw)) # unpack the product spec header start
      if args.verbose >= 2: s_print('   ', psh_start)
      if psh_start.psh_size == 8192:
        s_print("     psh_size signifies we should not try to read any further headers.")
      return psh_start

  def read_psh_header(self, psh_raw: bytes) -> 'psh: namedtuple':
      '''Given a raw set of bytes (expects bytes 32:58 of a noaaport packet), read the Product Specification Header'''
      psh = self.psh_meta(*self.psh_struct.unpack(psh_raw)) # unpack the product spec header
      if args.verbose >= 2:
        s_print(' ', psh)
        s_print(f'      recv_time:{human_time(psh.recv_time)}, send_time:{human_time(psh.send_time)}')
        ## s_print("    Block_number:", sbn.Block_number, "Data_block_size:", sbn.Data_block_size, \
        ##        "Header_length:", sbn.Header_length, "Transfer_type:", sbn.Transfer_type, "Product_sequence_number", sbn.Product_sequence_number)
      return psh, human_time(psh.recv_time), human_time(psh.send_time)

  def read_wmo_header(self, wmo_raw : bytes) -> str:
      '''Given a raw set of bytes, read the World Meteorological Organization Header (aka, the "COMMS" header)\n
      Expects 42 bytes of a noaaport packet - whose start is based on the Header_length and psh_size '''
      wmo_size = wmo_raw.index(b'\r\r\n') + 3
      wmo_header = wmo_raw[:wmo_size].decode().strip()
      wmo_header = wmo_header[:4] + ' ' + wmo_header[4:] # KDENTIRT00 -> KDEN TIRT00
      rstation, wmo_id, station, ddhhmm = wmo_header.split(maxsplit=3)
      yearm = datetime.now().strftime("%Y%m")
      Ymd = datetime.strptime(yearm+ddhhmm[:6], "%Y%m%d%H%M").strftime("%Y%m%d_%H%M")
      channel = ''
      if len(ddhhmm) > 6:
        channel = ddhhmm[7:]
      wmo = self.wmo_meta(wmo_header, rstation, wmo_id, station, ddhhmm[:6], channel, Ymd, wmo_size)
      s_print(f'    WMO Header: {wmo_header}, wmo_size: {wmo_size}', end='')
      if args.verbose >= 2: s_print('WMOHeader: {wmo}')
      return wmo

  def read_and_show(self):
    '''Tune in to our channel, read and show what's streaming\n
    Read the header values for the packets received (see read_*_header methods for formats and conventions used)\n
    Keep track of the sbn and product sequence numbers (with help from the read_*_header methods)'''
    expected_sbn_sequence = 0
    product_sequence = 0
    self.connect_to_socket()

    filename = 'dummy'
    byte_buffer = b''

    s_print(f'Opening a socket to {self.ip}:{self.port} while listening to noaaport channel {self.channel}')
    while True: # while we're listening
      data, addr = self.sock.recvfrom(65507) # read data packets (buffer size should be 5120, 5000?)
      host, port = addr # read sender info 
      packet_size = len(data)
      if args.verbose >= 2: s_print("\n* Received %d size packet from %s:%s while listening to %s:%s" % (packet_size, host, port, self.ip, self.port))

      flh, expected_sbn_sequence = self.read_frame_header(data[:16], expected_sbn_sequence) # parse the frame level header and assign its vars
      sbn, product_sequence = self.read_sbn_header(data[16:32], product_sequence) # parse the sbn header and assign its vars
      if sbn.Header_length == 16 and filename != 'dummy': # just data beyond the (abbreviated) header
        # if args.verbose >= 1:
        #   # s_print(f'    writing data[32:] to byte_buffer {filename}')
        #   s_print(f'.', end='')
        byte_buffer += data[32:]
        if sbn.Transfer_type in [6] and filename != 'dummy': # last packet for this data file, let's write to disk
          s_print(f'       writing byte_buffer to file {os.path.join(self.data_dir, filename)}')
          with open(os.path.join(self.data_dir, filename), 'wb') as data_file:
            data_file.write(byte_buffer)
        continue
      if sbn.Header_length == 32 and packet_size == 48: # no data in this packet, what is?
        s_print(f'data: {data}')
        continue
      psh_start = self.read_psh_header_start(data[32:36]) # parse the start of the product spec header
      wmo_start = sbn.Header_length + psh_start.psh_size
      ''' pd.DataFrame(flh, sbn, psh_try1) # populate an array with the vars and values we have so far
      print the DataFrame, save for stat finding?
      review relevant bits of meta data for querying and for cataloging'''
      if sbn.Block_number == 0: # or Header_length > 16: # the first in a set (may be complete)
        wmo = self.read_wmo_header(data[wmo_start:wmo_start+40]) # parse the wmo header
        data_start = wmo_start + wmo.size
        ext = get_extension(data[data_start:data_start+5]) # derive the file format / extension
        filename = '.'.join([wmo.wmo_ymd, wmo.wmo_id, wmo.station, wmo.wmo_awips, ext]).replace('..', '.')
        if args.verbose >= 1:
          # s_print(f'    writing data[{data_start}:] to byte_buffer {filename}')
          s_print(f'.', end='')
        byte_buffer = data[data_start:]
      ## else:
      ##   s_print(f'    data[{data_start}:] {filename}')
      ##   byte_buffer += data[data_start:]
      ##   if sbn.Transfer_type in [6] and filename != 'dummy': # last packet for this data file, let's write to disk
      ##     with open(os.path.join(self.data_dir, filename), 'wb') as data_file:
      ##       data_file.write(byte_buffer)
      try:
        if len(data) >= 68: # (or sbn.Header_length >= 52) and psh_start.psh_size != 8192: # only keep reading if we know there's more (above headers should tell us)
          psh = self.read_psh_header(data[32:68]) # parse the full product spec header
        else:
          s_print(f'     data length too short to read the full product spec header, not reading. end: {data[(sbn.Header_length + 16):]}')
      except struct.error as se:
        s_print("struct.error reading expected bytes.. %s" % se)
      except Exception as e:
        s_print("error occurred!", e)
      sys.stdout.flush()

# miscellaneous helper methods

def get_extension(first_bytes) -> str:
    '''Given the first 5 (or so) bytes of a file, determine the format and return an extension'''
    ext = 'none' # default filename extension
    if b'GRIB' in first_bytes:
        ext = 'grib2'
    elif b'HDF' in first_bytes:
        ext = 'nc'
    return ext

def human_time(epoch_secs) -> str:
    '''given an integer time (seconds since epoch), return a human readable date/time'''
    my_struct = time.gmtime(epoch_secs)
    my_time = datetime(*my_struct[:6], tzinfo=timezone.utc)
    my_str = my_time.strftime('%Y_%m_%d_%H:%M:%S')
    return my_str

s_print_lock = threading.Lock() # define a threading Lock
def s_print(*a, **b):
    '''Thread safe print function'''
    with s_print_lock:
        print(*a, **b)

def setup_arg_parser(description: str) -> argparse.ArgumentParser:
    '''Set up command line argument parsing'''
    parser = argparse.ArgumentParser(description=description)
    channels = list(range(1,12)) # preset range of channels being transmitted from the dish, used for arg parser
    # channels = noaaport_reader.channels # preset range of channels being transmitted from the dish, used for arg parser
    parser.add_argument('channel', help='Specify the channel you want to listen to',
      choices=[str(x) for x in channels])
    parser.add_argument('-v', '--verbose', help='Make output more verbose. Can be used '
      'multiple times.', action='count', default=0)
    return parser

def main():
    '''Our main / module usage example(s)\n
    Initialize a reader on our channel, and show what's streaming'''
    global args
    args = setup_arg_parser(desc).parse_args()
    if args.channel != 'all':
      this_r = noaaport_reader(args.channel)
      this_r.read_and_show()
    ## else: # try to listen on all channels at once
    ##   for channel in channels: # listen to all channels
    ##     t = threading.Thread(target=read_and_show, args=[channel])
    ##     # t.daemon = True # don't want to block the program
    ##     t.start() # spawn a new listener thread

if __name__ == '__main__':
    main()

# more multi threaded thoughts from: https://stackoverflow.com/questions/16734534/close-listening-socket-in-python-thread
## import socket
## import multiprocessing
## 
## def run():
##     host = '000.000.000.000'
##     port = 1212
##     sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
##     sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
##     sock.bind(('', port))
##     sock.listen(3)
##     while True:
##         p = multiprocessing.Process(target=worker, args=sock.accept()).start()
## def worker(conn, addr):
##     while True:
##         if data == '':
##             #remote connection closed
##             break
##          if len(dataList) > 2:
##             # do stuff
##             print 'This code is untested'
## 
## run()
