#!/usr/bin/env python3

# Copyright (c) 2023-2024 NOAA ESRL Global Systems Laboratory
# Distributed under the terms of the MIT License
# SPDX-License-Identifier: MIT

'''
Description
###########
:synopsis: A script developed to begin to unravel the UDP packets broadcast from a satellite
           dish and modem receiving a NOAAPort Satellite Broadcast Network (SBN)  data feed.
:usage: ``./noaaport_reader.py <channel>`` # where <channel> is a number from 1 to 11, or 'all'
:note: If you have a NOAAPort satellite data feed, you now have a way to ingest, decode,
       write, transport, and catalog the incoming weather related data.

References
##########
  - https://www.weather.gov/noaaport
  - https://www.weather.gov/tg/head
  - https://www.weather.gov/tg/awips
  - https://www.weather.gov/tg/fstandrd
  - https://www.weather.gov/media/tg/CBS-2000.pdf

NOAAPort Channel Specifications
###############################

======= ===== == ============ ==== ===  ================================================
Channel Name  ID Multicast IP Port PID  Full Name
======= ===== == ============ ==== ===  ================================================
1       NMC   5  224.0.1.1    1201 101  NMC / NCEP / NWSTG
2       GOES     224.0.1.2    1202 102  GOES / NESDIS
3       NMC2     224.0.1.3    1203 103  NMC2 / NCEP / NWSTG2
4       NOPT     224.0.1.4    1204 104  Optional Data - OCONUS Imagery / Model
5       NPP      224.0.1.5    1205 105  National Polar-Orbiting Partnership / POLARSAT
6       ADD      224.0.1.6    1206 151  AWIPS Data Delivery / National Blend of Models
7       ENC      224.0.1.7    1207 150  Encrypted
8       EXP   11 224.0.1.8    1208 106  Experimental
9       GRW   12 224.0.1.9    1209 107  GOES-R Series West
10      GRE   13 224.0.1.10   1210 108  GOES-R Series East
11      NWWS     224.1.1.1    1201 201  Weather Wire text alerts and warnings
======= ===== == ============ ==== ===  ================================================

Requirements
############
python v3.6.8+

Linux Operating systems must have some sysctl kernel parameters customized. The parameter
net.ipv4.ipfrag_max_dist controls the process by which UDP fragments are reassembled into a UDP
packet. Because the DVB-S2 receiver guarantees sequential delivery of fragmented NOAAPORT UDP
packets, this re-assembly process should be disabled by setting the parameter to 0 in order to
prevent spurious gaps from occurring in the UDP packet stream. Also, the value of parameter
net.ipv4.conf.default.rp_filter should be 1 or 2 in order to obtain correct support for a 
multi-homed system. As root, execute the commands::

 sysctl -w net.ipv4.ipfrag_max_dist=0
 sysctl -w net.ipv4.conf.default.rp_filter=1 # or 2
 sysctl -p

Also, Note sysctl changes made by pdinrs RPM install process

In addition, the multicast group must be joined and the transmitting ports must be added to the
multicast group membership.

Json Meta Reference
###################
To read the json metadata, note that each file contains multiple json objects - one for each packet
that makes up a product. One way to read in the json formatted metadata files is shown below.

.. code:: python

  prod = []
  with open('<filename>', 'r') as f:
    content = f.read()
    for packet in content.replace('},{', '}PACKET_SEP{').split('PACKET_SEP'):
      prod.append(json.loads(packet))

Code
####
''' 

import os
import re
import json
import signal
import socket
import logging
import threading
from pathlib import Path
from queue import Queue, Empty
from time import gmtime, sleep
from struct import Struct, pack
from io import BytesIO, StringIO
from collections import namedtuple
from argparse import ArgumentParser
from datetime import datetime, timezone
from typing import Tuple, Any, NamedTuple
from concurrent.futures import ThreadPoolExecutor

DESC = "A UDP socket reader of the NOAAPort SBN data stream"
readers = [] # our NOAAPort Reader instances or thread groups, one per channel

class Header:
    '''Defines the NOAAPort header structures, variable names, and how to read them.\n
    Our header _struct(ures) and NamedTuple classes facilitate unpacking the respective
    byte_ranges and assigning variable names to the transmitted header objects of a NOAAPort data stream.
    (eg: flh.HEADER_LENGTH)'''
 
    class FrameLevelHeader(NamedTuple):
        '''The Frame Level Header variables and types'''
        HDLC_address: int
        HDLC_control: int
        SBN_version: int
        SBN_control: int
        SBN_command: int
        SBN_data_stream: int # 1 GOES East, 2 GOES West, 3 Reserved, 4 Non-GOES Imagery/DCP, 5 NCEP/NWSTG
        SBN_source: int
        SBN_destination: int
        SBN_sequence_number: int
        SBN_run: int
        SBN_checksum: int

    FLH_SIZE = 16 # the number of bytes that make up the frame header
    flh_struct = Struct('!8B L 2H')
    '''The 16 byte structure of the Frame Level Header (unsigned char[8], long, unsigned short[2])'''
    TIME_SYNC_CMD = 5 # the SBN_command value that marks a time sync packet

    class SatelliteBroadcastNetworkHeader(NamedTuple):
        '''The Satellite Broadcast Network Header (aka the Product Definition Header) variables and types\n
        A variable to key off of, the Transfer_type uses these values::
  
          * Transfer-type bit-mask:
          *   0x01  Start-of-product frame
          *   0x02  Product transfer. Set in Start-of-product frame
          *   0x04  End-of-product frame
          *   0x08  Product error
          *   0x10  Data-block is zlib(3) compressed
          *   0x20  Product abort
          *   0x40  ???
          PDH_START_PRODUCT          1
          PDH_TRANSFER_IN_PROGRESS   2
          PDH_LAST_PACKET            4
          PDH_PRODUCT_ERROR          8
          PDH_PRODUCT_ABORT         32
          PDH_OPTION_HEADER_INC     64
          CCB_SIZE_BYTE_SHIFT        2
          HE_SIZE                 8192  '''
        PDHVN: int
        Transfer_type: int
        Header_length: int
        Block_number: int
        Data_block_offset: int
        Data_block_size: int
        Records_per_block: int
        Blocks_per_record: int
        Product_sequence_number: int

    SBN_SIZE = 16 # the number of bytes that make up the sbn header
    sbn_struct = Struct('!2B 4H 2B L')
    TRANSFER_LAST_PACKET = 6 # Transfer_type value marking the last packet for a product
    TRANSFER_ABORT = 22      # Transfer_type value marking an abort, or end of the product
    DATA_HEADER_LENGTH = 16  # Header_length value marking a packet that contains only data

    class ProductSpecHeader(NamedTuple):
        '''The Porduct Specification Header variables and types'''
        field_num: int
        field_type: int
        psh_size: int
        psh_ver: int
        psh_flag: int
        awips_size: int
        bytes_per_rec: int
        my_type: int
        category: int
        prod_code: int
        num_frags: int
        next_headoff: int
        reserved:int
        source: int
        segnum: int
        recv_time: int
        send_time: int
        currRunIOd: int
        origRunId: int

    PSH_SIZE = 36 # the number of bytes that make up the product header
    psh_struct = Struct('!2B H 2B 2H 2B 3H 2B 3L 2H')
    PROD_CAT = { 1:'TEXT', 2:'GRAPHIC', 3:'IMAGE', 4:'GRID', 5:'POINT', 6:'BINARY' } # , 8:'GOES'

    class WMOHeader(NamedTuple):
        '''The World Meteorological Organization Header (aka, the "COMMS" header) variables and types'''
        header: str   # the full WMO header string provided
        rstation: str # originating/sending/receiving station? 
        wmo_id: str
        station: str  # obs station format is 'KKKK'
        time: int     # format is 'ddhhmm', using current year and month
        awips: str    # user defined (eg: channel)
        ymd: str      # more human readable wmo.time                               # injected by read_wmo()
        size: int     # the number of bytes used for the wmo header, plus '\r\r\n' # injected by read_wmo()

    WMO_MAX_SIZE = 40
    '''The maximum observed size of a WMO header, in bytes'''
    nexrad3_wmo_finder = re.compile('((?:NX|SD|NO)US)\\d{2}[\\s\\w\\d]+\\w*(\\w{3})')
    '''Compile a regular expression to help find nexrad level3 files based on a wmo header'''
    gini_wmo_finder    = re.compile('(T\\w{3}\\d{2})[\\s\\w\\d]+\\w*(\\w{3})') # (see SCN 20-104, deactivated 12/2020)

    class TimeSyncHeader(NamedTuple):
        '''The Time Synchronization Header variables and types'''
        version: int
        length: int
        flag: int
        total_length: int
        time_send: int
        ascii_date: int
        reserved: int

    time_sync_struct = Struct('!2B ? B I 10s 10x L')

    def read_frame(self, flh_raw: bytes, expected_sbn_seq: int) -> Tuple[namedtuple, int]:
        '''Given a raw set of bytes (expects the first 16 bytes of a NOAAPort packet) and an expected sbn sequence,
        read the Frame Level Header and calculate the next expected sbn sequence. \n
        This and other NOAAPort headers are unpacked into their expected _struct(ures), and assigned to their
        defined (NamedTuple subclassed) variables.\n
        Track the expected_sbn_sequence, make sure it increments by 1 (not sure where we're joining in the sbn sequencing).
        Produce a warning if an sbn packet arrives out of sequence.'''
        flh = self.FrameLevelHeader(*self.flh_struct.unpack(flh_raw))
        if expected_sbn_seq != int(flh.SBN_sequence_number):
            self.logger.warning(f'Received an SBN fragment/packet out of sequence! Expected {expected_sbn_seq}'
                    f', got {flh.SBN_sequence_number} - on channel {self.channel} '
                    f'({flh.SBN_sequence_number - expected_sbn_seq} missing)')
        expected_sbn_seq = int(flh.SBN_sequence_number) + 1
        if self.verbose >= 3: self.logger.debug(f'  FLH: {[getattr(flh, i) for i in flh._fields]}')
        return flh, expected_sbn_seq

    def read_sbn(self, sbn_raw: bytes) -> namedtuple:
        '''Given a raw set of bytes (expects bytes 16:32 of a NOAAPort packet), read the Satellite Broadcast Network Header'''
        sbn = self.SatelliteBroadcastNetworkHeader(*self.sbn_struct.unpack(sbn_raw))
        if self.verbose >= 2: self.logger.debug(f' flh.SBN_seq#: {self.flh.SBN_sequence_number} sbn: '
          f'Product_seq#: {sbn.Product_sequence_number} Block#:{sbn.Block_number} data_size:'
          f'{sbn.Data_block_size} Header_len:{sbn.Header_length} Transfer_type:{sbn.Transfer_type}')
        if self.verbose >= 3: self.logger.debug(f'  SBN: {[getattr(sbn, i) for i in sbn._fields]}')
        return sbn

    def read_product(self, psh_raw: bytes) -> namedtuple:
        '''Given a raw set of bytes (expects bytes 32:58 of a NOAAPort packet), read the Product Specification Header'''
        psh = self.ProductSpecHeader(*self.psh_struct.unpack(psh_raw))
        if self.verbose >= 3:
            self.logger.debug(f'    PSH: {[getattr(psh, i) for i in psh._fields]}')
            self.logger.debug(f' recv:{human_time(psh.recv_time)} send:{human_time(psh.send_time)}')
        return psh

    def read_wmo(self, wmo_raw: bytes) -> namedtuple:
        '''Given a raw set of bytes, read the World Meteorological Organization Header (aka, the "COMMS" header)\n
        Expects 40ish bytes of a NOAAPort packet - whose start is based on the Header_length and psh_size '''
        wmo_size = len(wmo_raw)
        try:
            wmo_size = wmo_raw.rindex(b'\r\r\n') + 3
        except ValueError as ve:
            if self.verbose >= 2: self.logger.warning(f"wmo header doesn't end in '\r\r\n', {ve}. Trying '\n'.")
            try:
                wmo_size = wmo_raw.rindex(b'\n') + 1
            except ValueError as e:
                self.logger.error(f'Error parsing WMO. {e}\n{wmo_raw} {wmo_size}')
        wmo_raw_trim = f'{wmo_raw[:wmo_size]}'
        wmo_header = wmo_raw[:wmo_size].replace(b'\r\r\n', b' ').replace(b'\r\n', b' ').decode(encoding='utf-8').strip()
        wmo_header = wmo_header[:4] + ' ' + wmo_header[4:]
        rstation, wmo_id, station, ddhhmm = wmo_header.split(maxsplit=3)
        yearm = datetime.now(timezone.utc).strftime("%Y%m")
        ymd = datetime.strptime(yearm+ddhhmm[:6], "%Y%m%d%H%M").strftime("%Y%m%d_%H%M")
        awips = ''
        if len(ddhhmm) > 6:
            awips = ddhhmm[7:]
        wmo = self.WMOHeader(wmo_header,rstation,wmo_id,station,ddhhmm[:6],awips,ymd,wmo_size)
        if self.verbose >= 2: self.logger.debug(f'WMO Header:{wmo_header} size:{wmo_size} raw:{wmo_raw_trim}')
        if self.verbose >= 3: self.logger.debug(f'  WMO: {[getattr(wmo, i) for i in wmo._fields]}')
        return wmo

    def read_time_sync(self, time_sync_raw: bytes):
        '''Given a raw set of bytes, read the time synchronization packet'''
        time_sync = self.TimeSyncHeader(*self.time_sync_struct.unpack(time_sync_raw))
        this_date = datetime.fromtimestamp(time_sync.time_send, timezone.utc)
        if self.verbose: self.logger.debug(f'** ch.{self.channel} Time Sync Packet: {this_date}, offset:'
                                 f' {(datetime.now(timezone.utc) - this_date)})')

class Protocol:
    '''Defines the protocols for receiving packets, and reading the stream of NOAAPort products.
 
    sbn_message = the data product portion of the packet - as raw bytes (aka, the file content)\n
    packet = flh + time_sync # when flh.SBN_command is 5 or TIME_SYNC_CMD\n
    packet = flh + sbn + sbn_message # when flh.HEADER_LENGTH is 16 or DATA_HEADER_LENGTH\n
    packet = flh + sbn + psh + wmo + sbn_message # sometimes, if sbn.Block_number is 0, first packet\n
    where flh + sbn + psh + wmo = ccb # the NWSTG Communications Control Block - only in product's first packet\n
    The final packet for a product is marked by
      - sbn.Transfer_type is 6 or TRANSFER_LAST_PACKET (last packet for product)
      - sbn.Transfer_type is 22 or TRANSFER_ABORT (product abort, in hex)
    '''

    CHANNELS = [str(i) for i in range(1,12)] # the range of valid NOAAPort channels
    BUFFER_MAX = 65507 # how many bytes (max) to read or receive from the socket

    def set_channel(self, channel: int):
        '''Sets the channel we will be listening on'''
        self.channel = str(channel)
        if self.channel == '11':
            self.ip = "224.1.1.1"
            self.port = 1201
        else:
            self.ip = "224.0.1." + self.channel
            self.port = 1200 + int(channel)

    def open_socket(self) -> None:
        '''Opens a socket for communication. Set up options for a socket instance, and bind to it for receiving'''
        try:
            # sock.setsockopt(socket.IPPROTO_IP, socket.IP_MULTICAST_TTL, 32)
            # mreq = pack("=4sl", socket.inet_aton(self.ip), socket.inet_aton('10.129.0.31'))
            # sock.setsockopt(socket.SOL_IP, socket.IP_ADD_MEMBERSHIP,
            #             socket.inet_aton(multicast_group) + socket.inet_aton(interface_ip))
            self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM, socket.IPPROTO_UDP)
            self.sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            self.sock.bind(('', self.port))
            mreq = pack("=4sl", socket.inet_aton(self.ip), socket.INADDR_ANY)
            self.sock.setsockopt(socket.IPPROTO_IP, socket.IP_ADD_MEMBERSHIP, mreq)
            self.logger.info(f'Opening a socket to {self.ip}:{self.port} while '
                    f'listening to NOAAPort channel {self.channel}')
        except socket.error as e:
            self.logger.error(f'Failed to open socket: {e}')
            raise e

    def close_socket(self):
        '''Stop listening to an open channel / socket'''
        self.logger.info(f'Closing socket listening on ch.{self.channel} or.. {self.ip}:{self.port}')
        self.sock.close()
        if self.filepath is not None:
            self.add_to_queue()

    def read_first_packet(self, data: bytes):
        '''Read and process the first packet for a product set (which may be a complete product).
        This contains our full product information and metadata, sets our filename and extension'''
        psh_start = self.FLH_SIZE + self.SBN_SIZE
        psh = self.read_product(data[psh_start:psh_start + self.PSH_SIZE])
        prod_cat = self.PROD_CAT.get(psh.category, 'OTHER')
        wmo_start = self.sbn.Header_length + psh.psh_size
        wmo = self.read_wmo(data[wmo_start:wmo_start + self.WMO_MAX_SIZE])
        data_start = wmo_start + wmo.size
        ext = get_extension(data[data_start:data_start+8])
        if ext == 'ncf.txt':
            if self.verbose: self.logger.debug(f'** NCF Status Message on ch.{self.channel}:\n'
                                               f'{data[data_start:].decode().strip()}')
            return
        if ext == 'none' and self.nexrad3_wmo_finder.search(wmo.header):
            ext = 'nexrad3'
        filename = '.'.join(['NOAAPORT',prod_cat, wmo.wmo_id, wmo.station, wmo.ymd[6:8]+wmo.ymd[9:],
          datetime.now(timezone.utc).strftime("%Y%j%H%M%S%f")[:-3], wmo.awips, ext]).replace('..', '.')
        self.filepath = Path(self.data_dir, filename)
        if self.verbose:
            self.logger.info(f'Receiving product {self.filepath} ({psh.num_frags} frags) q:{self.queue.qsize()}')
        self.sbn_message = BytesIO()
        self.sbn_json = StringIO()
        self.sbn_message.write(data[data_start:])
        self.sbn_json.write(json.dumps({'product_info':f'{datetime.now(timezone.utc)} {filename} '
          f'({psh.num_frags} fragments)', f'packet_{self.sbn.Block_number}_meta': self.packet_meta,
          'FLH':self.flh._asdict(), 'SBN':self.sbn._asdict(), 'PSH':psh._asdict(),
          'WMO':wmo._asdict()}, indent=2, separators=(',', ': ')))
        if psh.num_frags == 0:
            self.add_to_queue()

    def read_data_packet(self, data: bytes):
        '''Read a data packet, which contains just binary data beyond the (abbreviated - flh + sbn) header'''
        if self.filepath is not None:
            self.sbn_message.write(data)
            self.sbn_json.write(',')
            self.sbn_json.write(json.dumps({f'packet_{self.sbn.Block_number}_meta':self.packet_meta,
              'FLH':self.flh._asdict(), 'SBN':self.sbn._asdict()}, indent=2, separators=(',',': ')))
            if (self.sbn.Transfer_type in [self.TRANSFER_LAST_PACKET, self.TRANSFER_ABORT]):
                self.add_to_queue()
        elif self.verbose >= 2:
            self.logger.debug(f' Reading data packet without the product spec / info (fragment# {self.sbn.Block_number})')

    def wait_for_data(self):
        '''Wait for data packets to arrive, and route to the appropriate Header class read method\n
        Read the header variable values for the packets received (following NOAAPort formats and conventions)\n
        Keep track of the sbn sequence number (with help from the read_frame method)'''
        expected_sequence = 0
        while not self.stop_event.is_set():
            data, (host, port) = self.sock.recvfrom(self.BUFFER_MAX)
            self.packet_meta = (f'{datetime.now(timezone.utc)} Received packet ({len(data)} bytes) '
              f'on {self.ip}:{self.port} (channel: {self.channel}) from {host}:{port}')
            if self.verbose >= 3: self.logger.debug(self.packet_meta)
    
            self.flh, expected_sequence = self.read_frame(data[:self.FLH_SIZE], expected_sequence)
            if self.flh.SBN_command == self.TIME_SYNC_CMD:
                self.read_time_sync(data[self.FLH_SIZE:])
                continue
    
            self.sbn = self.read_sbn(data[self.FLH_SIZE:self.SBN_SIZE + self.FLH_SIZE])
            if self.sbn.Header_length == self.DATA_HEADER_LENGTH:
                self.read_data_packet(data[self.SBN_SIZE + self.FLH_SIZE:])
                continue
    
            if self.sbn.Block_number == 0:
                self.read_first_packet(data)
                continue
    
            if self.verbose: self.logger.warning(f'Unexpected state reached. Meta: {self.packet_meta}, '
              f'flh: {self.flh}, sbn: {self.sbn}.')

class Worker:
    '''Define some methods to manage the NOAAPort workload - queue the received packets and write file products.'''

    def setup_queue(self):
        '''Sets up the queue for processing data, starts the producer and consumer of the queue.'''
        self.queue = Queue(1200)
        self.stop_event = threading.Event()
        self.writer = threading.Thread(target=self.consumer, daemon=True, name=f'NOAAPort_consumer_{self.channel}')
        self.writer.start()
        self.reader = threading.Thread(target=self.wait_for_data, daemon=True, name=f'NOAAPort_wait_for_data_{self.channel}')
        self.reader.start()
        self.logger.info(f'Setting up a queue for ch:{self.channel}. Number of active threads: {threading.active_count()}') # active_children()
    
    def add_to_queue(self):
        '''Adds the current file path and message (and metadata) to the queue for writing. Also resets
        the file path and closes our instance buffers, in preparation for the next data product.'''
        if not self.stop_event.is_set():
            self.queue.put_nowait((self.filepath, self.sbn_json.getvalue(), False))
            self.queue.put_nowait((self.filepath, self.sbn_message.getvalue(), True)) # or better not to write incomplete data?
        self.filepath = None
        self.sbn_json.close()
        self.sbn_message.close()
    
    def consumer(self):
        '''Continuously consumes items from the queue and writes the data to the appropriate location.'''
        while not self.stop_event.is_set():
            try:
                filepath, buffer, binary = self.queue.get(timeout=1)
            except Empty:
                continue
            if str(filepath).startswith('ldm:/'):
                self.write_to_ldm(self, filepath, buffer)
            elif binary:
                self.write_data(filepath, buffer)
            else:
                self.write_json(filepath, buffer)
            self.queue.task_done() # this method is *not* in the multiprocessing.Queue class
    
    def write_data(self, filepath:str, buffer:bytes):
        '''Writes the given binary data to the specified file.'''
        if self.verbose > 1: self.logger.debug(f'  writing sbn_message to file {filepath} of size '
          f'{human_size(len(buffer))}')
        with filepath.open('wb') as data_file:
            data_file.write(buffer)

    def write_json(self, filepath:str, buffer:str):
        '''Writes the given JSON metadata to the specified file.'''
        if self.verbose > 1: self.logger.debug(f'  writing metadata to file {filepath.with_suffix(".json")}')
        with filepath.with_suffix('.json').open('w', encoding="utf-8") as json_file:
            json_file.write(buffer)

    def write_to_ldm(self, filepath:str, buffer:Any):
        '''Writes the given data to LDM, inserting into the LDM queue via the shared library.'''
        if self.verbose >= 2: self.logger.debug(f'  writing to LDM sbn_message {self.filepath} of size '
          f'{human_size(len(self.sbn_message.getbuffer()))}')
        try:
            # Call the function to insert data into the LDM queue
            result = self.ldm_lib.insert_into_ldm(filepath[5:], buffer)
            if result != 0:
                raise Exception('Failed to insert data into LDM queue')
        except Exception as e:
            self.logger.error('Failed to insert data into LDM queue: %s', e)

class Reader(Header, Protocol, Worker):
    '''A class used to represent a NOAAPort reader, with class mixins: Header, Protocol and Worker.'''
    def __init__(self, channel: int, dest: str, logdir:str = None, verbose: int = 0):
        '''Initializes a NOAAPort reader on the given channel with logging.
        Args:
            channel (int): The channel to listen to.
            dest (str): The destination for received products.
            logdir (str, optional): The directory to write logs to. Defaults to None.
            verbose (int, optional): The verbosity level for logging. Defaults to 0.'''
        self.filepath = None
        self.verbose = verbose
        self.set_channel(channel)
        self.setup_logger(logdir)
        self.data_dir = Path(dest, self.channel)
        self.data_dir.mkdir(parents=True, exist_ok=True)
        # self.ldm_lib = ctypes.CDLL('/path/to/ldm.so') # Load the LDM shared library
        self.open_socket()
        self.setup_queue()

    def setup_logger(self, logdir:str = None):
        '''Sets up the logger for this class, noting the directory to write logs to.'''
        logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')
        self.logger = logging.getLogger(f'NOAAPort_reader.{int(self.channel):02d}')
        if logdir is not None: 
            if not Path(logdir).exists(): print(f'creating log directory {Path(logdir)}')
            Path(logdir).mkdir(exist_ok=True, parents=True)
            filename = Path(logdir,f'NOAAPort_reader_{self.channel}.log')
            file_handler = logging.FileHandler(filename=filename)
            file_handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
            self.logger.addHandler(file_handler)

def get_extension(first_bytes: bytes) -> str:
    '''Determines the file extension based on the first few bytes of the file.'''
    ext = 'none'
    if b'GRIB' in first_bytes:
        ext = 'grib2'
    elif b'HDF' in first_bytes or b'CDF' in first_bytes:
        ext = 'nc'
    elif b'BUFR' in first_bytes:
        ext = 'bufr'
    elif b'PNG' in first_bytes:
        ext = 'png'
    elif first_bytes[:2] == bytes.fromhex('1F8B'):
        ext = 'gz'
    elif b'HADS' in first_bytes:
        ext = 'hads_report.txt'
    elif b'AFWS' in first_bytes:
        ext = 'afws_report.txt'
    elif b'THIS IS ' in first_bytes:
        ext = 'ncf.txt'
    elif b'<?xml ' in first_bytes:
        ext = 'xml'
    return ext

def human_time(epoch_secs: int) -> str:
    '''Converts epoch time to human-readable time.'''
    my_time_struct = gmtime(epoch_secs)
    my_time = datetime(*my_time_struct[:6], tzinfo=timezone.utc)
    return my_time.strftime('%Y_%m_%d_%H:%M:%S')

def human_size(size: str, units = None) -> str:
    '''Converts size in bytes to human-readable format.'''
    if units is None: units = [' bytes','KB','MB','GB','TB', 'PB', 'EB']
    return f"{float(size):.1f}{units[0]}" if float(size)<1024 else human_size(size/1024, units[1:])

def parse_args(description: str) -> ArgumentParser:
    '''Parses command-line arguments, noting the program description for the help message.'''
    parser = ArgumentParser(description=description)
    parser.add_argument('channel', help='Specify the channel you want to listen to',
      choices=Reader.CHANNELS + ['all'])
    parser.add_argument('-v', '--verbose', help='Make output more verbose. Can be used '
      'multiple times.', action='count', default=0)
    parser.add_argument('-d', '--dest', help='Specify the destination for received products',
      type=str, default='/data/temp/')
    parser.add_argument('-l', '--logdir', help='Specify the directory to write logs to',
      type=str, default=None)
    return parser.parse_args()

def catch_signals():
    '''Sets up signal handling. This function catches all signals that can be caught,
    and sets up a signal handler for them.

    The signal handler logs the signal, closes all sockets, and exits the program.'''
    logger = logging.getLogger(Path(__file__).name + '_catch_signals') # provide a local logger
    def sig_handler(signum: int, frame):
        '''Handles received system signals and performs cleanup operations.

        Args:
            signum (int): The signal number.
            frame: Not used, but makes the available the current stack frame object.'''
        signame = signal.Signals(signum).name
        logger.info(f'Signal handler called with signal name:{signame},'
                f' num:{signum}') #  (frame: {frame})')
        logger.info(f'Closing socket(s) for {len(readers)} reader(s)')
        for n in readers:
            n.stop_event.set()
            n.close_socket()
            n.queue.queue.clear()
        os._exit(0) # sys.exit(0)

    catchable_sigs = set(signal.Signals) - {signal.SIGKILL, signal.SIGSTOP, signal.SIGWINCH}
    for sig in catchable_sigs:
        signal.signal(sig, sig_handler)

def start_reader(args_list: Tuple[int, str, str, int]) -> Reader:
    '''Starts a NOAAPort reader with the given arguments.
    
    Args:
        args_list (tuple): A tuple containing the arguments for the NOAAPort reader.
        args_tuple: channel:int, destination:str, log_directory:str, verbose:int

    Returns:
        The created NOAAPort Reader object. Also updates the list of started readers.
    '''
    ch, dest, logdir, verbose = args_list
    reader = Reader(ch, dest, logdir, verbose)
    # reader = Reader(*args_list)
    readers.append(reader)
    return reader

def main(args: ArgumentParser = None) -> None:
    '''The main function of the program. Parses arguments, starts readers, and handles signals.'''
    catch_signals()
    args = args or parse_args(DESC)
    channels = [args.channel] if args.channel != 'all' else Reader.CHANNELS
    with ThreadPoolExecutor(max_workers = 20) as executor:
        args_iter = ((ch, args.dest, args.logdir, args.verbose) for ch in channels)
        results = executor.map(start_reader, args_iter)
    for reader in results:
        reader.logger.info(f'started up a reader on channel {reader.channel}')
    # while True:
    #     pass # simply wait while data arrives and is written
    sleep(60) # or just run a few seconds to profile the code

def profile_main():
    '''Profiles the main function and prints the stats.'''
    import pstats
    import cProfile
    from contextlib import ExitStack
    with ExitStack() as stack:
        profiler = stack.enter_context(cProfile.Profile())
        main()
        profiler.dump_stats('cProfile.noaaport.log')
    p = pstats.Stats("cProfile.noaaport.log")
    p.sort_stats("cumulative").print_stats()
    # cProfile.run('main()', 'cProfile.noaaport.log')
    # p = pstats.Stats("cProfile.noaaport.log")
    # p.sort_stats("cumulative").print_stats()

if __name__ == '__main__':
    main()
    # profile_main()
