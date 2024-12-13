#!/usr/bin/env python3

# Copyright (c) 2023-2024 NOAA ESRL Global Systems Laboratory
# Distributed under the terms of the MIT License
# SPDX-License-Identifier: MIT

'''
Description
###########
:synopsis: A script developed to begin to unravel the UDP packets broadcast from a satellite
           dish and modem receiving a NOAAPort Satellite Broadcast Network (SBN) data feed.
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
  - https://www.noaasis.noaa.gov/docs/GoesDcsSystemDescription%20Dreyer.pdf

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
packet. Because the DVB-S2 receiver guarantees sequential delivery of fragmented NOAAPort UDP
packets, this re-assembly is disabled by setting the parameter to 0 in order to
prevent spurious gaps from occurring in the UDP packet stream. Also, the value of parameter
net.ipv4.conf.default.rp_filter should be 1 or 2 in order to obtain correct support for a 
multi-homed system. As root, execute the commands::

 sysctl -w net.ipv4.ipfrag_max_dist=0
 sysctl -w net.ipv4.conf.default.rp_filter=1 # or 2
 sysctl -p

Also, Note that additional sysctl changes may be needed. In addition, the multicast group must
be joined and the transmitting ports must be added to the multicast group membership.

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

file signature info
  - https://en.wikipedia.org/wiki/List_of_file_signatures
  - https://www.garykessler.net/library/file_sigs.html

Code
####
'''

import os
import re
import json
import signal
import socket
import logging
import asyncio
from queue import Queue
from time import gmtime
from pathlib import Path
from threading import Thread
from struct import Struct, pack
from io import BytesIO, StringIO
from collections import namedtuple
from argparse import ArgumentParser
from datetime import datetime, timezone
from typing import Tuple, Any, NamedTuple

DESC = "A UDP socket reader of the NOAAPort SBN data stream"

class Header:
    '''Defines the NOAAPort header structures, variable names, and how to read them.\n
    Our header _struct(ures) and NamedTuple classes facilitate unpacking the respective
    byte_ranges and assigning variable names to the transmitted header objects of a
    NOAAPort data stream. (eg: flh.SBN_command or sbn.Header_length)'''

    class FrameLevelHeader(NamedTuple):
        '''The Frame Level Header (flh) variables and types'''
        HDLC_address: int
        HDLC_control: int
        SBN_version: int
        SBN_control: int
        SBN_command: int
        SBN_data_stream: int
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
        '''The Satellite Broadcast Network (sbn) Header (aka the Product Definition Header) variables and types\n
        A variable to key off of, the Transfer_type uses these values::
  
          * Transfer-type bit-mask (note a CCB_SIZE_BYTE_SHIFT of +2):
          *   0x01  1  Start-of-product frame (START_PRODUCT)
          *   0x02  2  Product transfer. Set in Start-of-product frame (TRANSFER_IN_PROGRESS)
          *   0x04  4  End-of-product frame (LAST_PACKET)
          *   0x08  8  Product error (PRODUCT_ERROR)
          *   0x10  16 Data-block is zlib(3) compressed
          *   0x20  32 Product abort (PRODUCT_ABORT)
          *   0x40  64 ??? (OPTION_HEADER_INC)'''
        version: int
        Transfer_type: int
        Header_length: int
        Block_number: int
        Data_block_offset: int
        Data_block_size: int
        Records_per_block: int
        Blocks_per_record: int
        Product_sequence_number: int

    sbn_struct = Struct('!2B 4H 2B L')
    SBN_SIZE = 16            # the number of bytes that make up the sbn header
    TRANSFER_LAST_PACKET = 6 # Transfer_type value marking the last packet for a product
    TRANSFER_ABORT = 22      # Transfer_type value marking an abort, or end of the product
    DATA_HEADER_LENGTH = 16  # Header_length value marking a packet that contains only data

    class ProductSpecHeader(NamedTuple):
        '''The Product Specification Header (psh) variables and types'''
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
    PROD_CAT = { 1:'TEXT', 2:'GRAPHIC', 3:'IMAGE', 4:'GRID', 5:'POINT', 6:'BINARY', 8:'GOES' }

    class WMOHeader(NamedTuple):
        '''The World Meteorological Organization (WMO) Header (aka, the "COMMS" header) variables and types.
        https://www.weather.gov/tg/table'''
        header: str   # the full WMO header string provided
        rstation: str # originating/sending/receiving station?
        wmo_id: str
        station: str  # obs station format is 'KKKK'
        time: int     # format is 'ddhhmm', using current year and month
        awips: str    # user defined (eg: channel)
        ymd: str      # more human readable wmo.time                               # injected by read_wmo()
        size: int     # the number of bytes used for the wmo header, plus '\r\r\n' # injected by read_wmo()

    WMO_MAX_SIZE = 40 # The maximum observed size of a WMO header, in bytes
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

    def read_frame(self, flh_raw: bytes, expected_sbn_seq: int, verbose:int = 0) -> Tuple[namedtuple, int]:
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
        if verbose >= 3: self.logger.debug(f'  FLH: {[getattr(flh, i) for i in flh._fields]}')
        return flh, expected_sbn_seq

    def read_sbn(self, sbn_raw: bytes, verbose:int = 0) -> namedtuple:
        '''Given a raw set of bytes (expects bytes 16:32 of a NOAAPort packet), read the Satellite Broadcast Network Header'''
        sbn = self.SatelliteBroadcastNetworkHeader(*self.sbn_struct.unpack(sbn_raw))
        if verbose >= 2: self.logger.debug(f' flh.SBN_seq#: {self.flh.SBN_sequence_number} sbn: '
          f'Product_seq#: {sbn.Product_sequence_number} Block#:{sbn.Block_number} data_size:'
          f'{sbn.Data_block_size} Header_len:{sbn.Header_length} Transfer_type:{sbn.Transfer_type}')
        if verbose >= 3: self.logger.debug(f'  SBN: {[getattr(sbn, i) for i in sbn._fields]}')
        return sbn

    def read_product(self, psh_raw: bytes, verbose:int = 0) -> namedtuple:
        '''Given a raw set of bytes (expects bytes 32:58 of a NOAAPort packet), read the Product Specification Header'''
        psh = self.ProductSpecHeader(*self.psh_struct.unpack(psh_raw))
        if verbose >= 3:
            self.logger.debug(f'    PSH: {[getattr(psh, i) for i in psh._fields]}')
            self.logger.debug(f' recv:{human_time(psh.recv_time)} send:{human_time(psh.send_time)}')
        return psh

    def read_wmo(self, wmo_raw: bytes, verbose:int = 0) -> namedtuple:
        '''Given a raw set of bytes, read the World Meteorological Organization Header (aka, the "COMMS" header)\n
        Expects 40ish bytes of a NOAAPort packet - whose start is based on the Header_length and psh_size '''
        wmo_size = len(wmo_raw)
        try:
            wmo_size = wmo_raw.rindex(b'\r\r\n') + 3
        except ValueError as ve:
            if verbose >= 2: self.logger.warning(f"wmo header doesn't end in '\r\r\n', {ve}. Trying '\n'.")
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
        if verbose >= 2: self.logger.debug(f'WMO Header:{wmo_header} size:{wmo_size} raw:{wmo_raw_trim}')
        if verbose >= 3: self.logger.debug(f'  WMO: {[getattr(wmo, i) for i in wmo._fields]}')
        return wmo

    def read_time_sync(self, time_sync_raw: bytes, verbose:int = 0):
        '''Given a raw set of bytes, read the time synchronization packet'''
        time_sync = self.TimeSyncHeader(*self.time_sync_struct.unpack(time_sync_raw))
        this_date = datetime.fromtimestamp(time_sync.time_send, timezone.utc)
        if verbose: self.logger.debug(f'** ch.{self.channel} Time Sync Packet: {this_date}, offset:'
                                 f' {(datetime.now(timezone.utc) - this_date)})')

class Protocol(asyncio.DatagramProtocol):
    '''UDP client protocol for receiving and processing packets'''
    def __init__(self, reader):
        super().__init__()
        self.reader = reader

    def connection_made(self, transport):
        self.transport = transport

    def datagram_received(self, data, addr):
        host, port = addr
        self.reader.process_packet(data, host, port)

class Reader(Header):
    '''Defines methods for receiving packets, and reading the stream of NOAAPort products.
 
    sbn_message = the data product portion of the packet - as raw bytes (aka, the file content)\n
    packet = flh + time_sync # when flh.SBN_command is 5 or TIME_SYNC_CMD\n
    packet = flh + sbn + sbn_message # when sbn.Header_length is 16 or DATA_HEADER_LENGTH\n
    packet = flh + sbn + psh + wmo + sbn_message # when sbn.Block_number is 0, first packet\n
    where flh + sbn + psh + wmo = ccb # the NWSTG Communications Control Block - only in product's first packet\n
    The final packet for a product is marked by
      - a first packet where psh.num_frags is 0
      - sbn.Transfer_type is 6 or TRANSFER_LAST_PACKET (last packet for product)
      - sbn.Transfer_type is 22 or TRANSFER_ABORT (product abort, in hex)
    '''

    CHANNELS = [str(i) for i in range(1,12)] # the range of valid NOAAPort channels
    BUFFER_MAX = 65507 # how many bytes (max) to read or receive from the socket
    FIRST_BYTES = 8 # How many bytes, at the start of the data, to scan to determine the file type

    def __init__(self, channel: int, cue, dest: str, logdir:str = None, verbose: int = 0):
        '''Initializes a NOAAPort reader on the given channel with logging.
        Args:
            channel (int): The channel to listen to.
            cue (Queue): The queue we will add products to, to be written by the Worker
            dest (str): The destination for received products.
            logdir (str, optional): The directory to write logs to. Defaults to None.
            verbose (int, optional): The verbosity level for logging. Defaults to 0.'''
        self.filepath = None
        self.verbose = verbose
        self.queue = cue
        self.set_channel(channel)
        self.logger = setup_logger(f'NOAAPort_reader.{int(self.channel):02d}', logdir)
        self.expected_sequence = 0 # The sbn sequence number is expected to increment by 1
        self.data_dir = Path(dest, self.channel)
        self.data_dir.mkdir(parents=True, exist_ok=True)
        # self.ldm_lib = ctypes.CDLL('/path/to/ldm.so') # Load the LDM shared library
        self.setup_socket()
        loop = asyncio.get_event_loop()
        connect = loop.create_datagram_endpoint(lambda: Protocol(self), sock=self.sock)
        transport, protocol = loop.run_until_complete(connect)

    def set_channel(self, channel: int):
        '''Sets the channel we will be listening on'''
        self.channel = str(channel)
        if self.channel == '11':
            self.ip = "224.1.1.1"
            self.port = 1201
        else:
            self.ip = "224.0.1." + self.channel
            self.port = 1200 + int(channel)

    def setup_socket(self) -> None:
        '''Opens a socket for communication. Set up options for a socket instance, and bind to it for receiving'''
        try:
            self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM, socket.IPPROTO_UDP)
            self.sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            self.sock.bind(('', self.port))
            mreq = pack("=4sl", socket.inet_aton(self.ip), socket.INADDR_ANY)
            self.sock.setsockopt(socket.IPPROTO_IP, socket.IP_ADD_MEMBERSHIP, mreq)
            self.logger.info(f'Setting up a socket to {self.ip}:{self.port} while '
                    f'listening to NOAAPort channel {self.channel}')
        except socket.error as e:
            self.logger.error(f'Failed to open socket: {e}')
            raise e

    def add_to_queue(self):
        '''Adds the current file path and message (and metadata) to the queue for writing. Also resets
        the file path and closes our instance buffers, in preparation for the next data product.'''
        self.queue.put_nowait((self.filepath, self.sbn_json.getvalue(), False))
        self.queue.put_nowait((self.filepath, self.sbn_message.getvalue(), True))
        self.filepath = None
        self.sbn_json.close()
        self.sbn_message.close()

    def read_first_packet(self, data: bytes, verbose:int = 0):
        '''Read and process the first packet for a product set (which may be a complete product).
        This contains our full product information and metadata, sets our filename and extension'''
        psh_start = self.FLH_SIZE + self.SBN_SIZE
        psh = self.read_product(data[psh_start:psh_start + self.PSH_SIZE], verbose)
        prod_cat = self.PROD_CAT.get(psh.category, 'OTHER')
        wmo_start = self.sbn.Header_length + psh.psh_size
        wmo = self.read_wmo(data[wmo_start:wmo_start + self.WMO_MAX_SIZE], verbose)
        data_start = wmo_start + wmo.size
        ext = get_extension(data[data_start : data_start + self.FIRST_BYTES])
        if ext == 'ncf.txt':
            if verbose: self.logger.debug(f'** NCF Status Message on ch.{self.channel}:\n'
                                               f'{data[data_start:].decode().strip()}')
            return
        if ext == 'none' and self.nexrad3_wmo_finder.search(wmo.header):
            ext = 'nexrad3'
        filename = '.'.join(['NOAAPORT',prod_cat, wmo.wmo_id, wmo.station, wmo.ymd[6:8]+wmo.ymd[9:],
          datetime.now(timezone.utc).strftime("%Y%j%H%M%S%f")[:-3], wmo.awips, ext]).replace('..', '.')
        self.filepath = Path(self.data_dir, filename)
        if verbose:
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

    def read_data_packet(self, data: bytes, verbose:int = 0):
        '''Read a data packet, which contains just binary data beyond the abbreviated (flh + sbn) header'''
        if self.filepath is not None:
            self.sbn_message.write(data)
            self.sbn_json.write(',')
            self.sbn_json.write(json.dumps({f'packet_{self.sbn.Block_number}_meta':self.packet_meta,
              'FLH':self.flh._asdict(), 'SBN':self.sbn._asdict()}, indent=2, separators=(',',': ')))
            if (self.sbn.Transfer_type in [self.TRANSFER_LAST_PACKET, self.TRANSFER_ABORT]):
                self.add_to_queue()
        elif verbose >= 2:
            self.logger.debug(f' Reading data packet without the product spec / info (fragment# {self.sbn.Block_number})')

    def process_packet(self, data:bytes, host:str, port:int):
        '''As data packets arrive, route to the appropriate Header class read method\n
        Read the header variable values for the packets received (following NOAAPort formats and conventions)'''
        verbose = self.verbose
        self.packet_meta = (f'{datetime.now(timezone.utc)} Received packet ({len(data)} bytes) '
          f'on {self.ip}:{self.port} (channel: {self.channel}) from {host}:{port}')
        if verbose >= 3: self.logger.debug(self.packet_meta)

        self.flh, self.expected_sequence = self.read_frame(data[:self.FLH_SIZE], self.expected_sequence, verbose)
        if self.flh.SBN_command == self.TIME_SYNC_CMD:
            self.read_time_sync(data[self.FLH_SIZE:], verbose)
            return

        self.sbn = self.read_sbn(data[self.FLH_SIZE:self.SBN_SIZE + self.FLH_SIZE], verbose)
        if self.sbn.Header_length == self.DATA_HEADER_LENGTH:
            self.read_data_packet(data[self.SBN_SIZE + self.FLH_SIZE:], verbose)
            return

        if self.sbn.Block_number == 0:
            self.read_first_packet(data, verbose)
            return

        if verbose: self.logger.warning(f'Unexpected state reached. Meta: {self.packet_meta}, '
          f'flh: {self.flh}, sbn: {self.sbn}.')

class Worker(Thread):
    '''Define some methods to manage the NOAAPort workload - queue the received packets and write file products.'''

    def __init__(self, logdir:str, verbose:int):
        Thread.__init__(self, name='NOAAPort_writer_thread', daemon=True)
        self.logger = setup_logger('NOAAPort_writer', logdir)
        self.verbose = verbose
        self.queue = Queue() # Sets up the queue for processing data

    def run(self):
        '''Continuously consumes items from the queue and writes the data to the appropriate location.'''
        while True:
            try:
                filepath, buffer, binary = self.queue.get()
            except IndexError:
                continue # continue when queue is empty
            if str(filepath).startswith('ldm:/'):
                self.write_to_ldm(self, filepath, buffer)
            elif binary:
                self.write_data(filepath, buffer)
            else:
                self.write_json(filepath, buffer)

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
        if self.verbose >= 2: self.logger.debug(f'  writing to LDM sbn_message {filepath} of size '
          f'{human_size(len(buffer))}')
        try:
            # Call the function to insert data into the LDM queue
            result = self.ldm_lib.insert_into_ldm(filepath[5:], buffer)
            if result != 0:
                raise Exception('Failed to insert data into LDM queue')
        except Exception as e:
            self.logger.error('Failed to insert data into LDM queue: %s', e)

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

def setup_logger(name:str = 'NOAAPort_reader', logdir:str = None):
    '''Sets up the logger for this class, noting the directory to write logs to.'''
    logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')
    logger = logging.getLogger(name)
    if logdir is not None:
        if not Path(logdir).exists(): print(f'creating log directory {Path(logdir)}')
        Path(logdir).mkdir(exist_ok=True, parents=True)
        filename = Path(logdir,f'{name}.log')
        file_handler = logging.FileHandler(filename=filename)
        file_handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
        logger.addHandler(file_handler)
    return logger

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
        os._exit(0) # sys.exit(0)

    catchable_sigs = set(signal.Signals) - {signal.SIGKILL, signal.SIGSTOP, signal.SIGWINCH}
    for sig in catchable_sigs:
        signal.signal(sig, sig_handler)

def main():
    '''The main function of the program. Parses arguments, starts readers, and handles signals.'''
    catch_signals()
    args = parse_args(DESC)
    channels = [args.channel] if args.channel != 'all' else Reader.CHANNELS
    worker = Worker(args.logdir, args.verbose)
    for ch in channels:
        Reader(ch, worker.queue, args.dest, args.logdir, args.verbose)
    worker.start()
    loop = asyncio.get_event_loop()
    loop.run_forever()

if __name__ == '__main__':
    main()
