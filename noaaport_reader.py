#!/usr/bin/env python3

desc = "A UDP socket reader of NOAAPort SBN data"

# Copyright (c) 2024 NOAA ESRL Global Systems Laboratory
# Distributed under the terms of the BSD 3-Clause License.
# SPDX-License-Identifier: BSD-3-Clause

import os
import re
import json
import signal
import socket
import struct
import threading
from time import gmtime
from io import BytesIO, StringIO
from collections import namedtuple
from argparse import ArgumentParser
from datetime import datetime, timezone

class Noaaport:
  channels = list(range(1,12)) 

  flh_meta = namedtuple('FrameLevelHeader', 'HDLC_address HDLC_control SBN_version SBN_control'
    ' SBN_command SBN_data_stream SBN_source SBN_destination SBN_sequence_number SBN_run SBN_checksum')
  flh_struct = struct.Struct('!8B L 2H')

  sbn_meta = namedtuple('SatelliteBroadcastNetworkHeader', 'PDHVN Transfer_type Header_length'
    ' Block_number Data_block_offset Data_block_size Records_per_block Blocks_per_record'
    ' Product_sequence_number')
  sbn_struct = struct.Struct('!2B 4H 2B L')

  psh_meta = namedtuple('ProductSpecHeader', 'field_num field_type psh_size psh_ver psh_flag'
    ' awips_size bytes_per_rec my_type category prod_code num_frags next_headoff reserved source'
    ' segnum recv_time send_time currRunIOd origRunId')
  psh_struct = struct.Struct('!2B H 2B 2H 2B 3H 2B 3L 2H')

  wmo_meta = namedtuple('WMOHeader', 'header rstation wmo_id station time awips ymd size')
  wmo_struct = struct.Struct('!11s 5s 6s 8s 3s 7s')

  time_sync_meta = namedtuple('TimeSync', 'version length flag total_length time_send ascii_date'
    ' reserved')
  time_sync_struct = struct.Struct('!2B ? B I 10s 10x L')

  nexrad_l3_wmo_finder = re.compile('((?:NX|SD|NO)US)\\d{2}[\\s\\w\\d]+\\w*(\\w{3})')
  gini_wmo_finder      = re.compile('(T\\w{3}\\d{2})[\\s\\w\\d]+\\w*(\\w{3})')

  PROD_CAT = { 1: 'TEXT', 2: 'GRAPHIC', 3: 'IMAGE', 4: 'GRID', 5: 'POINT', 6: 'BINARY' } 

  def __init__(self, channel: int, dest: str, verbose: int = 0):
      self.verbose = verbose
      self.set_channel(channel)
      self.filepath = None
      self.data_dir = os.path.join(dest, self.channel) 
      if not os.path.isdir(self.data_dir): os.makedirs(self.data_dir) 
      self.open_socket()
      self.wait_for_data()

  def set_channel(self, channel: int):
      self.channel = str(channel)
      if self.channel == '11': 
        self.ip = "224.1.1.1"
        self.port = 1201
      else:
        self.ip = "224.0.1." + self.channel 
        self.port = 1200 + int(channel)     

  def open_socket(self):
      self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM, socket.IPPROTO_UDP) 
      self.sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1) 
      self.sock.bind(('', self.port)) 
      mreq = struct.pack("sl", socket.inet_aton(self.ip), socket.INADDR_ANY) 
      self.sock.setsockopt(socket.IPPROTO_IP, socket.IP_ADD_MEMBERSHIP, mreq) 
      s_print(f'{datetime.utcnow()} Opening a socket to {self.ip}:{self.port} while listening to noaaport channel {self.channel}')

      catchable_sigs = set(signal.Signals) - {signal.SIGKILL, signal.SIGSTOP}
      for sig in catchable_sigs:
        signal.signal(sig, self.handler)

  def handler(self, signum: int, frame: 'current stack frame object'):
      signame = signal.Signals(signum).name
      s_print(f'{datetime.utcnow()} Signal handler called with signal name:{signame}, num:{signum}')
      self.stop_listening()
      os._exit(0) 

  def stop_listening(self):
      s_print(f'{datetime.utcnow()} closing socket we were listening on.. %s:%d' % (self.ip, self.port))
      self.sock.shutdown
      self.sock.close()
      if self.filepath != None: 
        self.sbn_message.close()
        self.write_json()

  def read_time_sync(self, time_sync_raw: bytes):
      time_sync = Noaaport.time_sync_meta(*Noaaport.time_sync_struct.unpack(time_sync_raw)) 
      this_date = datetime.utcfromtimestamp(time_sync.time_send)
      if self.verbose: s_print(f'** Time Sync Packet: {this_date}, offset: {(datetime.utcnow() - this_date)})')

  def read_frame_header(self, flh_raw: bytes, expected_sbn_seq: int) -> 'flh: namedtuple, expected_sbn_seq: int' :
      flh = Noaaport.flh_meta(*Noaaport.flh_struct.unpack(flh_raw)) 
      if expected_sbn_seq != int(flh.SBN_sequence_number):
        s_print(f'  Received an SBN message/packet out of sequence! Expected {expected_sbn_seq}, got {flh.SBN_sequence_number}')
      expected_sbn_seq = int(flh.SBN_sequence_number) + 1 
      if self.verbose >= 3: s_print('    FLH:', [flh.__getattribute__(i) for i in flh._fields])
      return flh, expected_sbn_seq

  def read_sbn_header(self, sbn_raw: bytes) -> 'sbn: namedtuple' :
      sbn = Noaaport.sbn_meta(*Noaaport.sbn_struct.unpack(sbn_raw)) 
      if self.verbose >= 2: s_print( f' flh.SBN_seq#: {self.flh.SBN_sequence_number} sbn: Product_seq#:'
                f'{sbn.Product_sequence_number} Block#:{sbn.Block_number} data_size:{sbn.Data_block_size}'
                f' Header_len:{sbn.Header_length} Transfer_type:{sbn.Transfer_type}' )
      if self.verbose >= 3: s_print('    SBN:', [sbn.__getattribute__(i) for i in sbn._fields])
      return sbn

  def read_psh_header(self, psh_raw: bytes) -> 'psh: namedtuple':
      psh = Noaaport.psh_meta(*Noaaport.psh_struct.unpack(psh_raw)) 
      if self.verbose >= 3:
        s_print(f'    PSH:', [psh.__getattribute__(i) for i in psh._fields], end='')
        s_print(f' recv:{human_time(psh.recv_time)} send:{human_time(psh.send_time)}')
      return psh 

  def read_wmo_header(self, wmo_raw: bytes) -> 'wmo: namedtuple, wmo_raw: str' :
      wmo_size = len(wmo_raw)
      try:
        wmo_size = wmo_raw.rindex(b'\r\r\n') + 3
      except ValueError as ve:
        if self.verbose >= 3: s_print(f'wmo header does not end in "\r\r\n", {ve}. Attempting "\n" instead.')
        wmo_size = wmo_raw.rindex(b'\n') + 1
      except Error as e:
        s_print(f'Error parsing WMO. {e}\n{wmo_raw} {wmo_size}')
      wmo_raw_trim = f'{wmo_raw[:wmo_size]}' 
      wmo_header = wmo_raw[:wmo_size].replace(b'\r\r\n', b' ').replace(b'\r\n', b' ').decode().strip()
      wmo_header = wmo_header[:4] + ' ' + wmo_header[4:] 
      rstation, wmo_id, station, ddhhmm = wmo_header.split(maxsplit=3)
      yearm = datetime.now().strftime("%Y%m")
      Ymd = datetime.strptime(yearm+ddhhmm[:6], "%Y%m%d%H%M").strftime("%Y%m%d_%H%M")
      channel = '' 
      if len(ddhhmm) > 6:
        channel = ddhhmm[7:]
      wmo = self.wmo_meta(wmo_header, rstation, wmo_id, station, ddhhmm[:6], channel, Ymd, wmo_size)
      if self.verbose >= 2:      s_print(f'    WMO Header: {wmo_header} size: {wmo_size} raw:{wmo_raw_trim}')
      if self.verbose >= 3: s_print(f'    WMO:', [wmo.__getattribute__(i) for i in wmo._fields])
      return wmo, wmo_raw_trim

  def read_first_packet(self, data: bytes):
      psh = self.read_psh_header(data[32:68]) 
      prod_cat = Noaaport.PROD_CAT.get(psh.category, 'OTHER') 
      wmo_start = self.sbn.Header_length + psh.psh_size
      wmo, wmo_raw = self.read_wmo_header(data[wmo_start:wmo_start+40]) 
      data_start = wmo_start + wmo.size
      ext = get_extension(data[data_start:data_start+8]) 
      if ext == 'ncf.txt':
          if self.verbose: s_print(f'** NCF Status Message:\n{data[data_start:].decode()}')
          return
      elif ext == 'none' and Noaaport.nexrad_l3_wmo_finder.search(wmo.header):
          ext = 'nexrad_l3'
      filename = '.'.join(['NOAAPORT', prod_cat, wmo.wmo_id, wmo.station, wmo.ymd[6:8]+wmo.ymd[9:],
                  datetime.utcnow().strftime("%Y%j%H%M%S%f")[:-3], wmo.awips, ext]).replace('..', '.')
      self.filepath = os.path.join(self.data_dir, filename)
      if self.verbose: s_print(f' Receiving product {self.filepath} ({psh.num_frags} fragments)')
      self.sbn_message = BytesIO() 
      self.sbn_json = StringIO()   
      self.sbn_message.write(data[data_start:])
      self.sbn_json.write(json.dumps({'product_info':f'{datetime.utcnow()} {filename} ({psh.num_frags} fragments)',
        f'packet_{self.sbn.Block_number}_meta':self.packet_meta, 'FLH':self.flh._asdict(), 'SBN':self.sbn._asdict(),
        'PSH':psh._asdict(), 'WMO':wmo._asdict()}, indent=2, separators=(',', ': '))) 
      if psh.num_frags == 0: 
        self.write_data()
        self.write_json()

  def read_data_packet(self, data: bytes):
      if (self.filepath != None): 
        self.sbn_message.write(data[32:]) 
        self.sbn_json.write(',') 
        self.sbn_json.write(json.dumps({f'packet_{self.sbn.Block_number}_meta':self.packet_meta,'FLH':self.flh._asdict(),
                              'SBN':self.sbn._asdict()}, indent=2, separators=(',', ': ')))
        if (self.sbn.Transfer_type in [6, 22]): 
          self.write_data() 
          self.write_json() 
      elif self.verbose >= 2: s_print(f' Reading data packet without the product spec / info (fragment# {self.sbn.Block_number})')

  def wait_for_data(self):
      expected_sbn_sequence = 0
      while True: 
        data, (host, port) = self.sock.recvfrom(65507) 
        self.packet_meta = (f'{datetime.utcnow()} Received packet ({len(data)} bytes) on {self.ip}:{self.port} (channel:'
          f' {self.channel}) from {host}:{port}')
        if self.verbose >= 3: s_print(self.packet_meta)

        self.flh, expected_sbn_sequence = self.read_frame_header(data[:16], expected_sbn_sequence) 
        if self.flh.SBN_command == 5: 
          self.read_time_sync(data[16:])
          continue

        self.sbn = self.read_sbn_header(data[16:32]) 
        if self.sbn.Header_length == 16: 
          self.read_data_packet(data)
          continue

        if self.sbn.Block_number == 0: 
          self.read_first_packet(data)
          continue

        if self.verbose: s_print(f'We shouldnt have gotten this far but..meta: {self.packet_meta}, flh: {self.flh}; and all known cases handled.')

  def write_data(self): 
      if self.verbose >= 2: s_print(f'    writing sbn_message to file {self.filepath} of size '
        f'{human_size(len(self.sbn_message.getbuffer()))}')
      with open(self.filepath, 'wb') as data_file:
        data_file.write(self.sbn_message.getvalue()) 
      self.sbn_message.close() 

  def write_json(self): 
      if self.verbose >= 2: s_print(f'    writing metadata to file {self.filepath}.json')
      with open(self.filepath + '.json', 'w') as json_file:
        json_file.write(self.sbn_json.getvalue()) 
      self.sbn_json.close() 
      self.filepath = None 

def get_extension(first_bytes: bytes) -> str:
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

def human_time(epoch_secs: int) -> 'A human readable date/time string':
    my_struct = gmtime(epoch_secs)
    my_time = datetime(*my_struct[:6], tzinfo=timezone.utc)
    return my_time.strftime('%Y_%m_%d_%H:%M:%S')

def human_size(fsize: str, units=[' bytes','KB','MB','GB','TB', 'PB', 'EB']) -> 'A human readable string representation of bytes':
  return "{:.1f}{}".format(float(fsize), units[0]) if float(fsize) < 1024 else human_size(fsize / 1024, units[1:])

s_print_lock = threading.Lock() 
def s_print(*a, **b):
    with s_print_lock:
        print(*a, **b)

def setup_arg_parser(description: str) -> ArgumentParser:
    parser = ArgumentParser(description=description)
    parser.add_argument('channel', help='Specify the channel you want to listen to',
      choices=[str(x) for x in Noaaport.channels] + ['all'])
    parser.add_argument('-v', '--verbose', help='Make output more verbose. Can be used '
      'multiple times.', action='count', default=0)
    parser.add_argument('-d', '--dest', help='Specify the destination for received products',
      type=str, default='/data/temp/')
    return parser

def main():
    args = setup_arg_parser(desc).parse_args()
    if args.channel != 'all':
      n = Noaaport(args.channel, args.dest, args.verbose)

if __name__ == '__main__':
    main()
