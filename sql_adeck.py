#!/usr/bin/python
import sqlite3 as sql
import csv
import urllib2
import subprocess
import gzip
import re
import glob
import os
import ftplib
import time as _time
from datetime import datetime

def open_ftp():
  ftp = ftplib.FTP("ftp.nhc.noaa.gov")
  ftp.login("anonymous","abrammer@albany.edu")
  ftp.cwd("atcf/aid_public/")
  return ftp

def check_ftp(ftp):
  try:
    ret = ftp.voidcmd("NOOP");
  except:
    print "Reopening ftp";
    ftp = open_ftp();
  return ftp

def grab_file(ftp, f):
    ftp.retrbinary("RETR "+f, open(f,"wb").write)
    resp = ftp.sendcmd("MDTM "+f)
    timestamp = _time.mktime(datetime.strptime(resp[4:18],"%Y%m%d%H%M%S").timetuple())
    os.utime(f, (timestamp, timestamp))

def ftp_fetch_recent(ftp, f):  
  ftp = check_ftp(ftp);
  print "Checking recent "+f;
  if not os.path.isfile(f):
    grab_file(ftp, f)
    ret = True
  else:
    ftp_mt = ftp.sendcmd("MDTM "+f)
    ftp_mt = int(datetime.strptime(ftp_mt[4:], "%Y%m%d%H%M%S").strftime("%s"))
    loc_mt = int(os.path.getmtime(f));
    if ftp_mt > loc_mt: 
      grab_file(ftp, f)
      ret = True
    else:
      ret = False
  return ret


conn = sql.connect('adecks')
cursor=conn.cursor()
conn.execute('CREATE TABLE IF NOT EXISTS atl(id TEXT, date REAL, tech TEXT, fhr INT, lat REAL, lon REAL, vmax INT, mslp INT, type TEXT);')


recent_time = conn.execute('SELECT MAX(date) FROM atl').fetchone()[0]
if recent_time is None:
    recent_time = 0

print recent_time
#files =[]
#for f in glob.glob("./*2015.dat.gz"):
# files.append( [ f, os.path.getmtime(f)] )

#subprocess.call(' wget --quiet -np -nd -rcN -A "*2015.dat.gz" http://ftp.nhc.noaa.gov/atcf/aid_public/', shell=True)
#subprocess.call(' wget --quiet -np -nd -rcN -A "*2015.dat" http://www.ral.ucar.edu/hurricanes/repository/data/adecks_open/', shell=True)
#subprocess.call('wget --quiet -cN http://ftp.nhc.noaa.gov/atcf/aid_public/a{al,ep}{00..99}2015.dat.gz',shell=True)
# subprocess.call('wget --quiet -cN http://www.ral.ucar.edu/hurricanes/repository/data/adecks_open/'+id+'2015.dat', shell=True)
ftp = open_ftp();
f_files = ftp.nlst("*2015*");

for f in f_files:
 if ftp_fetch_recent(ftp, f):
   filli = f;
   print filli;
#for filli in glob.glob("./*2015.dat.gz"):
# for ff in files:
#  if filli in ff and os.path.getmtime(filli) != ff[1]:
   id =  re.sub('./','',re.sub('2015.dat.gz','',filli))
   print "----- "+id
   with gzip.open(filli,'rb') as f:
     reader = csv.reader(f)
     for row in reader:
        time = int(row[2])
        tech = row[4].strip()
        fhr = int(row[5])
        lat = int(row[6][:-1])/10.
        if row[6][-1] == 'S':
           lat = -lat
        lon = int(row[7][:-1])/10.
        if row[7][-1] == 'W':
           lon = -lon
        vmax = int(row[8])
	try:
          mslp = int(row[9])
	except:
	  mslp = 0
#	if(len(row)>=11):
	try:
          type = row[10].strip()
	except:
	  type = ""
#        print tech, fhr, "\n"
        if time < recent_time-48:
           continue
#        elif time == recent_time:
#             print "time may exist already"
#            data = conn.execute('SELECT 1 FROM atl WHERE id=? AND tech=? AND date=? AND fhr=?',[id, tech, time,fhr] ).fetchone()
#            if data is None:
#		print time, id, tech
#        conn.execute('INSERT OR IGNORE INTO atl values (?,?,?,?,?,?,?,?,?)',[id,time, tech, fhr, lat, lon, vmax, mslp, type])
        else:
#           print time, id, tech
#           print "time does not exist already"
           conn.execute('INSERT OR IGNORE INTO atl values (?,?,?,?,?,?,?,?,?)',[id,time, tech, fhr, lat, lon, vmax, mslp, type])
conn.commit()


