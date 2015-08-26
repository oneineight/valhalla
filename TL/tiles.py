#!/usr/bin/env python
from __future__ import print_function
import sys
import json
import urllib2
import math
import os
import shutil
import getopt
import time
from collections import defaultdict


#globals for valhalla tiles
minx_ = -180
miny_ = -90
maxx_ = 180
maxy_ = 90

tilesize_ = .25
ncolumns_ = int(math.ceil((maxx_ - minx_) / tilesize_))
nrows_ = int(math.ceil((maxy_ - miny_) / tilesize_))
max_tile_id_ = ((ncolumns_ * nrows_) - 1)
level_ = 2
output_transit_dir_ = '.'

class json_resource_t:
  #constructor
  def __init__(self, url):
    self.url = url
    try:
      #open a readable object at that url
      response = urllib2.urlopen(url)
    except Exception as e:
      print('Could not fetch %s' % url, file=sys.stderr)
      raise e

    try:
      #turn the json into a python dict
      self.kv = json.load(response)
    except Exception as e:
      print('Could not parse response from %s as json' % url, file=sys.stderr)
      raise e

  #put the json to file
  def write(self, file_name, pretty=False):
    with open(file_name, 'w') as f:
      #pretty print
      if pretty:
        json.dump(self.kv, f, indent=2, sort_keys=True)
      #minified
      else:
        json.dump(self.kv, f, separators=(',',':'))

class Tile(object):

  def __init__(self, min_x, min_y, max_x, max_y):
     self.minx = min_x
     self.miny = min_y
     self.maxx = max_x
     self.maxy = max_y

#The bounding boxes do NOT intersect if the other bounding box is
#entirely LEFT, BELOW, RIGHT, or ABOVE bounding box.
  def intersects(self,minx, miny, maxx, maxy):
     if ((minx < self.minx and maxx < self.minx) or
         (miny < self.miny and maxy < self.miny) or
         (minx > self.maxx and maxx > self.maxx) or
         (miny > self.maxy and maxy > self.maxy)):
            return False
     return True

  def digits(self, number):
     digits = 1 if (number < 0) else 0
     while (long(number)):
        number /= 10
        digits += 1
     return int(digits)

  def file(self, row, col, max_tile_id):

     #get the tile id 
     tile_id = (row * ncolumns_) + col

     max_length = self.digits(max_tile_id)

     remainder = max_length % 3
     if(remainder):
        max_length += 3 - remainder

     #if it starts with a zero the pow trick doesn't work
     if(level_ == 0):
        file_suffix = "0"
        file_suffix += '{:,}'.format(level_ * int(pow(10, max_length)) + tile_id).replace(',', '/')
        file_suffix += ".json"
        return file_suffix

     #it was something else
     file_suffix = '{:,}'.format(level_ * int(pow(10, max_length)) + tile_id).replace(',', '/')
     file_suffix += ".json"

     file = output_transit_dir_ + '/' + file_suffix

     directory = os.path.dirname(file)
     if not os.path.exists(directory):
        os.makedirs(directory)

     return file

def main(argv):
   global level_
   global tilesize_
   global output_transit_dir_
   
   configfile = ''
   try:
      opts, args = getopt.getopt(argv,"hc:",["cfile="])
   except getopt.GetoptError:
      print('tiles.py -c <config file: valhalla.json>')
      sys.exit(2)
   for opt, arg in opts:
      if opt == '-h':
         print('tiles.py -c <config file: valhalla.json>')
         sys.exit()
      elif opt in ("-c", "--cfile"):
         configfile = arg

   if not os.path.isfile(configfile):
      print('Config file not found.')
      print('tiles.py -c <config file: valhalla.json>')
      sys.exit()

   with open(configfile) as config_file:    
      valhalla_config = json.load(config_file)

      for level in valhalla_config['mjolnir']['hierarchy']['levels']:
         if 'name' in level.keys():
            if (level['name'] == 'local'):
               if 'level' in level.keys():
                  level_ = level['level']
               else: 
                  print('Using default level.')
               if 'size' in level.keys():
                  tilesize_ = level['size']
               else:
                  print('Using default size.')

      transit = valhalla_config['mjolnir']['transit']
      if 'transit_dir' in transit.keys():
         output_transit_dir_ = transit['transit_dir']
      else:
         print('Using default transit directory.')

   print('Level: ' + str(level_))
   print('Size: ' + str(tilesize_))
   print('Output transit directory: ' + output_transit_dir_)

#this is the entry point to the program
if __name__ == "__main__":

   now = time.strftime("%c")
   print ("Start time: %s" % now )
  
   main(sys.argv[1:])

   directory = output_transit_dir_ + "/" + str(level_)
   shutil.rmtree(directory, ignore_errors=True)

#until we get the list of BB from transitland, default to SF
   minx = -123.0977
   miny = 36.9005
   maxx = -121.2520
   maxy = 38.2576

#used to generate unique keys for onestop IDs
   onestop_key = 1
   onestops = dict()
 
   for row in xrange(0, nrows_):
      for col in xrange(0, ncolumns_):
         
         #tile for world BB
         min_x = minx_ + (col * tilesize_)
         min_y = miny_ + (row * tilesize_)
  	 max_x = min_x + tilesize_
         max_y = min_y + tilesize_
                
         tile = Tile(min_x,min_y,max_x,max_y)
         #After we get the BBs from transitland we will check if any of the BBs
         #intersect this tile
         if tile.intersects(minx,miny,maxx,maxy): 
            
            dictionary = defaultdict(list)
            file = tile.file( row, col, max_tile_id_)

            url = 'http://dev.transit.land/api/v1/stops?per_page=4000&bbox='
            url += str(min_x) + ',' + str(min_y) + ',' + str(max_x) + ',' + str(max_y)
            while url:
               
               tl_stops = json_resource_t(url)
               meta = tl_stops.kv.get('meta', [])

               for tl_s in tl_stops.kv.get('stops', []):
                  onestop_id = tl_s['onestop_id']
                  if onestop_id not in onestops:
                     onestops[onestop_id] = onestop_key
                     onestop_key = onestop_key + 1
                  
                  tl_s['key'] = onestops[onestop_id]
                  dictionary['stops'].append(tl_s)

               if not dictionary:
                  break

               print(url)
               dictionary['stops_url'].append(url)

               if 'next' in meta.keys():
                  url = meta['next']
               else:
                  url = ""

            if not dictionary:
               continue

            stop_pairs = defaultdict(list)

            url = 'http://dev.transit.land/api/v1/schedule_stop_pairs?per_page=4000&bbox='
            url += str(min_x) + ',' + str(min_y) + ',' + str(max_x) + ',' + str(max_y)

            while url:
               print(url) 

               tl_stop_pairs = json_resource_t(url)
               meta = tl_stop_pairs.kv.get('meta', [])

               dictionary['schedule_stop_pairs_url'].append(url)

               for tl_s_p in tl_stop_pairs.kv.get('schedule_stop_pairs', []):

                  onestop_id = tl_s_p['origin_onestop_id']
                  if onestop_id not in onestops:
                     onestops[onestop_id] = onestop_key
                     onestop_key = onestop_key + 1

                  tl_s_p['origin_key'] = onestops[onestop_id]
                
                  onestop_id = tl_s_p['destination_onestop_id']
                  if onestop_id not in onestops:
                     onestops[onestop_id] = onestop_key
                     onestop_key = onestop_key + 1

                  tl_s_p['destination_key'] = onestops[onestop_id] 
                  
                  onestop_id = tl_s_p['route_onestop_id']
                  if onestop_id not in onestops:
                     onestops[onestop_id] = onestop_key
                     onestop_key = onestop_key + 1

                  tl_s_p['route_key'] = onestops[onestop_id]

                  stop_pairs['schedule_stop_pairs'].append(tl_s_p)

               #should never happen.  Stops exist, but stop_pairs do not.
               if not stop_pairs:
                  break;

               if 'next' in meta.keys():
                  url = meta['next']
               else:
                  url = ""

            if not stop_pairs:
               raise ValueError('Stops exist, but stop_pairs do not!')

            dictionary.update(stop_pairs)

            routes = defaultdict(list)

            url = 'http://dev.transit.land/api/v1/routes?per_page=4000&bbox='
            url += str(min_x) + ',' + str(min_y) + ',' + str(max_x) + ',' + str(max_y)

            while url:
               print(url)

               tl_routes = json_resource_t(url)
               meta = tl_routes.kv.get('meta', [])

               routes['route_url'].append(url)

               for tl_r in tl_routes.kv.get('routes', []):

                  onestop_id = tl_r['onestop_id']
                  if onestop_id not in onestops:
                     onestops[onestop_id] = onestop_key
                     onestop_key = onestop_key + 1

                  tl_r['key'] = onestops[onestop_id]

                  routes['routes'].append(tl_r)

               #should never happen.  Stops and stop_pairs exist, but routes do not.
               if not routes:
                  break;

               if 'next' in meta.keys():
                  url = meta['next']
               else:
                  url = ""

            if not routes:
               raise ValueError('Stops and stop_pairs exist, but routes do not!')

            dictionary.update(routes)

            with open(file, 'w') as f:
               json.dump(dictionary, f, indent=2, sort_keys=True)

   now = time.strftime("%c")   
   print("End time: %s" % now )

