{
 "cells": [
  {
   "cell_type": "code",
   "execution_count": 1,
   "metadata": {},
   "outputs": [],
   "source": [
    "from pyspark import SparkContext\n",
    "sc = SparkContext()"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 2,
   "metadata": {},
   "outputs": [],
   "source": [
    "# We construct 2 RDDs for the bike and taxi data set\n",
    "\n",
    "bike = sc.textFile('citibike.csv')\n",
    "taxi = sc.textFile('yellow.csv.gz')"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 4,
   "metadata": {},
   "outputs": [
    {
     "data": {
      "text/plain": [
       "[(0, 'cartodb_id'),\n",
       " (1, 'the_geom'),\n",
       " (2, 'tripduration'),\n",
       " (3, 'starttime'),\n",
       " (4, 'stoptime'),\n",
       " (5, 'start_station_id'),\n",
       " (6, 'start_station_name'),\n",
       " (7, 'start_station_latitude'),\n",
       " (8, 'start_station_longitude'),\n",
       " (9, 'end_station_id'),\n",
       " (10, 'end_station_name'),\n",
       " (11, 'end_station_latitude'),\n",
       " (12, 'end_station_longitude'),\n",
       " (13, 'bikeid'),\n",
       " (14, 'usertype'),\n",
       " (15, 'birth_year'),\n",
       " (16, 'gender')]"
      ]
     },
     "execution_count": 4,
     "metadata": {},
     "output_type": "execute_result"
    }
   ],
   "source": [
    "# As usual, let's inspect the columns of their columns first\n",
    "list(enumerate(bike.first().split(',')))"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 5,
   "metadata": {},
   "outputs": [
    {
     "data": {
      "text/plain": [
       "[(0, 'tpep_pickup_datetime'),\n",
       " (1, 'tpep_dropoff_datetime'),\n",
       " (2, 'pickup_latitude'),\n",
       " (3, 'pickup_longitude'),\n",
       " (4, 'dropoff_latitude'),\n",
       " (5, 'dropoff_longitude')]"
      ]
     },
     "execution_count": 5,
     "metadata": {},
     "output_type": "execute_result"
    }
   ],
   "source": [
    "list(enumerate(taxi.first().split(',')))"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 6,
   "metadata": {},
   "outputs": [],
   "source": [
    "# We filter all the bike trips that originate from\n",
    "# the station of interest, and only for the Feb-01.\n",
    "# We mark each output with a one\n",
    "\n",
    "def filterBike(records):\n",
    "    for record in records:\n",
    "        fields = record.split(',')\n",
    "        if (fields[6]=='Greenwich Ave & 8 Ave' and \n",
    "            fields[3].startswith('2015-02-01')):\n",
    "            yield (fields[3][:19], 1)\n",
    "\n",
    "matchedBike = bike.mapPartitions(filterBike)"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 7,
   "metadata": {},
   "outputs": [
    {
     "data": {
      "text/plain": [
       "[('2015-02-01 00:05:00', 1), ('2015-02-01 00:05:00', 1)]"
      ]
     },
     "execution_count": 7,
     "metadata": {},
     "output_type": "execute_result"
    }
   ],
   "source": [
    "matchedBike.take(2)"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 8,
   "metadata": {},
   "outputs": [],
   "source": [
    "# This is location of the station of interest (we got it\n",
    "# by inspecting the records matching the station name)\n",
    "\n",
    "bikeStation = (-74.00263761, 40.73901691)"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 9,
   "metadata": {},
   "outputs": [
    {
     "data": {
      "text/plain": [
       "7278"
      ]
     },
     "execution_count": 9,
     "metadata": {},
     "output_type": "execute_result"
    }
   ],
   "source": [
    "# Knowing the station location, we can then filter all taxi\n",
    "# trips that dropped passengers around that station, mark-\n",
    "# ing each output with a zero\n",
    "\n",
    "def filterTaxi(pid, lines):\n",
    "    if pid==0:\n",
    "        next(lines)\n",
    "    import pyproj\n",
    "    proj = pyproj.Proj(init=\"epsg:2263\", preserve_units=True)\n",
    "    station = proj(-74.00263761, 40.73901691)\n",
    "    squared_radius = 1320**2\n",
    "    for trip in lines:\n",
    "            fields = trip.split(',')\n",
    "            if 'NULL' in fields[4:6]: continue # ignore trips without locations\n",
    "            dropoff = proj(fields[5], fields[4])\n",
    "            squared_distance = (dropoff[0]-station[0])**2 + (dropoff[1]-station[1])**2\n",
    "            if (fields[1].startswith('2015-02-01') and\n",
    "                squared_distance <= squared_radius):\n",
    "                yield (fields[1][:19], 0)\n",
    "\n",
    "matchedTaxi = taxi.mapPartitionsWithIndex(filterTaxi)\n",
    "matchedTaxi.count()"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 10,
   "metadata": {},
   "outputs": [
    {
     "data": {
      "text/plain": [
       "[('2015-02-01 00:11:03', 0), ('2015-02-01 00:10:23', 0)]"
      ]
     },
     "execution_count": 10,
     "metadata": {},
     "output_type": "execute_result"
    }
   ],
   "source": [
    "matchedTaxi.take(2)"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 11,
   "metadata": {},
   "outputs": [],
   "source": [
    "# Next we combine both data and sort them by time and marking\n",
    "allTrips = (matchedBike+matchedTaxi).sortByKey().cache()"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 30,
   "metadata": {},
   "outputs": [
    {
     "data": {
      "text/plain": [
       "65"
      ]
     },
     "execution_count": 30,
     "metadata": {},
     "output_type": "execute_result"
    }
   ],
   "source": [
    "# From the sorted list, we can keep the last taxi dropped off\n",
    "# time before any bike pickup to see whether it is within 10\n",
    "# minutes. Similar to the first ride of the day problem.\n",
    "def connectTrips(_, records):\n",
    "    import datetime\n",
    "    lastTaxiTime = None\n",
    "    count = 0\n",
    "    for dt,mode in records:\n",
    "        t = datetime.datetime.strptime(dt, '%Y-%m-%d %H:%M:%S')\n",
    "        if mode==1:\n",
    "            if lastTaxiTime!=None:\n",
    "                diff = (t-lastTaxiTime).total_seconds()\n",
    "                if diff>=0 and diff<=600:\n",
    "                    count += 1\n",
    "        else:\n",
    "            lastTaxiTime = t\n",
    "    yield(count)\n",
    "\n",
    "allTrips.mapPartitionsWithIndex(connectTrips).reduce(lambda x,y: x+y)"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": null,
   "metadata": {},
   "outputs": [],
   "source": []
  }
 ],
 "metadata": {
  "kernelspec": {
   "display_name": "Python 3",
   "language": "python",
   "name": "python3"
  },
  "language_info": {
   "codemirror_mode": {
    "name": "ipython",
    "version": 3
   },
   "file_extension": ".py",
   "mimetype": "text/x-python",
   "name": "python",
   "nbconvert_exporter": "python",
   "pygments_lexer": "ipython3",
   "version": "3.7.3"
  }
 },
 "nbformat": 4,
 "nbformat_minor": 2
}
