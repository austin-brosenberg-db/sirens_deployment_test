import math as m

from pyspark.sql.functions import expr, udf
from pyspark.sql.types import StructType, StructField, IntegerType, StringType
import timezonefinder


class GeoFeatures(object):
    """A class for extracting Geographic features."""

    latLonType = StructType([
        StructField('lat', IntegerType(), True),
        StructField('lon', IntegerType(), True)
    ])

    """Can be used as a normalization constant for distance measurements."""
    maxHalfCircumference = 12455

    @staticmethod
    def distance(from_column, to_column):
        """Pass two column names with latlon structs, get a SQL expression to calculate great circle distance in statute miles.

        :param: from_column = name of start location column containing GeoFeatures.latLonType
        :param: to_column = name of end location column containing GeoFeatures.latLonType"""
        command = """int(2. * 3958.8 * asin(sqrt(pow(sin((radians({l2}.lat) - radians({l1}.lat))/2.), 2) + 
                cos(radians({l1}.lat))* cos(radians({l2}.lat)) * 
                pow(sin((radians({l2}.lon) - radians({l1}.lon))/2.), 2))))""".format(l1=from_column, l2=to_column)
        return expr(command)

    @staticmethod
    def velocity(from_time, to_time, from_location, to_location):
        """Given start and end time and latitude/longitude calculate the velocity in Mach

        :param: from_time = name of start time column containing Spark TimestampType
        :param: to_time = name of end time column containing Spark TimestampType
        :param: from_location = name of start location column containing GeoFeatures.latLonType
        :param: to_location = name of end location column containing GeoFeatures.latLonType

        :returns: A SQL expr() to calculate the velocity to travel between the points in the time given,
                  normalized to the speed of sound.
        """
        command = """
        (2. * 3958.8 * asin(sqrt(pow(sin((radians({l2}.lat) - radians({l1}.lat))/2.), 2) + 
                      cos(radians({l1}.lat))* cos(radians({l2}.lat)) * 
                      pow(sin((radians({l2}.lon) - radians({l1}.lon))/2.), 2)))) * 4.69197
        / (TO_UNIX_TIMESTAMP({t2}) - TO_UNIX_TIMESTAMP({t1}))
        """.format(t1=from_time, t2=to_time, l1=from_location, l2=to_location)
        return expr(command)

    @staticmethod
    def _nearest_f(distance_map, location: latLonType):
        min_dist = 999999  # Much larger than 1/2 the earth's circumference
        min_name = None
        (l1lat, l1lon) = location
        for lat_lon in distance_map.keys():
            l2lat, l2lon = lat_lon
            name = distance_map[lat_lon]
            distance = int(2. * 3958.8 * m.asin(m.sqrt(pow(m.sin((m.radians(l2lat) - m.radians(l1lat))/2.), 2) +
                                                   m.cos(m.radians(l1lat)) * m.cos(m.radians(l2lat)) *
                                                   pow(m.sin((m.radians(l2lon) - m.radians(l1lon))/2.), 2))))
            if distance < min_dist:
                min_dist = distance
                min_name = name
        if min_name:
            return min_name, min_dist
        else:
            return "", None

    @staticmethod
    def nearestUdf(distance_map):  # camelCase to conform with Spark API style
        """Find the closest location from a map of locations

        :param: distance_map = A dictionary {(lat, lon) => 'Location name'}
        :param: location = A location column in  format to test against the distance_map.

        :returns: Spark UDF. When supplied with a location in GeoFunctions.latLonType,
                  returns ('Location Name', distance) for the closest location in distance_map.

        Usage:
        nearestUdf = GeoFeatures.nearestUdf(distance_map)
        df.withColumn("nearest", nearestUdf("location_column"))"""

        result_type = StructType([
            StructField("name", StringType(), True),
            StructField("distance", IntegerType(), True)
        ])
        return udf(lambda location: GeoFeatures._nearest_f(distance_map, location), result_type)

    @staticmethod
    def timezoneUdf():
        """Find the timezone string from a location.

        :returns: Spark UDF. When supplied with a location in GeoFunctions.latLonType,
                  returns the tz database timezone string for that location, e.g. "America/Los_Angeles"

        See https://en.wikipedia.org/wiki/List_of_tz_database_time_zones

        Usage:
        timezoneUdf = GeoFeatures.timezoneUdf()
        df.withColumn("timezone", timezoneUdf("location_column"))"""

        tz = timezonefinder.TimezoneFinderL(in_memory=True).timezone_at
        return udf(lambda location: tz(lat=location.lat, lng=location.lon), StringType())