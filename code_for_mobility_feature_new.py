from pyspark.sql import SparkSession, Window
import pyspark.sql.functions as F
from pyspark.sql.functions import to_timestamp
import datetime as dt
import os
import time

start = time.time()
print('start time : ', start)

#methode sql_code
def write_sql_code(calls = 'calls',
                   start_date = "\'2020-02-01\'",
                   end_date =  "\'2020-03-31\'",
                   start_date_weeks =  "\'2020-02-03\'",
                   end_date_weeks = "\'2020-03-29\'"):
    
    sql_code = {
    # Aggregate 1 (April 1 version)
    'count_unique_subscribers_per_region_per_day' :
    """
        SELECT * FROM (
            SELECT calls.call_date AS visit_date,
                cells.region AS region,
                count(DISTINCT msisdn) AS subscriber_count
            FROM calls
            INNER JOIN cells
                ON calls.location_id = cells.cell_id
            WHERE calls.call_date >= {}
                AND calls.call_date <= CURRENT_DATE
            GROUP BY 1, 2
        ) AS grouped
        WHERE grouped.subscriber_count >= 15
        """.format(start_date),

    # Intermediate Result - Home location
    'home_locations' :
    """
        SELECT msisdn, region FROM (
            SELECT
                msisdn,
                region,
                row_number() OVER (
                    PARTITION BY msisdn
                    ORDER BY total DESC, latest_date DESC
                ) AS daily_location_rank
            FROM (

                SELECT msisdn,
                    region,
                    count(*) AS total,
                    max(call_date) AS latest_date
                FROM (
                    SELECT calls.msisdn,
                        cells.region,
                        calls.call_date,
                        row_number() OVER (
                            PARTITION BY calls.msisdn, calls.call_date
                            ORDER BY calls.call_datetime DESC
                        ) AS event_rank
                    FROM calls
                    INNER JOIN cells
                        ON calls.location_id = cells.cell_id
                    WHERE calls.call_date >= {}
                        AND calls.call_date <= {}

                ) ranked_events

                WHERE event_rank = 1
                GROUP BY 1, 2

            ) times_visited
        ) ranked_locations
        WHERE daily_location_rank = 1
        """.format(start_date, end_date),

    # Aggregate 2 (April 1 version)
    'count_unique_active_residents_per_region_per_day' :
    """
        SELECT * FROM (
            SELECT calls.call_date AS visit_date,
                cells.region AS region,
                count(DISTINCT calls.msisdn) AS subscriber_count
            FROM calls
            INNER JOIN cells
                ON calls.location_id = cells.cell_id
            INNER JOIN home_locations homes     -- See intermediate_queries.sql for code to create the home_locations table
                ON calls.msisdn = homes.msisdn
                AND cells.region = homes.region
            GROUP BY 1, 2
        ) AS grouped
        WHERE grouped.subscriber_count >= 15""",

    'count_unique_visitors_per_region_per_day' :
    """
        SELECT * FROM (
            SELECT all_visits.visit_date,
                all_visits.region,
                all_visits.subscriber_count - coalesce(home_visits.subscriber_count, 0) AS subscriber_count
            FROM count_unique_subscribers_per_region_per_day all_visits
            LEFT JOIN count_unique_active_residents_per_region_per_day home_visits
                ON all_visits.visit_date = home_visits.visit_date
                AND all_visits.region = home_visits.region
        ) AS visitors
        WHERE visitors.subscriber_count >= 15""",

    # Aggregate 3 (April 1 version)
    'count_unique_subscribers_per_region_per_week' :
    """
        SELECT * FROM (
            SELECT extract(WEEK FROM calls.call_date) AS visit_week,
                cells.region AS region,
                count(DISTINCT calls.msisdn) AS subscriber_count
            FROM calls
            INNER JOIN cells
                ON calls.location_id = cells.cell_id
            WHERE calls.call_date >= {}
                AND calls.call_date <= {}
            GROUP BY 1, 2
        ) AS grouped
        WHERE grouped.subscriber_count >= 15
        """.format(start_date_weeks, end_date_weeks),

    # Aggregate 4 (April 1 version)
    'count_unique_active_residents_per_region_per_week' :
    """
    SELECT * FROM (
            SELECT extract(WEEK FROM calls.call_date) AS visit_week,
                cells.region AS region,
                count(DISTINCT calls.msisdn) AS subscriber_count
            FROM calls
            INNER JOIN cells
                ON calls.location_id = cells.cell_id
            INNER JOIN home_locations homes     -- See intermediate_queries.sql for code to create the home_locations table
                ON calls.msisdn = homes.msisdn
                AND cells.region = homes.region
            WHERE calls.call_date >= {}
                AND calls.call_date <= {}
            GROUP BY 1, 2
        ) AS grouped
        WHERE grouped.subscriber_count >= 15
        """.format(start_date_weeks, end_date_weeks),

    'count_unique_visitors_per_region_per_week' :
    """
    SELECT * FROM (
            SELECT all_visits.visit_week,
                all_visits.region,
                all_visits.subscriber_count - coalesce(home_visits.subscriber_count, 0) AS subscriber_count
            FROM count_unique_subscribers_per_region_per_week all_visits
            LEFT JOIN count_unique_active_residents_per_region_per_week home_visits
                ON all_visits.visit_week = home_visits.visit_week
                AND all_visits.region = home_visits.region
        ) AS visitors
        WHERE visitors.subscriber_count >= 15""",

    # Aggregate 5 (April 1 version)
    'regional_pair_connections_per_day' :
    """
    SELECT * FROM (
            SELECT connection_date,
                region1,
                region2,
                count(*) AS subscriber_count
            FROM (

                SELECT t1.call_date AS connection_date,
                    t1.msisdn AS msisdn,
                    t1.region AS region1,
                    t2.region AS region2
                FROM (
                    SELECT DISTINCT calls.msisdn,
                        calls.call_date,
                        cells.region
                    FROM calls
                    INNER JOIN cells
                        ON calls.location_id = cells.cell_id
                    WHERE calls.call_date >= {}
                        AND calls.call_date <= CURRENT_DATE
                    ) t1

                    FULL OUTER JOIN

                    (
                    SELECT DISTINCT calls.msisdn,
                        calls.call_date,
                        cells.region
                    FROM calls
                    INNER JOIN cells
                        ON calls.location_id = cells.cell_id
                    WHERE calls.call_date >= {}
                        AND calls.call_date <= CURRENT_DATE
                    ) t2

                    ON t1.msisdn = t2.msisdn
                    AND t1.call_date = t2.call_date
                WHERE t1.region < t2.region

            ) AS pair_connections
            GROUP BY 1, 2, 3
        ) AS grouped
        WHERE grouped.subscriber_count >= 15
        """.format(start_date, start_date),

    # Aggregate 6 (April 2 version)
    'directed_regional_pair_connections_per_day' :
    """
        WITH subscriber_locations AS (
            SELECT calls.msisdn,
                calls.call_date,
                cells.region,
                min(calls.call_datetime) AS earliest_visit,
                max(calls.call_datetime) AS latest_visit
            FROM calls
            INNER JOIN cells
                ON calls.location_id = cells.cell_id
            WHERE calls.call_date >= {}
                AND calls.call_date <= CURRENT_DATE
            GROUP BY msisdn, call_date, region
        )
        SELECT * FROM (
            SELECT connection_date,
                msisdn,
                region_from,
                region_to,
                count(*) AS subscriber_count
            FROM (

                SELECT t1.call_date AS connection_date,
                    t1.msisdn AS msisdn,
                    t1.region AS region_from,
                    t2.region AS region_to
                FROM subscriber_locations t1
                FULL OUTER JOIN subscriber_locations t2
                ON t1.msisdn = t2.msisdn
                    AND t1.call_date = t2.call_date
                WHERE t1.region <> t2.region
                    AND t1.earliest_visit < t2.latest_visit

            ) AS pair_connections
            GROUP BY 1, 2, 3
        ) AS grouped
        """.format(start_date),

    # Aggregate 7 (April 3 version)
    'total_calls_per_region_per_day' :
    """
        SELECT
            call_date,
            region,
            total_calls
        FROM (
            SELECT calls.call_date AS call_date,
                cells.region AS region,
                count(DISTINCT msisdn) AS subscriber_count,
                count(*) AS total_calls
            FROM calls
            INNER JOIN cells
                ON calls.location_id = cells.cell_id
            WHERE calls.call_date >= {}
                AND calls.call_date <= CURRENT_DATE
            GROUP BY 1, 2
        ) AS grouped
        WHERE grouped.subscriber_count >= 15
        """.format(start_date),

    # Aggregate 8 (April 3 version)
    'home_location_counts_per_region' :
    """
        SELECT * FROM (
            SELECT region, count(msisdn) AS subscriber_count
            FROM home_locations     -- See intermediate_queries.sql for code to create the home_locations table
            GROUP BY region
        ) AS home_counts
        WHERE home_counts.subscriber_count >= 15"""}
    return sql_code


######## Priority Indicators ########

#### Indicator 1

# result:
# - apply sample period filter
# - groupby region and frequency
# - then count observations
# - apply privacy filter

def transactions(df, time_filter, frequency):
    #ajout de 'msisdn' et suppression de la clause 'where'
    result = df.where(time_filter)\
        .groupby(frequency, 'region', 'msisdn')\
        .count()
    
    return result




#### Indicator 2

# result:
# - apply sample period filter
# - groupby region and frequency
# - then count distinct sims
# - apply privacy filter

def unique_subscribers(df, time_filter, frequency):
    #suppression de la clause where
    result = df.where(time_filter)\
        .groupby(frequency, 'region')\
        .agg(F.countDistinct('msisdn')\
        .alias('count'))

    return result



#### Indicator 3

# result:
# - apply sample period filter
# - groupby frequency
# - then count distinct sims
# - apply privacy filter

def unique_subscribers_country(df, time_filter, frequency):
    #suppression de la clause where
    result = df.where(time_filter)\
        .groupby(frequency)\
        .agg(F.countDistinct('msisdn')\
        .alias('count'))

    return result





#### Indicator 5

# assert correct frequency

# result (intra-day od):
# - get intra-day od matrix using flowminder definition

# prep (inter-day od):
# - apply sample period filter
# - create timestamp lag per user
# - create day lag per user, with a max calue of 7 days
# - filter for observations that involve a day change (cause we have intra-day already)
# - also filter for region changes only, since we are computing od matrix
# - groupby o(rigin) and (d)estination, and frequency
# - count observations
# - apply privacy filter

# result (joining intra-day od (result) and inter-day od (prep)):
# - join on o, d, and frequency
# - fill columns for NA's that arose in merge, so that we have complete columns
# - compute total od summing intra and inter od count

def origin_destination_connection_matrix(df, time_filter, frequency):

    assert frequency == 'day', 'This indicator is only defined for daily frequency'

    result = spark.sql(sql_code['directed_regional_pair_connections_per_day'])

    prep = df.where(time_filter).withColumn('call_datetime_lag', F.lag('call_datetime')\
        .over(user_window))\
        .withColumn('day_lag', F.when((F.col('call_datetime').cast('long') - F.col('call_datetime_lag').cast('long')) <= (cutoff_days * 24 * 60 * 60), F.lag('day').over(user_window))\
        .otherwise(F.col('day')))\
        .where((F.col('region_lag') != F.col('region')) & ((F.col('day') > F.col('day_lag'))))\
        .groupby(frequency, 'region', 'region_lag', 'msisdn')\
        .agg(F.count(F.col('msisdn')).alias('od_count'))

    result = result.join(prep, (prep.region == result.region_to) & (prep.region_lag == result.region_from) & (prep.day == result.connection_date) & (prep.msisdn == result.msisdn), 'full')\
        .withColumn('region_to', F.when(F.col('region_to').isNotNull(), F.col('region_to')).otherwise(F.col('region')))\
        .withColumn('region_from', F.when(F.col('region_from').isNotNull(), F.col('region_from')).otherwise(F.col('region_lag')))\
        .withColumn('connection_date', F.when(F.col('connection_date').isNotNull(), F.col('connection_date')).otherwise(F.col('day')))\
        .na.fill({'od_count' : 0, 'subscriber_count' : 0})\
        .withColumn('total_count', F.col('subscriber_count') + F.col('od_count'))\
        .drop('region').drop('region_lag').drop('day')

    return result


#### Indicator 6 helper method to find home locations for a given frequency

# define user-day and user-frequency windows

# result:
# - apply sample period filter
# - Get last timestamp of the day for each user
# - Dummy when an observation is the last of the day
# - Count how many times a region is the last of the day
# - Dummy for the region with highest count
# - Keep only the region with higest count
# - Keep only one observation of that region

def assign_home_locations(df, time_filter, frequency):

    user_day = Window.orderBy(F.desc_nulls_last('call_datetime')).partitionBy('msisdn', 'day')

    user_frequency = Window.orderBy(F.desc_nulls_last('last_region_count')).partitionBy('msisdn', frequency)

    result = df.where(time_filter).na.fill({'region' : missing_value_code }).withColumn('last_timestamp',F.first('call_datetime').over(user_day))\
        .withColumn('last_region', F.when(F.col('call_datetime') == F.col('last_timestamp'), 1).otherwise(0))\
        .orderBy('call_datetime')\
        .groupby('msisdn', frequency, 'region')\
        .agg(F.sum('last_region').alias('last_region_count'))\
        .withColumn('modal_region', F.when(F.first('last_region_count').over(user_frequency) == F.col('last_region_count'),1).otherwise(0))\
        .where(F.col('modal_region') == 1)\
        .groupby('msisdn', frequency)\
        .agg(F.last('region').alias('home_region'))

    return result



#### Indicator 6 + 11

# result:
# - find home locations using helper method
# - group by frequency and home region
# - count observations
# - apply privacy_filter

def unique_subscriber_home_locations(df, time_filter, frequency):

    result = assign_home_locations(df, time_filter, frequency)\
        .groupby(frequency, 'home_region')\
        .count()\
        .where(F.col('count') > privacy_filter)

    return result

# methode de sauvegarde
def save(df, outfolder):
    df.write.mode('overwrite').parquet(outfolder)


#creation de la SparkSession
spark = SparkSession \
    .builder \
    .appName("mm") \
    .config("spark.driver.memory", "32g") \
    .config("spark.executor.cores", "16") \
    .config("spark.executor.instances", "16") \
    .config("spark.executor.memory", "16g") \
    .config("spark.driver.maxResultSize", "8g")\
    .getOrCreate()
spark.sparkContext.setLogLevel("ERROR")




#chemin d'accès au donnees
path_data = "/nfs_sunshine/cider/data/data_oci/cdr"
path_antennas = "/nfs_sunshine/cider/data/data_oci/antennas"


#chargement des donnees cdr
calls = spark.read.csv(path_data + '/202105/*.csv', header = True)

#traitements out
cols_out = ['caller_id', 'timestamp', 'caller_antenna']
outgoing = calls.select(cols_out).dropna(subset = 'caller_antenna')\
    .withColumnRenamed('caller_id', 'msisdn')\
    .withColumnRenamed('timestamp', 'call_datetime')\
    .withColumnRenamed('caller_antenna', 'location_id')
outgoing = outgoing.withColumn('call_datetime', to_timestamp(outgoing['call_datetime'], 'yyyy-MM-dd HH:mm:ss'))
outgoing = outgoing.withColumn('call_date', outgoing.call_datetime.cast('date'))

#traitement in
cols_in = ['recipient_id', 'timestamp', 'recipient_antenna']
incoming = calls.select(cols_in).dropna(subset ='recipient_antenna')\
    .withColumnRenamed('recipient_id', 'msisdn')\
    .withColumnRenamed('timestamp', 'call_datetime')\
    .withColumnRenamed('recipient_antenna', 'location_id')
incoming = incoming.withColumn('call_datetime', to_timestamp(incoming['call_datetime'], 'yyyy-MM-dd HH:mm:ss'))
incoming = incoming.withColumn('call_date', incoming.call_datetime.cast('date'))

#fin traitement donnees cdr
calls = outgoing.select(incoming.columns).union(incoming)

#on filtre les numeros cryptés
calls = calls.withColumn('len_msisdn', F.length(F.col('msisdn'))).filter('len_msisdn >= 20').drop('len_msisdn')

#chargement du maping cell_id et admin
cells = spark.read.csv(path_antennas + '/antennas_map_admin.csv',sep = ',', header = True)

cells_cols = ['Cell_ID_LAC', 'UA']
cells = cells.select(cells_cols)\
    .dropna(subset ='Cell_ID_LAC')\
    .withColumnRenamed('Cell_ID_LAC', 'cell_id')\
    .withColumnRenamed('UA', 'region')

#filtre sur la periode de date
dates = {'start_date' : dt.datetime(2021,5,1),
        'end_date' : dt.datetime(2021,5,31)}


#add_weeks_date
idx = dates['start_date'].weekday() % 7
idx2 = dates['end_date'].weekday() + 1 % 7
if idx == 0:
    idx = 7
if idx2 == 7:
    idx2 = 0
dates['start_date_weeks'] = dates['start_date'] + dt.timedelta(7-idx)
dates['end_date_weeks'] = dates['end_date'] - dt.timedelta(idx2)


#
period_filter = (F.col('call_datetime') >= dates['start_date']) & (F.col('call_datetime') <= dates['end_date'] + dt.timedelta(1))

# we only include full weeks, these have been inherited
weeks_filter = (F.col('call_datetime') >= dates['start_date_weeks']) & (F.col('call_datetime') <= dates['end_date_weeks'] + dt.timedelta(1))

privacy_filter = 15
missing_value_code = 99999
cutoff_days = 7
max_duration = 21



############# Windows for window functions

# window by cardnumber
user_window = Window.partitionBy('msisdn').orderBy('call_datetime')

# window by cardnumber starting with last transaction
user_window_rev = Window.partitionBy('msisdn').orderBy(F.desc('call_datetime'))

# user date window
user_date_window = Window.partitionBy('msisdn', 'call_date').orderBy('call_datetime')

# user date window starting from last date
user_date_window_rev = Window.partitionBy('msisdn', 'call_date').orderBy(F.desc('call_datetime'))


dates_sql = {'start_date' : "\'" + dates['start_date'].isoformat('-')[:10] +  "\'",
        'end_date' :  "\'" + dates['end_date'].isoformat('-')[:10] +  "\'",
        'start_date_weeks' :  "\'" + dates['start_date_weeks'].isoformat('-')[:10] +  "\'",
        'end_date_weeks' : "\'" + dates['end_date_weeks'].isoformat('-')[:10] +  "\'"}



calls.createOrReplaceTempView('calls')
cells.createOrReplaceTempView('cells')


sql_code = write_sql_code(calls = calls,
                        start_date = dates_sql['start_date'],
                        end_date = dates_sql['end_date'],
                        start_date_weeks = dates_sql['start_date_weeks'],
                        end_date_weeks = dates_sql['end_date_weeks'])



df = calls.join(cells, calls.location_id == cells.cell_id, how = 'left')\
        .drop('cell_id')\
        .join(spark.sql(sql_code['home_locations'])\
        .withColumnRenamed('region', 'home_region'), 'msisdn', 'left')\
        .orderBy('msisdn', 'call_datetime').withColumn('region_lag', F.lag('region').over(user_window))\
        .withColumn('region_lead', F.lead('region').over(user_window))\
        .withColumn('call_datetime_lag',F.lag('call_datetime').over(user_window))\
        .withColumn('call_datetime_lead',F.lead('call_datetime').over(user_window))\
        .withColumn('hour_of_day', F.hour('call_datetime').cast('byte'))\
        .withColumn('hour', F.date_trunc('hour', F.col('call_datetime')))\
        .withColumn('week', F.date_trunc('week', F.col('call_datetime')))\
        .withColumn('month', F.date_trunc('month', F.col('call_datetime')))\
        .withColumn('constant', F.lit(1).cast('byte'))\
        .withColumn('day', F.date_trunc('day', F.col('call_datetime')))\
        .na.fill({'region' : missing_value_code , 'region_lag' : missing_value_code , 'region_lead' : missing_value_code })


# Calcul des indicateurs et sauvegarde

save(transactions(df, period_filter, frequency = 'hour'), '/nfs_sunshine/cider/data/output_ind_mobility_202105/ind_1_commune_per_hour')

save(unique_subscribers_country(df, period_filter, frequency = 'day'), '/nfs_sunshine/cider/data/output_ind_mobility_202105/ind_3_commune_per_day')

save(origin_destination_connection_matrix(df, period_filter, frequency = 'day'), '/nfs_sunshine/cider/data/output_ind_mobility_202105/ind_5_commune_per_day')

save(unique_subscriber_home_locations(df, period_filter, frequency = 'month'), '/nfs_sunshine/cider/data/output_ind_mobility_202105/ind_11_commune_per_month')

save(assign_home_locations(df, period_filter, frequency = 'day'), '/nfs_sunshine/cider/data/output_ind_mobility_202105/ind_6_commune_per_day')

save(unique_subscribers(df, period_filter, frequency = 'hour'), '/nfs_sunshine/cider/data/output_ind_mobility_202105/ind_2_commune_per_hour')

start = time.time()
print('start time : ', start)

