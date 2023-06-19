from ast import Dict
from pyspark.sql import DataFrame as SparkDataFrame
import pyspark.sql.functions as F
from pyspark.sql.functions import col, lit, date_trunc, to_timestamp
from pyspark.sql.window import Window
from typing import List
from box import Box
from pyspark.sql import SparkSession
import os
import shutil
import pandas as pd

def add_all_cat(df, cols):
    """
    Duplicate dataframe rows so that groupby result includes an "all interactions" category for specified column(s)

    Args:
        df: spark dataframe
        cols: string defining columns to duplicate

    Returns:
        df: spark dataframe
    """
    # Define mapping from column name to column value
    # e.g. the column daytime will also have a value called "allday" to denote both day and night
    if cols == 'week':
        col_mapping = {'weekday': 'allweek'}
    elif cols == 'week_day':
        col_mapping = {'weekday': 'allweek', 'daytime': 'allday'}
    elif cols == 'week_day_dir':
        col_mapping = {'weekday': 'allweek', 'daytime': 'allday', 'direction': 'alldir'}
    else:
        raise ValueError("'cols' argument should be one of {week, week_day, week_day_dir}")

    # For each of the columns defined in the mapping, duplicate entries
    for column, value in col_mapping.items():
        df = (df
              .withColumn(column, F.array(lit(value), col(column)))
              .withColumn(column, F.explode(column)))

    return df


def pivot_df(df, index, columns, values, indicator_name):
    """
    Recreate pandas pivot method for dataframes

    Args:
        df: spark dataframe
        index: columns to use to make new frames index
        columns: columns to use to make new frames columns
        values: column(s) to use for populating new frames values
        indicator_name: name of indicator to prefix to new columns

    Returns:
        df: pivoted spark dataframe
    """
    # Iterate through columns
    while columns:
        column = columns.pop()
        # Pivot values based on current column selection
        df = (df
              .groupby(index + columns)
              .pivot(column)
              .agg(*[F.first(val).alias(val) for val in values]))
        #  Update values for next pivot
        values = [val for val in df.columns if val not in index and val not in columns]

    # Rename columns by prefixing indicator name
    col_selection = [col(col_name).alias(indicator_name + '_' + col_name) for col_name in df.columns
                     if col_name != 'caller_id']
    df = df.select('caller_id', *col_selection)

    return df


def tag_conversations(df, max_wait = 3600):
    """
    From bandicoot's documentation: "We define conversations as a series of text messages between the user and one
    contact. A conversation starts with either of the parties sending a text to the other. A conversation will stop if
    no text was exchanged by the parties for an hour or if one of the parties call the other. The next conversation will
    start as soon as a new text is send by either of the parties."
    This functions tags interactions with the conversation id they are part of: the id is the start unix time of the
    conversation.

    Args:
        df: spark dataframe
        max_wait: time (in seconds) after which a conversation ends if no texts or calls have been exchanged

    Returns:
        df: tagged spark dataframe
    """
    w = Window.partitionBy('caller_id', 'recipient_id').orderBy('timestamp')

    df = (df
          .withColumn('ts', col('timestamp').cast('long'))
          .withColumn('prev_txn', F.lag(col('txn_type')).over(w))
          .withColumn('prev_ts', F.lag(col('ts')).over(w))
          .withColumn('wait', col('ts') - col('prev_ts'))
          .withColumn('conversation', F.when((col('txn_type') == 'text') &
                                             ((col('prev_txn') == 'call') |
                                              (col('prev_txn').isNull()) |
                                              (col('wait') >= max_wait)), col('ts')))
          .withColumn('convo', F.last('conversation', ignorenulls=True).over(w))
          .withColumn('conversation', F.when(col('conversation').isNotNull(), col('conversation'))
                                       .otherwise(F.when(col('txn_type') == 'text', col('convo'))))
          .drop('ts', 'prev_txn', 'prev_ts', 'convo'))

    return df


def great_circle_distance(df):
    """
    Return the great-circle distance in kilometers between two points, in this case always the antenna handling an
    interaction and the barycenter of all the user's interactions.
    Used to compute the radius of gyration.
    """
    r = 6371.  # Earth's radius

    df = (df
          .withColumn('delta_latitude', F.radians(col('latitude') - col('bar_lat')))
          .withColumn('delta_longitude', F.radians(col('longitude') - col('bar_lon')))
          .withColumn('latitude1', F.radians(col('latitude')))
          .withColumn('latitude2', F.radians(col('bar_lat')))
          .withColumn('a', F.sin(col('delta_latitude')/2)**2 +
                      F.cos('latitude1')*F.cos('latitude2')*(F.sin(col('delta_longitude')/2)**2))
          .withColumn('r', 2*lit(r)*F.asin(F.sqrt('a'))))

    return df


def summary_stats(col_name):
    # Standard list of functions to be applied to column after group by
    functions = [
        F.mean(col_name).alias('mean'),
        F.min(col_name).alias('min'),
        F.max(col_name).alias('max'),
        F.stddev_pop(col_name).alias('std'),
        F.expr(f'percentile_approx({col_name}, 0.5)').alias('median'),
        F.skewness(col_name).alias('skewness'),
        F.kurtosis(col_name).alias('kurtosis')
    ]

    return functions

################ add features #################

def all_spark(df, antennas, cfg):
    """
    Compute cdr features starting from raw interaction data

    Args:
        df: spark dataframe with cdr interactions
        antennas: spark dataframe with antenna ids and coordinates
        cfg: config object

    Returns:
        features: list of features as spark dataframes
    """
 #   features = []

    df = (df
          # Add weekday and daytime columns for subsequent groupby(s)
          .withColumn('weekday', F.when(F.dayofweek('day').isin(cfg["weekend"]), 'weekend').otherwise('weekday'))
          .withColumn('daytime', F.when((F.hour('timestamp') < cfg["start_of_day"]) |
                                        (F.hour('timestamp') >= cfg["end_of_day"]), 'night').otherwise('day'))
          # Duplicate rows, switching caller and recipient columns
          .withColumn('direction', lit('out'))
          .withColumn('directions', F.array(lit('in'), col('direction')))
          .withColumn('direction', F.explode('directions'))
          .withColumn('caller_id_copy', col('caller_id'))
          .withColumn('caller_antenna_copy', col('caller_antenna'))
          .withColumn('caller_id', F.when(col('direction') == 'in', col('recipient_id')).otherwise(col('caller_id')))
          .withColumn('recipient_id',
                      F.when(col('direction') == 'in', col('caller_id_copy')).otherwise(col('recipient_id')))
          .withColumn('caller_antenna',
                      F.when(col('direction') == 'in', col('recipient_antenna')).otherwise(col('caller_antenna')))
          .withColumn('recipient_antenna',
                      F.when(col('direction') == 'in', col('caller_antenna_copy')).otherwise(col('recipient_antenna')))
          .drop('directions', 'caller_id_copy', 'recipient_antenna_copy'))

    # Assign interactions to conversations if relevant
    df = tag_conversations(df)

    # Compute features and saved parquet file
    save_df(active_days(df), '/nfs_sunshine/cider/data/output_cider/Ind_cider_202105/active_days')
    save_df(number_of_contacts(df), '/nfs_sunshine/cider/data/output_cider/Ind_cider_202105/number_of_contacts')
    save_df(call_duration(df), '/nfs_sunshine/cider/data/output_cider/Ind_cider_202105/call_duration')
    save_df(percent_nocturnal(df), '/nfs_sunshine/cider/data/output_cider/Ind_cider_202105/percent_nocturnal')
    save_df(percent_initiated_conversations(df), '/nfs_sunshine/cider/data/output_cider/Ind_cider_202105/percent_initiated_conversations')
    save_df(percent_initiated_interactions(df), '/nfs_sunshine/cider/data/output_cider/Ind_cider_202105/percent_initiated_interactions')
    save_df(response_delay_text(df), '/nfs_sunshine/cider/data/output_cider/Ind_cider_202105/response_delay_text')
    save_df(response_rate_text(df), '/nfs_sunshine/cider/data/output_cider/Ind_cider_202105/response_rate_text')
    save_df(entropy_of_contacts(df), '/nfs_sunshine/cider/data/output_cider/Ind_cider_202105/entropy_of_contacts')
    save_df((balance_of_contacts(df)), '/nfs_sunshine/cider/data/output_cider/Ind_cider_202105/balance_of_contacts')
    save_df(interactions_per_contact(df), '/nfs_sunshine/cider/data/output_cider/Ind_cider_202105/interactions_per_contact')
    save_df(interevent_time(df), '/nfs_sunshine/cider/data/output_cider/Ind_cider_202105/interevent_time')
    save_df(percent_pareto_interactions(df), '/nfs_sunshine/cider/data/output_cider/Ind_cider_202105/percent_pareto_interactions')
    save_df((percent_pareto_durations(df)), '/nfs_sunshine/cider/data/output_cider/Ind_cider_202105/percent_pareto_durations')
    save_df(number_of_interactions(df), '/nfs_sunshine/cider/data/output_cider/Ind_cider_202105/number_of_interactions')
    save_df(number_of_antennas(df), '/nfs_sunshine/cider/data/output_cider/Ind_cider_202105/number_of_antennas')
    save_df(entropy_of_antennas(df), '/nfs_sunshine/cider/data/output_cider/Ind_cider_202105/entropy_of_antennas')
    save_df(frequent_antennas(df), '/nfs_sunshine/cider/data/output_cider/Ind_cider_202105/frequent_antennas')
    save_df(percent_at_home(df), '/nfs_sunshine/cider/data/output_cider/Ind_cider_202105/percent_at_home')
    save_df(radius_of_gyration(df, antennas), '/nfs_sunshine/cider/data/output_cider/Ind_cider_202105/radius_of_gyration')


def active_days(df):
    """
    Returns the number of active days per user, disaggregated by type and time of day
    """
    df = add_all_cat(df, cols='week_day')

    out = (df
           .groupby('caller_id', 'weekday', 'daytime')
           .agg(F.countDistinct('day').alias('active_days')))

    out = pivot_df(out, index=['caller_id'], columns=['weekday', 'daytime'], values=['active_days'],
                   indicator_name='active_days')

    return out


def number_of_contacts(df):
    """
    Returns the number of distinct contacts per user, disaggregated by type and time of day, and transaction type
    """
    df = add_all_cat(df, cols='week_day')

    out = (df
           .groupby('caller_id', 'weekday', 'daytime', 'txn_type')
           .agg(F.countDistinct('recipient_id').alias('number_of_contacts')))

    out = pivot_df(out, index=['caller_id'], columns=['weekday', 'daytime', 'txn_type'], values=['number_of_contacts'],
                   indicator_name='number_of_contacts')

    return out


def call_duration(df):
    """
    Returns summary stats of users' call durations, disaggregated by type and time of day
    """
    df = df.where(col('txn_type') == 'call')
    df = add_all_cat(df, cols='week_day')

    out = (df
           .groupby('caller_id', 'weekday', 'daytime', 'txn_type')
           .agg(*summary_stats('duration')))

    out = pivot_df(out, index=['caller_id'], columns=['weekday', 'daytime', 'txn_type'],
                   values=['mean', 'std', 'median', 'skewness', 'kurtosis', 'min', 'max'],
                   indicator_name='call_duration')

    return out


def percent_nocturnal(df):
    """
    Returns the percentage of interactions done at night, per user, disaggregated by type of day and transaction type
    """
    df = add_all_cat(df, cols='week')

    out = (df
           .withColumn('nocturnal', F.when(col('daytime') == 'night', 1).otherwise(0))
           .groupby('caller_id', 'weekday', 'txn_type')
           .agg(F.mean('nocturnal').alias('percent_nocturnal')))

    out = pivot_df(out, index=['caller_id'], columns=['weekday', 'txn_type'], values=['percent_nocturnal'],
                   indicator_name='percent_nocturnal')

    return out


def percent_initiated_conversations(df):
    """
    Returns the percentage of conversations initiated by the user, disaggregated by type and time of day
    """
    df = add_all_cat(df, cols='week_day')

    out = (df
           .where(col('conversation') == col('timestamp').cast('long'))
           .withColumn('initiated', F.when(col('direction') == 'out', 1).otherwise(0))
           .groupby('caller_id', 'weekday', 'daytime')
           .agg(F.mean('initiated').alias('percent_initiated_conversations')))

    out = pivot_df(out, index=['caller_id'], columns=['weekday', 'daytime'], values=['percent_initiated_conversations'],
                   indicator_name='percent_initiated_conversations')

    return out


def percent_initiated_interactions(df):
    """
    Returns the percentage of interactions initiated by the user, disaggregated by type and time of day
    """
    df = df.where(col('txn_type') == 'call')
    df = add_all_cat(df, cols='week_day')

    out = (df
           .withColumn('initiated', F.when(col('direction') == 'out', 1).otherwise(0))
           .groupby('caller_id', 'weekday', 'daytime')
           .agg(F.mean('initiated').alias('percent_initiated_interactions')))

    out = pivot_df(out, index=['caller_id'], columns=['weekday', 'daytime'], values=['percent_initiated_interactions'],
                   indicator_name='percent_initiated_interactions')

    return out


def response_delay_text(df):
    """
    Returns summary stats of users' delays in responding to texts, disaggregated by type and time of day
    """
    df = df.where(col('txn_type') == 'text')
    df = add_all_cat(df, cols='week_day')

    w = Window.partitionBy('caller_id', 'recipient_id', 'conversation').orderBy('timestamp')
    out = (df
           .withColumn('prev_dir', F.lag(col('direction')).over(w))
           .withColumn('response_delay', F.when((col('direction') == 'out') & (col('prev_dir') == 'in'), col('wait')))
           .groupby('caller_id', 'weekday', 'daytime')
           .agg(*summary_stats('response_delay')))

    out = pivot_df(out, index=['caller_id'], columns=['weekday', 'daytime'],
                   values=['mean', 'std', 'median', 'skewness', 'kurtosis', 'min', 'max'],
                   indicator_name='response_delay_text')

    return out


def response_rate_text(df):
    """
    Returns the percentage of texts to which the users responded, disaggregated by type and time of day
    """
    df = df.where(col('txn_type') == 'text')
    df = add_all_cat(df, cols='week_day')

    w = Window.partitionBy('caller_id', 'recipient_id', 'conversation')
    out = (df
           .withColumn('dir', F.when(col('direction') == 'out', 1).otherwise(0))
           .withColumn('responded', F.max(col('dir')).over(w))
           .where((col('conversation') == col('timestamp').cast('long')) & (col('direction') == 'in'))
           .groupby('caller_id', 'weekday', 'daytime')
           .agg(F.mean('responded').alias('response_rate_text')))

    out = pivot_df(out, index=['caller_id'], columns=['weekday', 'daytime'], values=['response_rate_text'],
                   indicator_name='response_rate_text')

    return out


def entropy_of_contacts(df):
    """
    Returns the entropy of interactions the users had with their contacts, disaggregated by type and time of day, and
    transaction type
    """
    df = add_all_cat(df, cols='week_day')

    w = Window.partitionBy('caller_id', 'weekday', 'daytime', 'txn_type')
    out = (df
           .groupby('caller_id', 'recipient_id', 'weekday', 'daytime', 'txn_type')
           .agg(F.count(lit(0)).alias('n'))
           .withColumn('n_total', F.sum('n').over(w))
           .withColumn('n', (col('n')/col('n_total').cast('float')))
           .groupby('caller_id', 'weekday', 'daytime', 'txn_type')
           .agg((-1*F.sum(col('n')*F.log(col('n')))).alias('entropy')))

    out = pivot_df(out, index=['caller_id'], columns=['weekday', 'daytime', 'txn_type'], values=['entropy'],
                   indicator_name='entropy_of_contacts')

    return out


def balance_of_contacts(df):
    """
    Returns summary stats for the balance of interactions (out/(in+out)) the users had with their contacts,
    disaggregated by type and time of day, and transaction type
    """
    df = add_all_cat(df, cols='week_day')

    out = (df
           .groupby('caller_id', 'recipient_id', 'direction', 'weekday', 'daytime', 'txn_type')
           .agg(F.count(lit(0)).alias('n'))
           .groupby('caller_id', 'recipient_id', 'weekday', 'daytime', 'txn_type')
           .pivot('direction')
           .agg(F.first('n').alias('n'))
           .fillna(0)
           .withColumn('n_total', col('in')+col('out'))
           .withColumn('n', (col('out')/col('n_total')))
           .groupby('caller_id', 'weekday', 'daytime', 'txn_type')
           .agg(*summary_stats('n')))

    out = pivot_df(out, index=['caller_id'], columns=['weekday', 'daytime', 'txn_type'],
                   values=['mean', 'std', 'median', 'skewness', 'kurtosis', 'min', 'max'],
                   indicator_name='balance_of_contacts')

    return out


def interactions_per_contact(df):
    """
    Returns summary stats for the number of interactions the users had with their contacts, disaggregated by type and
    time of day, and transaction type
    """
    df = add_all_cat(df, cols='week_day')

    out = (df
           .groupby('caller_id', 'recipient_id', 'weekday', 'daytime', 'txn_type')
           .agg(F.count(lit(0)).alias('n'))
           .groupby('caller_id', 'weekday', 'daytime', 'txn_type')
           .agg(*summary_stats('n')))

    out = pivot_df(out, index=['caller_id'], columns=['weekday', 'daytime', 'txn_type'],
                   values=['mean', 'std', 'median', 'skewness', 'kurtosis', 'min', 'max'],
                   indicator_name='interactions_per_contact')

    return out


def interevent_time(df):
    """
    Returns summary stats for the time between users' interactions, disaggregated by type and time of day, and
    transaction type
    """
    df = add_all_cat(df, cols='week_day')

    w = Window.partitionBy('caller_id', 'weekday', 'daytime', 'txn_type').orderBy('timestamp')
    out = (df
           .withColumn('ts', col('timestamp').cast('long'))
           .withColumn('prev_ts', F.lag(col('ts')).over(w))
           .withColumn('wait', col('ts') - col('prev_ts'))
           .groupby('caller_id', 'weekday', 'daytime', 'txn_type')
           .agg(*summary_stats('wait')))

    out = pivot_df(out, index=['caller_id'], columns=['weekday', 'daytime', 'txn_type'],
                   values=['mean', 'std', 'median', 'skewness', 'kurtosis', 'min', 'max'],
                   indicator_name='interevent_time')

    return out


def percent_pareto_interactions(df, percentage = 0.8):
    """
    Returns the percentage of a user's contacts that account for 80% of their interactions, disaggregated by type and
    time of day, and transaction type
    """
    df = add_all_cat(df, cols='week_day')

    w = Window.partitionBy('caller_id', 'weekday', 'daytime', 'txn_type')
    w1 = Window.partitionBy('caller_id', 'weekday', 'daytime', 'txn_type').orderBy(col('n').desc())
    w2 = Window.partitionBy('caller_id', 'weekday', 'daytime', 'txn_type').orderBy('row_number')
    out = (df
           .groupby('caller_id', 'recipient_id', 'weekday', 'daytime', 'txn_type')
           .agg(F.count(lit(0)).alias('n'))
           .withColumn('row_number', F.row_number().over(w1))
           .withColumn('total', F.sum('n').over(w))
           .withColumn('cumsum', F.sum('n').over(w2))
           .withColumn('fraction', col('cumsum')/col('total'))
           .withColumn('row_number', F.when(col('fraction') >= percentage, col('row_number')))
           .groupby('caller_id', 'weekday', 'daytime', 'txn_type')
           .agg(F.min('row_number').alias('pareto_users'),
                F.countDistinct('recipient_id').alias('n_users'))
           .withColumn('pareto', col('pareto_users')/col('n_users')))

    out = pivot_df(out, index=['caller_id'], columns=['weekday', 'daytime', 'txn_type'], values=['pareto'],
                   indicator_name='percent_pareto_interactions')

    return out


def percent_pareto_durations(df, percentage = 0.8):
    """
    Returns the percentage of a user's contacts that account for 80% of their call durations, disaggregated by type and
    time of day, and transaction type
    """
    df = df.where(col('txn_type') == 'call')
    df = add_all_cat(df, cols='week_day')

    w = Window.partitionBy('caller_id', 'weekday', 'daytime')
    w1 = Window.partitionBy('caller_id', 'weekday', 'daytime').orderBy(col('duration').desc())
    w2 = Window.partitionBy('caller_id', 'weekday', 'daytime').orderBy('row_number')
    out = (df
           .groupby('caller_id', 'recipient_id', 'weekday', 'daytime')
           .agg(F.sum('duration').alias('duration'))
           .withColumn('row_number', F.row_number().over(w1))
           .withColumn('total', F.sum('duration').over(w))
           .withColumn('cumsum', F.sum('duration').over(w2))
           .withColumn('fraction', col('cumsum')/col('total'))
           .withColumn('row_number', F.when(col('fraction') >= percentage, col('row_number')))
           .groupby('caller_id', 'weekday', 'daytime')
           .agg(F.min('row_number').alias('pareto_users'),
                F.countDistinct('recipient_id').alias('n_users'))
           .withColumn('pareto', col('pareto_users')/col('n_users')))

    out = pivot_df(out, index=['caller_id'], columns=['weekday', 'daytime'], values=['pareto'],
                   indicator_name='percent_pareto_durations')

    return out


def number_of_interactions(df):
    """
    Returns the number of interactions per user, disaggregated by type and time of day, transaction type, and direction
    """
    df = add_all_cat(df, cols='week_day_dir')

    out = (df
           .groupby('caller_id', 'weekday', 'daytime', 'txn_type', 'direction')
           .agg(F.count(lit(0)).alias('n')))

    out = pivot_df(out, index=['caller_id'], columns=['direction', 'weekday', 'daytime', 'txn_type'], values=['n'],
                   indicator_name='number_of_interactions')

    return out


def number_of_antennas(df):
    """
    Returns the number of antennas the handled users' interactions, disaggregated by type and time of day
    """
    df = add_all_cat(df, cols='week_day')

    out = (df
           .groupby('caller_id', 'weekday', 'daytime')
           .agg(F.countDistinct('caller_antenna').alias('n_antennas')))

    out = pivot_df(out, index=['caller_id'], columns=['weekday', 'daytime'], values=['n_antennas'],
                   indicator_name='number_of_antennas')

    return out


def entropy_of_antennas(df):
    """
    Returns the entropy of a user's antennas' shares of handled interactions, disaggregated by type and time of day
    """
    df = add_all_cat(df, cols='week_day')

    w = Window.partitionBy('caller_id', 'weekday', 'daytime')
    out = (df
           .groupby('caller_id', 'caller_antenna', 'weekday', 'daytime')
           .agg(F.count(lit(0)).alias('n'))
           .withColumn('n_total', F.sum('n').over(w))
           .withColumn('n', (col('n')/col('n_total').cast('float')))
           .groupby('caller_id', 'weekday', 'daytime')
           .agg((-1*F.sum(col('n')*F.log(col('n')))).alias('entropy')))

    out = pivot_df(out, index=['caller_id'], columns=['weekday', 'daytime'], values=['entropy'],
                   indicator_name='entropy_of_antennas')

    return out


def percent_at_home(df):
    """
    Returns the percentage of interactions handled by a user's home antenna, disaggregated by type and time of day
    """
    df = add_all_cat(df, cols='week_day')

    df = df.dropna(subset=['caller_antenna'])

    # Compute home antennas for all users, if possible
    w = Window.partitionBy('caller_id').orderBy(col('n').desc())
    home_antenna = (df
                    .where(col('daytime') == 'night')
                    .groupby('caller_id', 'caller_antenna')
                    .agg(F.count(lit(0)).alias('n'))
                    .withColumn('row_number', F.row_number().over(w))
                    .where(col('row_number') == 1)
                    .withColumnRenamed('caller_antenna', 'home_antenna')
                    .drop('n'))

    out = (df
           .join(home_antenna, on='caller_id', how='inner')
           .withColumn('home_interaction', F.when(col('caller_antenna') == col('home_antenna'), 1).otherwise(0))
           .groupby('caller_id', 'weekday', 'daytime')
           .agg(F.mean('home_interaction').alias('mean')))

    out = pivot_df(out, index=['caller_id'], columns=['weekday', 'daytime'], values=['mean'],
                   indicator_name='percent_at_home')

    return out


def radius_of_gyration(df, antennas):
    """
    Returns the radius of gyration of users, disaggregated by type and time of day

    References
    ----------
    .. [GON2008] Gonzalez, M. C., Hidalgo, C. A., & Barabasi, A. L. (2008).
        Understanding individual human mobility patterns. Nature, 453(7196),
        779-782.
    """
    df = add_all_cat(df, cols='week_day')

    df = (df
          .join(antennas, on=df.caller_antenna == antennas.antenna_id, how='inner')
          .dropna(subset=['latitude', 'longitude']))

    bar = (df
           .groupby('caller_id', 'weekday', 'daytime')
           .agg(F.sum('latitude').alias('latitude'),
                F.sum('longitude').alias('longitude'),
                F.count(lit(0)).alias('n'))
           .withColumn('bar_lat', col('latitude')/col('n'))
           .withColumn('bar_lon', col('longitude') / col('n'))
           .drop('latitude', 'longitude'))

    df = df.join(bar, on=['caller_id', 'weekday', 'daytime'])
    df = great_circle_distance(df)
    out = (df
           .groupby('caller_id', 'weekday', 'daytime')
           .agg(F.sqrt(F.sum(col('r')**2/col('n'))).alias('r')))

    out = pivot_df(out, index=['caller_id'], columns=['weekday', 'daytime'], values=['r'],
                   indicator_name='radius_of_gyration')

    return out


def frequent_antennas(df, percentage = 0.8):
    """
    Returns the percentage of antennas accounting for 80% of users' interactions, disaggregated by type and time of day
    """
    df = add_all_cat(df, cols='week_day')

    w = Window.partitionBy('caller_id', 'weekday', 'daytime')
    w1 = Window.partitionBy('caller_id', 'weekday', 'daytime').orderBy(col('n').desc())
    w2 = Window.partitionBy('caller_id', 'weekday', 'daytime').orderBy('row_number')
    out = (df
           .groupby('caller_id', 'caller_antenna', 'weekday', 'daytime')
           .agg(F.count(lit(0)).alias('n'))
           .withColumn('row_number', F.row_number().over(w1))
           .withColumn('total', F.sum('n').over(w))
           .withColumn('cumsum', F.sum('n').over(w2))
           .withColumn('fraction', col('cumsum')/col('total'))
           .withColumn('row_number', F.when(col('fraction') >= percentage, col('row_number')))
           .groupby('caller_id', 'weekday', 'daytime')
           .agg(F.min('row_number').alias('pareto_antennas')))

    out = pivot_df(out, index=['caller_id'], columns=['weekday', 'daytime'], values=['pareto_antennas'],
                   indicator_name='frequent_antennas')

    return out

######## add utils functions #######
def long_join_pyspark(dfs, on, how):
    """
    Join list of spark dfs

    Args:
        dfs: list of spark df
        on: column on which to join
        how: type of join

    Returns: single joined spark df
    """
    df = dfs[0]
    for i in range(1, len(dfs)):
        df = df.join(dfs[i], on=on, how=how)
    return df

def filter_dates_dataframe(df, start_date, end_date, colname = 'timestamp'):
    """
    Filter dataframe rows whose timestamp is outside [start_date, end_date)

    Args:
        df: spark df
        start_date: initial date to keep
        end_date: first date to exclude
        colname: name of timestamp column

    Returns: filtered spark df

    """
    if colname not in df.columns:
        raise ValueError('Cannot filter dates because missing timestamp column')
    df = df.where(col(colname) >= pd.to_datetime(start_date))
    df = df.where(col(colname) < pd.to_datetime(end_date) + pd.Timedelta(value=1, unit='days'))
    return df

#save results to parquet files
def save_df(df, outfname):
    """
    Saves spark dataframe to parquet file, using work-around to deal with spark's automatic partitioning and naming
    """
    df.write.mode("overwrite").parquet(outfname)


###### Start code ####################

# Build spark session
spark = SparkSession \
    .builder \
    .appName("mm") \
    .config("spark.sql.files.maxPartitionBytes", 67108864) \
    .config("spark.driver.memory", "16g") \
    .config("spark.executor.cores", "10") \
    .config("spark.executor.instances", "6") \
    .config("spark.executor.memory", "10g") \
    .config("spark.driver.maxResultSize", "8g")\
    .getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

#path data
path_data = "/nfs_sunshine/cider/data/data_oci"

#load cdr files
df_cdr = spark.read.csv(path_data + '/cdr/202105/*.csv', header=True)

# Clean timestamp column
df_cdr = df_cdr.withColumn('timestamp', to_timestamp(df_cdr['timestamp'], 'yyyy-MM-dd HH:mm:ss')) \
    .withColumn('day', date_trunc('day', col('timestamp')))

# Filter dataframe
df_cdr = filter_dates_dataframe(df_cdr, start_date = '2021-05-01', end_date = '2021-06-01', colname = 'timestamp')

# Clean duration column
df_cdr = df_cdr.withColumn('duration', col('duration').cast('float'))

# load antennas
df_antennas = spark.read.csv(path_data + '/antennas/antennas.csv', header=True)


#calcul cdr features
param_cdr = {"weekend": [1, 7], "start_of_day": 7, "end_of_day": 19}

#compute and save in parquet file 
all_spark(df_cdr, df_antennas, cfg=param_cdr)
