import os
import dagster as dg
from dagster import InputContext, OpExecutionContext, Out, RetryPolicy, TimeWindow, WeeklyPartitionsDefinition, asset, io_manager, op, repository, with_resources, DailyPartitionsDefinition
from psaw import PushshiftAPI
import pandas as pd

import datetime as dt

from urllib3 import Retry

from reddit_datasource.assets import partition


@op(
    out=Out(io_manager_key='pandas_io_manager'),
    required_resource_keys={'pushshift_limiter'},
    retry_policy=dg.RetryPolicy(
        10,
        delay=1, 
        backoff=dg.Backoff.EXPONENTIAL,
        jitter=dg.Jitter.PLUS_MINUS
    )
)
def subreddit(context: OpExecutionContext, subreddit: str):
    
    # partition ~ `2022-08-10-00:00`
    start_time, end_time = context.partition_time_window
    
    context.log.info(
        f"Processing asset partition '{partition}'"
    )

    with context.resources.pushshift_limiter.ratelimit('pushshift', delay=True):
        api = PushshiftAPI()
        posts_generator = api.search_submissions(
            after=int(start_time.timestamp()),
            before=int(end_time.timestamp()),
            subreddit=subreddit,
            filter=['id', 'created_utc', 'url','author', 'title', 'subreddit'],
            # limit=
        )
    posts_dicts = list(map(lambda x: x.d_, posts_generator))
    posts = pd.DataFrame(posts_dicts)

    posts = posts.set_index('id')

    context.log.info(api.metadata_)


    return posts

@op(
    out=dg.DynamicOut()
)
def get_subs():

    with open('subreddits', 'r') as f:
        subs = f.readlines()[:1000]
    
    subreddits = map(lambda x: x.strip(), subs)

    yield from map(lambda x: dg.DynamicOutput(x, mapping_key=x), subreddits)

@op(
    ins={'x': dg.In(input_manager_key='noop_io_manager')}
)
def dummy_collect(x):
    return

@dg.graph
def reddit():
    subs = get_subs()
    subreddit_data = subs.map(subreddit)
    return dummy_collect(subreddit_data.collect())

reddit_asset = dg.AssetsDefinition.from_graph(
    reddit,
    partitions_def=partition
)