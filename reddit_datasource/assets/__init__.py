import dagster as dg
import datetime as dt
# partition = dg.MonthlyPartitionsDefinition(dt.datetime(2017, 8, 18))
# partition = dg.DailyPartitionsDefinition(dt.datetime(2017, 8, 18))
partition = dg.WeeklyPartitionsDefinition(dt.datetime(2017, 8, 18))

from .subreddit import subreddit

