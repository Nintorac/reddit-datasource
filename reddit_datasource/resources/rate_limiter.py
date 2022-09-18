

from dagster import resource
from pyrate_limiter import Duration, Limiter, FileLockSQLiteBucket, RequestRate

@resource
def limiter():

    rate = RequestRate(1, 6*Duration.SECOND)
    limiter = Limiter(rate, bucket_class=FileLockSQLiteBucket)


    return limiter