import time

from pyspark.sql.functions import udf
from pyspark.sql.types import LongType

from mymodule import fns

squared_udf = udf(fns.square, LongType())
