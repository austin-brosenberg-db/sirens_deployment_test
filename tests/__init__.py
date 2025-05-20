import os
import sys

# Ensures local spark cluster uses same python as is running this test
os.environ['PYSPARK_PYTHON'] = sys.executable