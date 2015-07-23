# googleclusterdata
a collection of scripts to convert googleclusterdata into useful formats

# Formats

Currently the only supported output is postgresql via the psycopg2 module.

# Warning

None of the scripts here are secured against malicious data. Make sure to
use the original Google dataset, verify the checksums, look at the code, 
etc. As stated in the license, *you are using these tools at your own risk*!

# Flow

1. copy `conf/defaults.cfg.example` to `conf/defaults.cfg` and edit it
2. create the tables with `apply_schema.py`
3. fill the tables with `fill_tables.py` (run with option `-d` to test)

