# File scraper

This program can scrape an s3 bucket or filesystem and record the file's metadata in a postgresql table.


## Deleted Files

If a file gets deleted it will get marked as deleted in the table.  The timestamp will be when the file-scraper notices that the file is gone.
