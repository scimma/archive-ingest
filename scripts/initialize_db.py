#!/usr/bin/env python3

import logging
import argparse
import asyncio
from archive import utility_api
from archive import database_api

async def initialize_db(config):
    db = database_api.DbFactory(config)
    await db.connect()
    await db.make_schema()
    await db.close()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Initialize the archive database structure",
        formatter_class=argparse.RawDescriptionHelpFormatter)
    
    database_api.add_parser_options(parser)
    utility_api.add_parser_options(parser)

    config = parser.parse_args().__dict__

    utility_api.make_logging(config)

    asyncio.run(initialize_db(config))
