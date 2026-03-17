#!/usr/bin/env python3

import argparse
import asyncio
import logging
import traceback

from archive import database_api, decision_api, store_api, utility_api
import hop
from hop import bson

async def index_message(record, db, dc, st, update):
	if record.size > dc.text_index_message_size_limit:
		return False # don't try if the message is too big
	if not dc.should_index_topic(record.topic, record.timestamp/1000):
		return False # ignore messages on non-indexed topics
	# try to pull the raw data
	raw_data = await st.get_object(record.key)
	if raw_data is None:
		return False # give up if the data is in accessible
	data = bson.loads(raw_data)
	message = data["message"]
	metadata = hop.io.Metadata(**data["metadata"], partition=0, offset=0, _raw=None)
	headers = metadata.headers
	annotations = data["annotations"]
	for attr in ["size", "key", "bucket", "crc32", "title", "sender", "file_name"]:
		annotations[attr] = getattr(record, attr)
	if "media_type" not in annotations:
		annotations["media_type"] = dc.get_data_format(message, headers)
	text_to_index = dc.get_indexable_text(message, headers, annotations)
	await db.set_indexed_text(record.uuid, text_to_index,
	                          annotations.get("text_fully_indexed", False), is_update=update)
	return True

async def reindex_text(config):
	db = database_api.DbFactory(config)
	st = store_api.StoreFactory(config)
	dc = decision_api.Decider(config)
	start_time = config["start_time"]
	try:
		await db.connect()
		await db.make_schema()
		await st.connect()
		
		reindexed = 0
		total_not_fully_indexed = 0
		if config["reindex_partial"]:
			next_bookmark = None
			while(True):
				not_indexed, next_bookmark, _ = await db.get_messages_not_fully_text_indexed(next_bookmark, 1024, start_time=start_time)
				total_not_fully_indexed += len(not_indexed)
				for record in not_indexed:
					try:
						if await index_message(record, db, dc, st, True):
							reindexed += 1
					except Exception as e:
						print(f"Failed to re-index message {record.uuid}: {e}")
				if next_bookmark is None:
					break
		
		total_not_indexed = 0
		next_bookmark = None
		while(True):
			not_indexed, next_bookmark, _ = await db.get_messages_not_text_indexed(next_bookmark, 1024, start_time=start_time)
			total_not_indexed += len(not_indexed)
			print(f" results so far: {total_not_indexed}, first entry: {not_indexed[0].id}")
			for record in not_indexed:
				try:
					if await index_message(record, db, dc, st, False):
						reindexed += 1
				except Exception as e:
					print(f"Failed to index message {record.uuid}: {e}")
			if next_bookmark is None:
				break
	except KeyboardInterrupt:
		pass
	finally:
		await db.close()
		await st.close()
		dc.close()
	if config["reindex_partial"]:
		print(f"There were {total_not_fully_indexed} messages not fully text-indexed")
	print(f"There were {total_not_indexed} messages not text-indexed")
	print(f"Re-indexed {reindexed} messages")

if __name__ == "__main__":
	parser = argparse.ArgumentParser(
		description="Initialize the archive database structure",
		formatter_class=argparse.RawDescriptionHelpFormatter)
	
	database_api.add_parser_options(parser)
	decision_api.add_parser_options(parser)
	store_api.add_parser_options(parser)
	utility_api.add_parser_options(parser)
	
	parser.add_argument("--start-time", help="Earliest timestamp to process", type=int, default=None, required=False)
	parser.add_argument("--reindex-partial", help="More completely index messages which were not fully indexed before", type=bool, default=False, required=False)
	
	config = parser.parse_args().__dict__
	
	utility_api.make_logging(config)
	
	asyncio.run(reindex_text(config))
