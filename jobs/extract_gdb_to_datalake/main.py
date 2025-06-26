# -*- coding: utf-8 -*-
import os
import subprocess
import time
import sys
import datetime

import firebirdsql
import pandas as pd


def log(msg):
	current_time = datetime.datetime.now().replace(tzinfo=None)
	print(f"{current_time}| {msg}", flush=True)


def get_connection(db_path, user="SYSDBA", password="masterkey", charset="WIN1252"):
	con = None
	# Sometimes we can't connect the first or second times we try,
	# so let's loop it and limit our attempts
	MAX_ATTEMPTS = 20
	attempt = 1
	success = False
	while not success:
		# If we've tried too many times, then give up :\
		if attempt > MAX_ATTEMPTS:
			log("Conection to Firebird failed :(")
			exit(1)
		# Make sure Firebird is running. This command might be run multiple times
		# but it doesn't error out, so it's fine
		subprocess.call(["/etc/init.d/firebird", "start"])

		# We give it a few seconds to start and warm up, we got time
		time.sleep(5)

		# Attempts to connect to Firebird
		try:
			log(f"Attempting connection... {attempt}/{MAX_ATTEMPTS}")
			con = firebirdsql.connect(
				dsn=db_path,
				user=user,
				password=password,
				charset=charset
			)
			log("Conected!")
			success = True
		except firebirdsql.OperationalError as e:
			# Uh oh, we couldn't connect. Don't fret, this happens. We just gotta try
			# again a couple more times to make sure
			log(f"firebirdsql.OperationalError: {e}")
			attempt += 1
		except Exception as e:
			# Something unexpected happened. I don't know what it is. Good luck though
			log(f"Unexpected Exception!")
			log(repr(e))
			attempt += 1

	return con


def execute_query(con, query):
	try:
		cur = con.cursor()
		log(f"Running query:\n{query}")
		cur.execute(query)

		log("Obtaining results...")
		START_TIME = time.time()
		rows = cur.fetchall()
		TOTAL_TIME = time.time() - START_TIME
		log(f"Took {TOTAL_TIME:.1f}s")

		columns = [ desc[0] for desc in cur.description ]
		return (rows, columns)

	except Exception as e:
		log(f"Unexpected Exception!")
		log(repr(e))
		cur.close()
		con.close()
		exit(1)


def execute_query(con, query):
	try:
		cur = con.cursor()
		log(f"Running query:\n{query}")
		cur.execute(query)

		log("Obtaining results...")
		START_TIME = time.time()
		rows = cur.fetchall()
		TOTAL_TIME = time.time() - START_TIME
		log(f"Took {TOTAL_TIME:.1f}s")

		columns = [ desc[0] for desc in cur.description ]
		return (rows, columns)

	except Exception as e:
		log(f"Unexpected Exception!")
		log(repr(e))
		cur.close()
		con.close()
		exit(1)


def export_table_to_csv(con, table_name):
	log(f"Reading entire table '{table_name}'")

	try:
		query = f"""
SELECT * FROM {table_name}
		"""
		(rows, columns) = execute_query(con, query)
		df = pd.DataFrame(rows, columns=columns)
		row_count = len(df)
		log(f"Fetched {row_count} rows")

		# Guarantees /csv directory exists
		file_path = f"/data/csv/{table_name}.csv"
		os.makedirs(os.path.dirname(file_path), exist_ok=True)
		df.to_csv(file_path, mode='w', header=True, index=False)
		log(f"Saved to '{file_path}")

	except Exception as e:
		log(f"Unexpected Exception!")
		log(repr(e))
		con.close()
		exit(1)


def export_table_to_csv_chunked(con, table_name, chunk_size):
	log(f"Reading table '{table_name}' in chunks of {chunk_size} rows")

	offset = 0
	first_write = True
	while True:
		try:
			# Get chunked results via FIRST N SKIP M syntax using RDB$DB_KEY as a
			# unique representation of each table record
			# [Ref FIRST/SKIP] https://www.firebirdsql.org/refdocs/langrefupd20-select.html#langrefupd20-first-skip
			# [Ref RDB$DB_KEY] https://www.ibphoenix.com/articles/art-00000384
			query = f"""
SELECT FIRST {chunk_size} SKIP {offset} *
FROM {table_name}
ORDER BY RDB$DB_KEY
			"""
			(rows, columns) = execute_query(con, query)

			# We could manually write the CSV but we can just use Pandas instead
			df = pd.DataFrame(rows, columns=columns)
			row_count = len(df)
			log(f"Fetched {row_count} rows")

			file_path = f"/data/csv/{table_name}.csv"
			if first_write:
				# Guarantees /csv directory exists
				os.makedirs(os.path.dirname(file_path), exist_ok=True)
				df.to_csv(file_path, mode='w', header=True, index=False)
				first_write = False
			else:
				df.to_csv(file_path, mode='a', header=False, index=False)
			log(f"Saved to '{file_path}")

			# If we fetched no rows (empty table, row count is exact multiple
			# of chunk_size, ...), we're done
			if row_count <= 0:
				log("Fetched no rows; assuming end of table")
				break
			# If the number of rows fetched is less than chunk_size, we're done
			if row_count < chunk_size:
				log(f"Fetched fewer rows than `chunk_size` ({chunk_size}); assuming end of table")
				break
			# Increment offset for the next chunk
			offset += chunk_size

		except Exception as e:
			log(f"Unexpected Exception!")
			log(repr(e))
			con.close()
			exit(1)


################################################################################


def main():
	PATH = os.environ.get("FB_GDB_PATH")
	if not os.path.isfile(PATH):
		log(f"FB_GDB_PATH='{PATH}' is not a file!")
		exit(1)

	USER = os.environ.get("FB_USER", "SYSDBA")
	PASS = os.environ.get("FB_PASSWORD", "masterkey")
	CHAR = os.environ.get("FB_CHARSET", "WIN1252")

	TABLE_LIST = os.environ.get("TABLE_LIST", "all")
	NO_CHUNKS = os.environ.get("NO_CHUNKS") is not None

	# Attempts connection
	con = get_connection(PATH)

	# Gets all available tables in the Database
	# [Ref] https://ib-aid.com/download/docs/firebird-language-reference-2.5/fblangref-appx04-relations.html
	(found_tables, _) = execute_query(con, """
SELECT RDB$RELATION_NAME
FROM RDB$RELATIONS
WHERE RDB$SYSTEM_FLAG = 0 AND RDB$VIEW_BLR IS NULL
ORDER BY RDB$RELATION_NAME
	""")

	wanted_tables = None
	tables_that_exist = None
	# If user wants ALL tables
	if TABLE_LIST.lower() == "all":
		# WE GET THEM ALL
		wanted_tables = [ row[0] for row in found_tables ]
		tables_that_exist = wanted_tables
	# Otherwise
	else:
		# We split each table name on ';', strip whitespace
		wanted_tables = [ t.strip() for t in TABLE_LIST.split(";") ]
		tables_that_exist = []
		# For every table we found on the database
		for table_tuple in found_tables:
			table_name = table_tuple[0]
			# If this is a table we want
			if table_name in wanted_tables:
				# Save it
				tables_that_exist.append(table_name)

	log(f"Found {len(tables_that_exist)} requested tables (out of {len(wanted_tables)} requested, {len(found_tables)} total)\n")

	CHUNK_SIZE = 10_000
	# For every table that exists
	for i, table in enumerate(tables_that_exist):
		log(f"Reading table {i+1}/{len(tables_that_exist)}")

		# If user doesn't want chunks, we just try exporting the entire table
		if NO_CHUNKS:
			export_table_to_csv(con, table)
		# Otherwise, we do the more labor-intensive process of chunking the results
		else:
			export_table_to_csv_chunked(con, table, CHUNK_SIZE)

		log(f"Done with {table}!\n")
		log("-"*10)

	con.close()
	return


if __name__ == "__main__":
	main()