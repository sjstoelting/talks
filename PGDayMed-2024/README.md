# PostgreSQL JSON Features - NoSQL In A RDBMS

The first JSON support was integrated in PostgreSQL in version 9.2. Every new version afterwards implemented new JSON features. This way users have been enabled to use SQL and NoSQL in one database with the best solutions of both paradigmas.

In this talk I will take the participants on a journey through PostgreSQL JSON and JSONB features that are already available in PostgreSQL up to 17.<br />
It will include json_path_query/jsonb_path_query and json_table, which works with both, JSON and JSONB.

You will see examples with data to experience, what is and will be possible.


# PostgreSQL As Data Integration Tool With Foreign Data Wrappers

Instead of using ETL Tools, which consume tons of memory on their own system, you will learn how to do ETL jobs directly in and with a database.
The PostgreSQL implementation of the the standard ISO/IEC 9075-9:2016, Management of External Data ([SQL/MED](https://www.iso.org/standard/63476.html)), is also known as Foreign Data Wrapper (FDW). With Foreign Data Wrapper, there is nearly no limit of external data, that you could use directly inside a PostgreSQL database.

The talk will walk you through the definition of Foreign Data Wrapper as implemented in PostgreSQL.
In the second part of the talk you will see how this technology does work shown by examples with several data sources.

In addition it will cover how I ended up bringing some Foreign Data Wrappers alive again.
