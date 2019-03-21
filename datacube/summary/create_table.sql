-- Create agdc schema

create schema if not exists agdc;

-- Create product_sumaries table

create table if not exists agdc.product_summaries (
	-- ID PK
	id serial primary key,

    -- A Unique query with field names in Json
	query jsonb not null,

    -- Bounding box
	spatial_bounds jsonb,

    -- GEOJson extent of datasets of the query
	spatial_footprint jsonb,

	-- dates.  A JSON array of strings in 'YYYY-MM-DD' format.
	dates jsonb,

	-- When it was added and by whom.
	added timestamptz default now() not null,
	added_by name default user not null,

	unique(query)
);