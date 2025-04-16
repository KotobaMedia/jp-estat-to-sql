#!/bin/bash -e

# A quick script to convert the data in PostGIS to a PMTiles vector archive.
# Prerequisites: data in PostGIS, tippecanoe

# Usage: ./to_vector.sh <output_file> <table_name> "<PG connection string>"

tmpdir=$(mktemp -d)
trap 'rm -rf "$tmpdir"' EXIT

output_file=$1
table_name=$2
pg_connection_string=$3

ogr2ogr -f FlatGeobuf "$tmpdir/output_full.fgb" PG:"$pg_connection_string" -sql "SELECT pref_name, city_name, s_name, jinko, setai, geom FROM $table_name"

ogr2ogr -f FlatGeobuf "$tmpdir/output_pref_city.fgb" PG:"$pg_connection_string" -sql "SELECT pref_name, city_name, SUM(jinko) as jinko, SUM(setai) as setai, ST_Union(geom, 0.000001) FROM $table_name GROUP BY pref_name, city_name"

ogr2ogr -f FlatGeobuf "$tmpdir/output_pref.fgb" PG:"$pg_connection_string" -sql "SELECT pref_name, SUM(jinko) as jinko, SUM(setai) as setai, ST_Union(geom, 0.000001) FROM $table_name GROUP BY pref_name"

tippecanoe -o "$tmpdir/pref.mbtiles" \
  -Z0 \
  -z5 \
  --no-simplification-of-shared-nodes \
  -f \
  --layer="$table_name" \
  -T jinko:int -T setai:int \
  "$tmpdir/output_pref.fgb"

tippecanoe -o "$tmpdir/pref_city.mbtiles" \
  -Z6 \
  -z9 \
  --no-simplification-of-shared-nodes \
  -f \
  --layer="$table_name" \
  -T jinko:int -T setai:int \
  "$tmpdir/output_pref_city.fgb"

tippecanoe -o "$tmpdir/full.mbtiles" \
  -Z10 \
  -z12 \
  --no-simplification-of-shared-nodes \
  -f \
  --layer="$table_name" \
  -T jinko:int -T setai:int \
  "$tmpdir/output_full.fgb"

tile-join -o "$output_file" \
  --force \
  "$tmpdir/pref.mbtiles" \
  "$tmpdir/pref_city.mbtiles" \
  "$tmpdir/full.mbtiles"
