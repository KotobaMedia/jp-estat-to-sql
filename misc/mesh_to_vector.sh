#!/bin/bash -e

# A quick script to convert the data in PostGIS to a PMTiles vector archive.
# Prerequisites: data in PostGIS, tippecanoe
# This only works on 2020 data right now.
# Make sure all 3,4,5 zoom levels are available in the database.

# Usage: ./mesh_to_vector.sh <output_file> "<PG connection string>"

tmpdir=$(mktemp -d)
# echo "Working in temporary directory: $tmpdir"
trap 'rm -rf "$tmpdir"' EXIT

output_file=$1
pg_connection_string=$2

ogr2ogr -f FlatGeobuf "$tmpdir/output3.fgb" PG:"$pg_connection_string" -mapFieldType IntegerList=String,Integer64List=String -sql "SELECT x.*, jismesh.to_meshpoly_geom(x.\"KEY_CODE\") as \"geom\" FROM jp_estat_mesh_2020_t001140_3 x"

ogr2ogr -f FlatGeobuf "$tmpdir/output4.fgb" PG:"$pg_connection_string" -mapFieldType IntegerList=String,Integer64List=String -sql "SELECT x.*, jismesh.to_meshpoly_geom(x.\"KEY_CODE\") as \"geom\" FROM jp_estat_mesh_2020_t001141_4 x"

ogr2ogr -f FlatGeobuf "$tmpdir/output5.fgb" PG:"$pg_connection_string" -mapFieldType IntegerList=String,Integer64List=String -sql "SELECT x.*, jismesh.to_meshpoly_geom(x.\"KEY_CODE\") as \"geom\" FROM jp_estat_mesh_2020_t001142_5 x"

tippecanoe -o "$tmpdir/3.mbtiles" \
  -Z0 \
  -z9 \
  --no-simplification-of-shared-nodes \
  -f \
  --layer=jp_estat_mesh_2020 \
  "$tmpdir/output3.fgb"

tippecanoe -o "$tmpdir/4.mbtiles" \
  -Z10 \
  -z12 \
  --no-simplification-of-shared-nodes \
  -f \
  --layer=jp_estat_mesh_2020 \
  "$tmpdir/output4.fgb"

tippecanoe -o "$tmpdir/5.mbtiles" \
  -Z13 \
  -z13 \
  --no-simplification-of-shared-nodes \
  -f \
  --layer=jp_estat_mesh_2020 \
  "$tmpdir/output5.fgb"

tile-join -o "$output_file" \
  --force \
  "$tmpdir/3.mbtiles" \
  "$tmpdir/4.mbtiles" \
  "$tmpdir/5.mbtiles"
