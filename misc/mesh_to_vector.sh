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

ogr2ogr -f FlatGeobuf "$tmpdir/output1.fgb" PG:"$pg_connection_string" -sql 'SELECT left(x."KEY_CODE"::text, 4)::bigint as "KEY_CODE", 1 as "level", sum(x."人口（総数）") as "人口（総数）", sum(x."人口（総数）男") as "人口（総数）男", sum(x."人口（総数）女") as "人口（総数）女", jismesh.to_meshpoly_geom(left(x."KEY_CODE"::text, 4)::bigint) as "geom" FROM jp_estat_mesh_2020_t001140_3 x GROUP BY left(x."KEY_CODE"::text, 4)::bigint'

ogr2ogr -f FlatGeobuf "$tmpdir/output2.fgb" PG:"$pg_connection_string" -sql 'SELECT left(x."KEY_CODE"::text, 6)::bigint as "KEY_CODE", 2 as "level", sum(x."人口（総数）") as "人口（総数）", sum(x."人口（総数）男") as "人口（総数）男", sum(x."人口（総数）女") as "人口（総数）女", jismesh.to_meshpoly_geom(left(x."KEY_CODE"::text, 6)::bigint) as "geom" FROM jp_estat_mesh_2020_t001140_3 x
GROUP BY left(x."KEY_CODE"::text, 6)::bigint'

ogr2ogr -f FlatGeobuf "$tmpdir/output3.fgb" PG:"$pg_connection_string" -mapFieldType IntegerList=String,Integer64List=String -sql "SELECT x.*, 3 as "level", jismesh.to_meshpoly_geom(x.\"KEY_CODE\") as \"geom\" FROM jp_estat_mesh_2020_t001140_3 x"

ogr2ogr -f FlatGeobuf "$tmpdir/output4.fgb" PG:"$pg_connection_string" -mapFieldType IntegerList=String,Integer64List=String -sql "SELECT x.*, 4 as "level", jismesh.to_meshpoly_geom(x.\"KEY_CODE\") as \"geom\" FROM jp_estat_mesh_2020_t001141_4 x"

ogr2ogr -f FlatGeobuf "$tmpdir/output5.fgb" PG:"$pg_connection_string" -mapFieldType IntegerList=String,Integer64List=String -sql "SELECT x.*, 5 as "level", jismesh.to_meshpoly_geom(x.\"KEY_CODE\") as \"geom\" FROM jp_estat_mesh_2020_t001142_5 x"

tippecanoe -o "$tmpdir/1.mbtiles" \
    -Z0 \
    -z4 \
    --no-simplification-of-shared-nodes \
    -f \
    --layer=jp_estat_mesh_2020 \
    "$tmpdir/output1.fgb"

tippecanoe -o "$tmpdir/2.mbtiles" \
    -Z5 \
    -z8 \
    --no-simplification-of-shared-nodes \
    -f \
    --layer=jp_estat_mesh_2020 \
    "$tmpdir/output2.fgb"

tippecanoe -o "$tmpdir/3.mbtiles" \
    -Z9 \
    -z10 \
    --no-simplification-of-shared-nodes \
    -f \
    --layer=jp_estat_mesh_2020 \
    "$tmpdir/output3.fgb"

tippecanoe -o "$tmpdir/4.mbtiles" \
    -Z11 \
    -z11 \
    --no-simplification-of-shared-nodes \
    -f \
    --layer=jp_estat_mesh_2020 \
    "$tmpdir/output4.fgb"

tippecanoe -o "$tmpdir/5.mbtiles" \
    -Z12 \
    -z12 \
    --no-simplification-of-shared-nodes \
    -f \
    --layer=jp_estat_mesh_2020 \
    "$tmpdir/output5.fgb"

tile-join -o "$output_file" \
    --force \
    "$tmpdir/1.mbtiles" \
    "$tmpdir/2.mbtiles" \
    "$tmpdir/3.mbtiles" \
    "$tmpdir/4.mbtiles" \
    "$tmpdir/5.mbtiles"
