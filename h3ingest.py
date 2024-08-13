"""
Sketch of idea related to representing arbitrary sensor data as a generic collection of hex grids

Assumptions: slight spatial aggregation (1) improves relatability with other data sources at other
spatial resolutions; (2) aggregation actually slightly increases the confidence of the mean value
represented by the hex grid cell (the pixels are already some sort of complicated average from
photons that scattered all around the location of interest) 

Some numbers: Using grid zoom level 11 for 20m resolution and level 12 for 10m resolution bands
results in about 4-6 pixels per cell. There are about 10,000 observed S2 tiles in one day. Each
data set is on the order of 100s of MB. The resulting parquet files here are on the order of 10s
of MB, thus about 90% compression.

❯ python h3ingest.py
S2B_MSIL2A_20231201T235229_R130_T57KUS_20231202T022040 vis_nir [157.06563865, -21.78317872] 2023-12-01 10.0 3.417688319228384 0.058893638772103685
> /Users/rbraswell/repo/cronch/h3ingest.py(59)process_one_item()
-> for band_group, band_info in BAND_GROUPS.items():
(Pdb) df
                   hex12 band  value
0        8c9e688000001ff  B02   2469
1        8c9e688000001ff  B03   2321
2        8c9e688000001ff  B04   2385
3        8c9e688000001ff  B08   2358
4        8c9e688000003ff  B02   2458
...                  ...  ...    ...
8308307  8c9e6d49edb6bff  B08   2281
8308308  8c9e6d49edb6dff  B02   2510
8308309  8c9e6d49edb6dff  B03   2359
8308310  8c9e6d49edb6dff  B04   2346
8308311  8c9e6d49edb6dff  B08   2342

[8308312 rows x 3 columns]
"""
import pystac_client
import h3
import planetary_computer
import odc.stac
import geopandas as gpd
import ray.remote_function
from shapely.geometry import shape
from shapely.geometry.polygon import Polygon
import ray


# This could be expanded to include other sensors, just Sentinel-2 for now
BAND_GROUPS = {
    "vis_nir": {
        "resolution": 10.0,
        "zoom": 12,
        "bands": ["B02", "B03", "B04", "B08"],
    },
    "swir_rededge": {
        "resolution": 20.0,
        "zoom": 11.0,
        "bands": ["B05", "B06", "B07", "B11", "B12"],
    }
}

# Random day, Sentinel-2 has about 3700 of them
TEST_DAY = "2023-12-01"


# could use @ray.remote() here, run on a Ray cluster, but it would still take a while
def process_one_item(item):
    """
    Take a STAC item and process selected assets into H3 hexagons
    """
    date = item.get_datetime().date()
    llcorner = item.bbox[:2]

    geom = item.geometry
    shapely_polygon: Polygon = shape(geom)
    gdf = gpd.GeoDataFrame({"index": [0], "geometry":[shapely_polygon]}, geometry="geometry", crs="EPSG:4326")
    area = float(gdf.to_crs("epsg:5070").area[0])

    poly = h3.geo_to_h3shape(geom)

    # treat 10m and 20m bands (or whatever bands) separately
    for band_group, band_info in BAND_GROUPS.items():

        res = band_info["resolution"]
        hex_level = band_info["zoom"]
        bands = band_info["bands"]

        data = odc.stac.load([item], bands=bands)
        assert res == float(data.spatial_ref.GeoTransform.split()[1]), "Resolution is donked up"

        # different data sources might represent this differently
        num_tile_pixels = data.sizes["x"] * data.sizes["y"]
        num_pixels = area / res**2
        valid_frac = num_pixels/num_tile_pixels

        # average number of pixels per cell
        cells = h3.h3shape_to_cells(poly, hex_level)
        ratio = num_pixels / len(cells)

        # this seems like too much pandas, in a couple different ways
        df = data\
            .to_dataframe()\
            .drop(columns=["spatial_ref"])\
            .stack()\
            .reset_index()\
            .drop(columns=["time"])\
            .rename(columns={"level_3":"band", 0:"value"})\

        # H3 uses geographic lon lat for coordinates exclusively, so
        df = gpd.GeoDataFrame(df, geometry=gpd.points_from_xy(df.x, df.y), crs=data.spatial_ref.crs_wkt)
        df = df.to_crs("epsg:4326")

        # find hex grids containing the points
        hex_col = 'hex'+str(hex_level)
        df[hex_col] = df.apply(lambda row: h3.latlng_to_cell(row.geometry.y, row.geometry.x, hex_level), axis=1)

        # aggregate data within the hex grids
        df = df.groupby([hex_col,"band"])["value"]\
            .mean()\
            .to_frame("value")\
            .reset_index()\
            .astype({"value":"int16"})

        # Could do this stuff to make a plot but I think there are better ways
        # from plot_utils import plot_scatter
        # df["lat"] = df[hex_col].apply(lambda x: h3.cell_to_latlng(x)[0])
        # df["lng"] = df[hex_col].apply(lambda x: h3.cell_to_latlng(x)[1])
        # plot_scatter(df, metric_col='value', marker='o',figsize=(17,15))
        # plt.show()

        # I learned a lot from just looking at these items
        print(item.id, band_group, llcorner, date, res, ratio, valid_frac)

        # If we were using Snowflake or some other big data thing we could save records instead of
        # parquet. I did this just to check out file sizes. Million-ish records, takes a minute.
        df.to_parquet(f"{item.id}.parquet")

    # yep
    breakpoint()


def main():

    catalog = pystac_client.Client.open(
        "https://planetarycomputer.microsoft.com/api/stac/v1",
        modifier=planetary_computer.sign_inplace,
    )

    # so many other options https://stacindex.org/catalogs/microsoft-pc#/
    collection_id = "sentinel-2-l2a"

    items = catalog.search(
        collections=[collection_id],
        datetime=TEST_DAY,
    ).item_collection()

    # try just one to give you the idea
    # I did about a hundred to look at timing/sizing answer is "slow/big"
    process_one_item(items[0])

    # futures = [process_one_item.remote(item) for item in items]
    # dont_actually_do_this = ray.get(futures)


if __name__ == "__main__":
    main()
