import h3
from matplotlib import pyplot as plt
import contextily as cx


def plot_df(df, column=None, ax=None):
    'Plot based on the `geometry` column of a GeoPandas dataframe'
    df = df.copy()
    df = df.to_crs(epsg=3857) # web mercator
    if ax is None:
        fig, ax = plt.subplots(figsize=(8,8))
    ax.get_xaxis().set_visible(False)
    ax.get_yaxis().set_visible(False)
    df.plot(
        ax=ax,
        alpha=0.5, edgecolor='k',
        column=column, categorical=True,
        legend=True, legend_kwds={'loc': 'upper left'}, 
    )
    cx.add_basemap(ax, crs=df.crs, source=cx.providers.CartoDB.Positron)


def plot_shape(shape, ax=None):
    df = gpd.GeoDataFrame({'geometry': [shape]}, crs='EPSG:4326')
    plot_df(df, ax=ax)


def plot_cells(cells, ax=None):
    shape = h3.cells_to_h3shape(cells)
    plot_shape(shape, ax=ax)


def plot_scatter(df, metric_col, x='lng', y='lat', marker='.', alpha=1, figsize=(16,12), colormap='viridis'):    
    df.plot.scatter(x=x, y=y, c=metric_col, title=metric_col, edgecolors='none', colormap=colormap, marker=marker, alpha=alpha, figsize=figsize)
    plt.xticks([], []); plt.yticks([], [])
