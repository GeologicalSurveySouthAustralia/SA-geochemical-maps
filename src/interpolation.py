import cartopy.crs as ccrs
import cartopy.feature as cfeature
import cartopy.mpl as cmpl
from matplotlib.colors import BoundaryNorm
import matplotlib.pyplot as plt
from mpl_toolkits.axes_grid1.inset_locator import InsetPosition, inset_axes
import numpy as np
import pandas as pd
from metpy.interpolate import interpolate_to_grid

places = [
    [138.60, -34.9349, "Adelaide"],
    [133.66, -32.099, "Ceduna"],
    [137.77, -32.49, "Port Augusta"],
    [136.8, -31.2, "Woomera"],
    [134.75, -29.01, "Coober Pedy"],
    [132.04, -26.46, "Umuwa"],
    [139.55, -32.58, "Yunta"],
    [134.57, -30.71, "Tarcoola"],
]
extent = [129, 141, -38, -26]


def MapBase(proj, title, inset_title, add_inset=True):
    """Make our basic default map for plotting"""
    fig = plt.figure(figsize=(10, 11))
    # view = fig.add_axes([0, 0, 1, 1], projection=proj, aspect='auto')
    view = fig.add_subplot(1, 1, 1, projection=proj, aspect="auto")
    view.set_title(title, fontsize=20)
    view.set_extent(extent)
    view.add_feature(cfeature.STATES.with_scale("10m"))
    view.add_feature(cfeature.OCEAN)
    view.add_feature(cfeature.COASTLINE)
    view.add_feature(cfeature.BORDERS, linestyle=":")
    gl = view.gridlines(color="lightgrey", linestyle="-", draw_labels=True)
    gl.top_labels = gl.right_labels = False
    for p in places:
        view.text(
            p[0] + 0.1,
            p[1],
            p[2],
            horizontalalignment="left",
            transform=ccrs.Geodetic(),
        )
        view.plot(
            p[0],
            p[1],
            color="blue",
            marker="o",
            markersize=5,
            transform=ccrs.Geodetic(),
        )
    if add_inset:
        # inset = plt.axes([0, 0, 1, 1], projection=proj)
        inset = inset_axes(
            view,
            width="30%",
            height="40%",
            loc=3,
            axes_class=cmpl.geoaxes.GeoAxes,
            axes_kwargs=dict(map_projection=proj),
        )
        # ip = InsetPosition(view, [0.01, 1.0, 0.3, 0.4])
        # inset.set_axes_locator(ip)
        inset.set_title(inset_title, fontsize=12)
        inset.set_extent(extent)
        inset.add_feature(cfeature.STATES.with_scale("10m"))
        inset.add_feature(cfeature.OCEAN)
        inset.add_feature(cfeature.COASTLINE)
    else:
        inset = None

    return fig, view, inset


def interpolate(
    in_data: pd.DataFrame, proj, interp_type="natural_neighbor", hres=10000
):
    x, y, data = (
        in_data.LONGITUDE_GDA2020_x.values,
        in_data.LATITUDE_GDA2020_x.values,
        in_data.times_ave_crustal_abund.values,
    )
    xp, yp, _ = proj.transform_points(ccrs.Geodetic(), x, y).T

    gx, gy, img = interpolate_to_grid(xp, yp, data, interp_type=interp_type, hres=hres)
    img = np.ma.masked_where(np.isnan(img), img)

    return gx, gy, img
