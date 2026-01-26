import h5py
import matplotlib.pyplot as plt
import numpy as np
from matplotlib.colors import BoundaryNorm, to_rgb

from config import CHMI_COLORS, PRECIP_LEVELS


def maxz_to_png(hdf_path: str, output_path: str, dpi: int = 100):
    """
    Convert a CHMI MaxZ ODIM_H5 radar file to PNG using OPERA dBZ encoding.
    """
    RAW_MIN = 73
    RAW_STEP = 8
    N_BINS = len(CHMI_COLORS)
    with h5py.File(hdf_path, "r") as hdf:
        data = hdf["/dataset1/data1/data"][:]
        attrs = hdf["/dataset1/data1/what"].attrs
        nodata = attrs.get("nodata", None)
        undetect = attrs.get("undetect", None)
    raw = data.astype(int)
    bin_indices = (raw - RAW_MIN) // RAW_STEP
    mask = (bin_indices < 0) | (bin_indices >= N_BINS)
    if nodata is not None:
        mask |= data == nodata
    if undetect is not None:
        mask |= data == undetect

    bin_indices[mask] = -1
    rgba = np.zeros((*raw.shape, 4), dtype=np.float32)
    for idx, color in enumerate(CHMI_COLORS):
        rgba[bin_indices == idx, :3] = to_rgb(color)
        rgba[bin_indices == idx, 3] = 1.0
    height, width = raw.shape

    # calculate rain score normalized from 0 to 1
    valid = bin_indices >= 0  # raining pixels (non-transparent)
    total_pixels = bin_indices.size  # all pixels
    rain_pixels = np.sum(valid)
    rain_score = float(rain_pixels / total_pixels)

    fig = plt.figure(figsize=(width / dpi, height / dpi), dpi=dpi)
    ax = fig.add_axes([0, 0, 1, 1])
    ax.set_facecolor("none")
    ax.imshow(rgba, origin="upper", interpolation="none", aspect="auto")
    ax.axis("off")
    fig.patch.set_alpha(0.0)
    plt.savefig(output_path, dpi=dpi, transparent=True, facecolor=(0, 0, 0, 0))
    plt.close()

    return rain_score


def merge1h_to_png(hdf_path: str, output_path: str, dpi: int = 100):
    """
    Convert CHMI Merge1h ODIM_H5 file to PNG using physical units and proper colormap.
    """
    with h5py.File(hdf_path, "r") as hdf:
        data = hdf["/dataset1/data1/data"][:]
        what = hdf["/dataset1/data1/what"].attrs
        gain = what.get("gain", 1.0)
        offset = what.get("offset", 0.0)
        nodata = what.get("nodata", None)
        undetect = what.get("undetect", None)
    data_physical = data.astype(np.float32) * gain + offset
    mask_values = []
    if nodata is not None:
        mask_values.append(nodata)
    if undetect is not None:
        mask_values.append(undetect)
    mask = np.isin(data, mask_values)
    mask |= data_physical < PRECIP_LEVELS[0]
    norm = BoundaryNorm(PRECIP_LEVELS, ncolors=len(CHMI_COLORS), clip=True)
    normed = norm(data_physical)
    normed[mask] = -1

    # calculate rain score
    total_pixels = normed.size
    rain_pixels = np.sum(normed >= 0)
    rain_score = float(rain_pixels / total_pixels)

    rgba = np.zeros((*data.shape, 4), dtype=np.float32)
    for idx, hex_color in enumerate(CHMI_COLORS):
        rgba[normed == idx, :3] = to_rgb(hex_color)
        rgba[normed == idx, 3] = 1.0
    height, width = data.shape
    fig = plt.figure(figsize=(width / dpi, height / dpi), dpi=dpi)
    ax = fig.add_axes([0, 0, 1, 1])
    ax.set_facecolor("none")
    ax.imshow(rgba, origin="upper", interpolation="none", aspect="auto")
    ax.axis("off")
    fig.patch.set_alpha(0.0)
    plt.savefig(output_path, dpi=dpi, transparent=True, facecolor=(0, 0, 0, 0))
    plt.close()
    return rain_score
