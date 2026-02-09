from pathlib import Path

import h5py
import matplotlib.pyplot as plt
import numpy as np
from matplotlib.colors import BoundaryNorm, to_rgb
from PIL import Image

from config import CHMI_COLORS, PRECIP_LEVELS

# CHMI legend thresholds in dBZ (15) - lower bounds of bins
CHMI_DBZ_THRESHOLDS = np.array(
    [4, 8, 12, 16, 20, 24, 28, 32, 36, 40, 44, 48, 52, 56, 60],
    dtype=np.float32,
)

PALETTE_RGB = np.array(
    [[int(c[i : i + 2], 16) for i in (1, 3, 5)] for c in CHMI_COLORS],
    dtype=np.uint8,
)


def hdf_to_png(
    hdf_path,
    png_path,
    *,
    raw_visible_min: int | None = 78,  # None = no extra suppression, use >=4 dBZ
):
    hdf_path = Path(hdf_path)
    png_path = Path(png_path)
    with h5py.File(hdf_path, "r") as hdf:
        raw = hdf["/dataset1/data1/data"][:].astype(np.uint8)
        what = hdf["/dataset1/data1/what"].attrs
        gain = float(what["gain"])
        offset = float(what["offset"])
        nodata = int(float(what["nodata"]))
        undetect = int(float(what["undetect"]))
    # mask invalid pixels (CHMI shows them as black)
    invalid = (raw == nodata) | (raw == undetect)
    # decode to dBZ (used only for binning / optional legend cutoff)
    dbz = raw.astype(np.float32) * gain + offset
    # bisibility mask
    if raw_visible_min is None:
        # pure legend behavior: show everything >= 4 dBZ (Max Z)
        visible = np.logical_and(np.logical_not(invalid), dbz >= 4.0)
    else:
        # CHMI CAPPI behavior: suppress weak echoes in RAW space
        visible = np.logical_and(np.logical_not(invalid), raw >= raw_visible_min)
    # build class indices 0..14 using thresholds:
    cls = np.searchsorted(CHMI_DBZ_THRESHOLDS, dbz, side="right") - 1
    cls = cls.astype(np.int16)
    # anything not visible -> -1
    cls[~visible] = -1
    # rain score (normalized 0..1): fraction of visible (non-transparent) pixels
    total_pixels = cls.size
    rain_pixels = int(np.sum(cls >= 0))
    rain_score = float(rain_pixels / total_pixels)
    # RGBA output (transparent background)
    h, w = raw.shape
    out = np.zeros((h, w, 4), dtype=np.uint8)
    m = cls >= 0
    out[m, :3] = PALETTE_RGB[cls[m]]
    out[m, 3] = 255  # opaque where colored
    Image.fromarray(out, mode="RGBA").save(png_path)
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
