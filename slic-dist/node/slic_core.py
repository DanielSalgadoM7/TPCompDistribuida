"""SLIC puro (sem rede). Só NumPy/SciPy/scikit-image.

Divisão do trabalho do SLIC iterativo:
  - assign():         atribui cada pixel da faixa ao centro mais próximo  (nos workers)
  - accumulate():     soma (l,a,b,x,y) e conta pixels por centro          (nos workers)
  - update_centers(): soma as parciais e recalcula os centros             (no líder)
Cada pixel é atribuído independentemente dos outros, por isso não precisa de halo.
"""
import numpy as np
from scipy import ndimage
from skimage.measure import label as cc_label


def init_centers(lab, k):
    """Centros em grade regular. Retorna (centers[K,5], S)."""
    h, w, _ = lab.shape
    s = max(int(np.sqrt(h * w / k)), 1)
    ys = np.arange(s // 2, h, s)
    xs = np.arange(s // 2, w, s)
    centers = [[*lab[y, x], x, y] for y in ys for x in xs]
    return np.array(centers, dtype=np.float32), s


def assign(strip_lab, g0, centers, s, m):
    """strip_lab: (h,W,3) com as linhas globais g0..g0+h. Retorna labels (h,W) int32."""
    h, w, _ = strip_lab.shape
    dist = np.full((h, w), np.inf, dtype=np.float32)
    labels = np.full((h, w), -1, dtype=np.int32)
    wxy = (m / s) ** 2
    for k, (l, a, b, cx, cy) in enumerate(centers):
        y0 = max(int(cy - s) - g0, 0)
        y1 = min(int(cy + s) + 1 - g0, h)
        x0 = max(int(cx - s), 0)
        x1 = min(int(cx + s) + 1, w)
        if y0 >= y1 or x0 >= x1:
            continue
        p = strip_lab[y0:y1, x0:x1]
        dlab = (p[..., 0] - l) ** 2 + \
            (p[..., 1] - a) ** 2 + (p[..., 2] - b) ** 2
        dxy = (np.arange(x0, x1)[None, :] - cx) ** 2 + \
            (np.arange(y0, y1)[:, None] + g0 - cy) ** 2
        d = dlab + wxy * dxy
        sub = dist[y0:y1, x0:x1]
        mask = d < sub
        sub[mask] = d[mask]
        labels[y0:y1, x0:x1][mask] = k
    return labels


def accumulate(strip_lab, g0, labels, k):
    """Somas parciais por centro: sums[K,5] (l,a,b,x,y) e counts[K]."""
    valid = labels >= 0
    lb = labels[valid]
    ys, xs = np.nonzero(valid)
    vals = strip_lab[valid]
    sums = np.zeros((k, 5), dtype=np.float64)
    for c in range(3):
        sums[:, c] = np.bincount(lb, weights=vals[:, c], minlength=k)
    sums[:, 3] = np.bincount(lb, weights=xs, minlength=k)
    sums[:, 4] = np.bincount(lb, weights=ys + g0, minlength=k)
    counts = np.bincount(lb, minlength=k).astype(np.int32)
    return sums, counts


def update_centers(sums, counts, old):
    """Novos centros = média. Retorna (novos, resíduo = deslocamento médio em pixels)."""
    new = old.copy()
    nz = counts > 0
    new[nz] = (sums[nz] / counts[nz, None]).astype(np.float32)
    residual = float(np.linalg.norm(new[:, 3:] - old[:, 3:], axis=1).mean())
    return new, residual


def enforce_connectivity(labels, s):
    """Pós-processamento: preenche pixels sem rótulo e funde fragmentos pequenos."""
    if (labels < 0).any():
        idx = ndimage.distance_transform_edt(
            labels < 0, return_distances=False, return_indices=True)
        labels = labels[tuple(idx)]
    comp = cc_label(labels, connectivity=1)
    sizes = np.bincount(comp.ravel())
    min_size = (s * s) // 4
    for idx, sl in enumerate(ndimage.find_objects(comp), start=1):
        if sl is None or sizes[idx] >= min_size:
            continue
        sl2 = (slice(max(sl[0].start - 1, 0), sl[0].stop + 1),
               slice(max(sl[1].start - 1, 0), sl[1].stop + 1))
        region = comp[sl2]
        mask = region == idx
        ring = ndimage.binary_dilation(mask) & ~mask
        neigh = region[ring]
        neigh = neigh[neigh != idx]
        if neigh.size:
            region[mask] = np.bincount(neigh).argmax()
    _, out = np.unique(comp, return_inverse=True)
    return out.reshape(labels.shape).astype(np.int32)


def slic_local(lab, k, m=10.0, max_iter=10, tol=0.1):
    """Versão sequencial completa (referência para testes e para comparar desempenho)."""
    centers, s = init_centers(lab, k)
    labels = None
    for _ in range(max_iter):
        labels = assign(lab, 0, centers, s, m)
        sums, counts = accumulate(lab, 0, labels, len(centers))
        centers, res = update_centers(sums, counts, centers)
        if res < tol:
            break
    return enforce_connectivity(labels, s)


if __name__ == "__main__":
    # Teste isolado: python node/slic_core.py
    import time
    from skimage import data, io, segmentation, img_as_ubyte
    from skimage.color import rgb2lab
    img = data.astronaut()
    t0 = time.time()
    out = slic_local(rgb2lab(img).astype(np.float32), 400)
    print(f"{out.max() + 1} superpixels em {time.time() - t0:.2f}s")
    io.imsave("local.png", img_as_ubyte(
        segmentation.mark_boundaries(img, out)))
