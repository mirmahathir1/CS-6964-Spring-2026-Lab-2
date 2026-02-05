"""
Simple Google Drive dataset downloader for Lab 2.

Downloads the public dataset zip from Google Drive and extracts CSVs into:
  ./dataset/

Usage:
  python downloader.py

Or from the notebook:
  from downloader import ensure_dataset
  ensure_dataset()
"""

from __future__ import annotations

import html as _html
import os
import re
import shutil
import sys
import tempfile
import zipfile
from pathlib import Path
from urllib import request
from urllib.parse import urlencode
import http.cookiejar


FILE_ID = "1HJgF25M5Fq8LbV2XMWWxp_ApT1TbqbIn"
SHARE_URL = f"https://drive.google.com/file/d/{FILE_ID}/view?usp=sharing"


def _default_base_dir() -> Path:
    # Notebook and script live in the same directory.
    return Path(__file__).resolve().parent


def _is_zip_response(content_type: str | None, content_disp: str | None, first_bytes: bytes) -> bool:
    ct = (content_type or "").lower()
    cd = (content_disp or "").lower()
    if first_bytes.startswith(b"PK\x03\x04"):
        return True
    if "application/zip" in ct:
        return True
    # Google Drive often serves downloads as octet-stream with attachment header
    if "application/octet-stream" in ct and "attachment" in cd:
        return True
    return False


def _extract_download_url_from_html(page: str, file_id: str) -> str | None:
    # Unescape HTML and also decode common escaped sequences.
    s = _html.unescape(page)
    try:
        s = s.encode("utf-8", "backslashreplace").decode("unicode_escape")
    except Exception:
        pass

    # Virus-scan / large-file interstitial: a <form> with hidden inputs.
    # Example action: https://drive.usercontent.google.com/download
    form_action = re.search(r'<form[^>]+action="([^"]+)"', s)
    if form_action:
        action = form_action.group(1).replace("&amp;", "&")
        inputs = dict(re.findall(r'<input[^>]+name="([^"]+)"[^>]+value="([^"]*)"', s))
        # Ensure file id is present even if parsing missed it.
        inputs.setdefault("id", file_id)
        params = {k: inputs[k] for k in ("id", "export", "confirm", "uuid") if k in inputs}
        if params and action.startswith("http"):
            return action + "?" + urlencode(params)

    patterns = [
        r'href="(/uc\?export=download[^"]+)"',
        r"(https://drive\.google\.com/uc\?export=download[^\"']+)",
        r"(https://drive\.usercontent\.google\.com/download\?[^\"']+)",
        r"(https://drive\.usercontent\.google\.com/uc\?[^\"']+)",
    ]
    for pat in patterns:
        m = re.search(pat, s)
        if not m:
            continue
        url = m.group(1)
        url = url.replace("&amp;", "&")
        if url.startswith("/"):
            url = "https://drive.google.com" + url
        return url

    # Fallback: confirm token
    m = re.search(r"confirm=([0-9A-Za-z_-]+)", s)
    if m:
        confirm = m.group(1)
        return f"https://drive.google.com/uc?export=download&confirm={confirm}&id={file_id}"

    return None


def _download_gdrive_zip(file_id: str, out_zip: Path) -> None:
    """
    Download a public Google Drive file (zip) handling the confirm interstitial.
    """
    out_zip.parent.mkdir(parents=True, exist_ok=True)

    cj = http.cookiejar.CookieJar()
    opener = request.build_opener(request.HTTPCookieProcessor(cj))
    ua_headers = {"User-Agent": "Mozilla/5.0"}

    def open_url(url: str):
        req = request.Request(url, headers=ua_headers)
        return opener.open(req)

    # Start with the uc?export=download endpoint (works for many public files).
    initial = f"https://drive.google.com/uc?export=download&id={file_id}"

    with open_url(initial) as resp:
        first = resp.read(4)
        if _is_zip_response(resp.headers.get("Content-Type"), resp.headers.get("Content-Disposition"), first):
            with open(out_zip, "wb") as f:
                f.write(first)
                shutil.copyfileobj(resp, f)
            return

        # HTML interstitial: read remainder and extract confirm URL.
        page = (first + resp.read()).decode("utf-8", errors="ignore")
        confirm_url = _extract_download_url_from_html(page, file_id)

    if not confirm_url:
        raise RuntimeError(
            "Failed to obtain Google Drive download URL/confirm token. "
            f"Try opening the share link in a browser to verify access: {SHARE_URL}"
        )

    with open_url(confirm_url) as resp2:
        first2 = resp2.read(4)
        if not _is_zip_response(resp2.headers.get("Content-Type"), resp2.headers.get("Content-Disposition"), first2):
            page2 = (first2 + resp2.read()).decode("utf-8", errors="ignore")
            confirm_url2 = _extract_download_url_from_html(page2, file_id)
            if not confirm_url2:
                raise RuntimeError("Download still returned HTML (not a zip). Network or Drive interstitial changed.")
            with open_url(confirm_url2) as resp3:
                with open(out_zip, "wb") as f:
                    shutil.copyfileobj(resp3, f)
        else:
            with open(out_zip, "wb") as f:
                f.write(first2)
                shutil.copyfileobj(resp2, f)

    # Validate
    try:
        with zipfile.ZipFile(out_zip, "r") as zf:
            zf.testzip()
    except zipfile.BadZipFile as e:
        raise RuntimeError("Downloaded file is not a valid zip. Re-run or check access.") from e


def _extract_csvs(zip_path: Path, dest_dir: Path) -> None:
    tmp_dir = Path(tempfile.mkdtemp(prefix="lab2_dataset_"))
    try:
        with zipfile.ZipFile(zip_path, "r") as zf:
            zf.extractall(tmp_dir)

        # Zip may contain a top-level dataset/ directory.
        src_root = tmp_dir / "dataset" if (tmp_dir / "dataset").is_dir() else tmp_dir

        csvs = list(src_root.rglob("*.csv"))
        if not csvs:
            # Some zips nest deeper; fallback to full temp search.
            csvs = list(tmp_dir.rglob("*.csv"))
        if not csvs:
            raise RuntimeError("No CSV files found after extraction.")

        if dest_dir.exists():
            shutil.rmtree(dest_dir)
        dest_dir.mkdir(parents=True, exist_ok=True)

        for f in csvs:
            shutil.copy2(f, dest_dir / f.name)
    finally:
        shutil.rmtree(tmp_dir, ignore_errors=True)


def ensure_dataset(
    base_dir: str | os.PathLike[str] | None = None,
    dataset_dirname: str = "dataset",
    file_id: str = FILE_ID,
    keep_zip: bool = False,
) -> Path:
    """
    (Re)create ./dataset by downloading and extracting the zip.

    Returns the dataset directory path.
    """
    base = Path(base_dir) if base_dir is not None else _default_base_dir()
    dataset_dir = base / dataset_dirname

    # Always refresh to avoid stale/partial datasets during grading.
    if dataset_dir.exists():
        shutil.rmtree(dataset_dir, ignore_errors=True)

    out_zip = base / "dataset.zip"
    if out_zip.exists() and not keep_zip:
        # Avoid confusing partial downloads.
        out_zip.unlink()
    print(f"Downloading dataset zip from Google Drive (file_id={file_id})...")
    _download_gdrive_zip(file_id=file_id, out_zip=out_zip)

    print("Extracting CSVs into ./dataset/ ...")
    _extract_csvs(zip_path=out_zip, dest_dir=dataset_dir)

    if not keep_zip and out_zip.exists():
        out_zip.unlink()

    print(f"Done. CSVs are in: {dataset_dir}")
    return dataset_dir


def main(argv: list[str]) -> int:
    _ = argv
    ensure_dataset()
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))

