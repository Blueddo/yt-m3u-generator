#!/usr/bin/env python3
"""
Δημιουργεί αρχείο M3U από YouTube URLs χρησιμοποιώντας yt-dlp.

Ρυθμίσεις (όπως ποιότητα, timeout, retries, no_metadata κ.λπ.)
φορτώνονται από αρχείο JSON (προεπιλογή: settings.json).

Όλα τα μηνύματα/σχόλια είναι στα Ελληνικά.
"""
from __future__ import annotations
import argparse
import concurrent.futures
import subprocess
import sys
import os
import re
import json
import time
from datetime import datetime, timedelta, timezone
from typing import Optional, List, Set, Tuple, Dict
from tqdm import tqdm
from termcolor import colored

# Προσπάθεια υποστήριξης ζώνης ώρας (αν υπάρχει)
try:
    from zoneinfo import ZoneInfo
    ATHENS_TZ = ZoneInfo("Europe/Athens")
    UTC_TZ = ZoneInfo("UTC")
except Exception:
    ATHENS_TZ = timezone.utc
    UTC_TZ = timezone.utc

EXPIRE_RE = re.compile(r'expire/(\d+)', re.IGNORECASE)

# ---------- helpers ----------
def load_json_config(path: str) -> Dict:
    """
    Φορτώνει ρυθμίσεις από JSON.
    Αν το αρχείο δεν υπάρχει, επιστρέφει κενό dict.
    """
    try:
        with open(path, "r", encoding="utf-8") as f:
            cfg = json.load(f)
            return cfg if isinstance(cfg, dict) else {}
    except FileNotFoundError:
        print(colored(f"Το αρχείο ρυθμίσεων '{path}' δεν βρέθηκε. Χρήση προεπιλογών.", "yellow"))
        return {}
    except json.JSONDecodeError as e:
        print(colored(f"Σφάλμα ανάγνωσης JSON ({path}): {e}. Χρήση προεπιλογών.", "red"))
        return {}

def safe_attr(s: Optional[str]) -> str:
    return (s or "").replace('"', '%22')

def normalize_input_line(line: str) -> str:
    line = line.strip()
    if not line:
        return ""
    if not (line.startswith("http://") or line.startswith("https://")):
        return f"https://www.youtube.com/watch?v={line}"
    return line

def run_cmd(cmd: List[str], timeout: int) -> subprocess.CompletedProcess:
    return subprocess.run(cmd, capture_output=True, text=True, timeout=timeout)

def run_yt_dlp_cmd(args_list: List[str], cookies: Optional[str], cookies_from_browser: Optional[str], timeout: int) -> subprocess.CompletedProcess:
    cmd = ["yt-dlp"]
    if cookies:
        cmd += ["--cookies", cookies]
    elif cookies_from_browser:
        cmd += ["--cookies-from-browser", cookies_from_browser]
    cmd += args_list
    return run_cmd(cmd, timeout)

def run_yt_dlp_cmd_with_retries(args_list: List[str], cookies: Optional[str], cookies_from_browser: Optional[str], timeout: int, retries: int = 2, backoff: float = 1.5):
    attempt = 0
    while True:
        try:
            return run_yt_dlp_cmd(args_list, cookies, cookies_from_browser, timeout)
        except subprocess.TimeoutExpired:
            attempt += 1
            if attempt > retries:
                raise
            time.sleep(backoff * attempt)
        except Exception:
            attempt += 1
            if attempt > retries:
                raise
            time.sleep(backoff * attempt)

def first_http_line(text: str) -> Optional[str]:
    if not text:
        return None
    for ln in text.splitlines():
        l = ln.strip()
        if l.startswith("http://") or l.startswith("https://"):
            return l
    return None

def is_manifest_url(url: str) -> bool:
    if not url:
        return False
    u = url.lower()
    return (".m3u8" in u) or ("manifest" in u) or ("hls_playlist" in u)

def human_readable_delta_greek_full(delta: timedelta) -> str:
    total = int(delta.total_seconds())
    if total <= 0:
        return "λήγει τώρα"
    hours = total // 3600
    rem = total % 3600
    minutes = rem // 60
    seconds = rem % 60
    parts = []
    if hours:
        parts.append(f"{hours} {'ώρα' if hours==1 else 'ώρες'}")
    if minutes:
        parts.append(f"{minutes} {'λεπτό' if minutes==1 else 'λεπτά'}")
    if seconds:
        parts.append(f"{seconds} {'δευτερόλεπτο' if seconds==1 else 'δευτερόλεπτα'}")
    return "σε " + " και ".join(parts)

# ---------- metadata helpers ----------
def get_minimal_meta(url: str, timeout: int, cookies: Optional[str], cookies_from_browser: Optional[str], retries: int) -> Tuple[Optional[str], Optional[str], Optional[str]]:
    """
    Προσπαθεί να πάρει μόνο id,title,thumbnail με --print (γρηγορότερη μέθοδος).
    Επιστρέφει (meta_id, title, thumbnail) ή (None,None,None).
    """
    fmt = "%(id)s\t%(title)s\t%(thumbnail)s"
    try:
        proc = run_yt_dlp_cmd_with_retries(["--print", fmt, url], cookies, cookies_from_browser, timeout, retries=retries)
        out = (proc.stdout or proc.stderr or "").strip()
        if not out:
            return (None, None, None)
        first = out.splitlines()[0]
        parts = first.split("\t")
        meta_id = parts[0].strip() if len(parts) >= 1 and parts[0].strip() else None
        title = parts[1].strip() if len(parts) >= 2 and parts[1].strip() else None
        thumbnail = parts[2].strip() if len(parts) >= 3 and parts[2].strip() else None
        return (meta_id, title, thumbnail)
    except Exception:
        return (None, None, None)

def get_stream_url_with_ytdlp(url: str, fmt: str, timeout: int, cookies: Optional[str], cookies_from_browser: Optional[str], retries: int = 2) -> Optional[str]:
    """
    Προσπαθεί με διάφορες στρατηγικές να πάρει direct stream (manifest) ή άλλο url.
    Επιστρέφει την πρώτη URL που βρει.
    """
    try:
        proc = run_yt_dlp_cmd_with_retries(["-g", url], cookies, cookies_from_browser, timeout, retries=retries)
        out = (proc.stdout or proc.stderr or "").strip()
        m = first_http_line(out)
        if m:
            return m
    except Exception:
        pass
    try:
        if fmt:
            proc = run_yt_dlp_cmd_with_retries(["-f", fmt, "-g", url], cookies, cookies_from_browser, timeout, retries=retries)
            out = (proc.stdout or proc.stderr or "").strip()
            m = first_http_line(out)
            if m:
                return m
    except Exception:
        pass
    # δοκιμές fallback / JSON
    try:
        proc = run_yt_dlp_cmd_with_retries(["-j", url], cookies, cookies_from_browser, timeout, retries=retries)
        out = (proc.stdout or proc.stderr or "").strip()
        if out:
            parsed = json.loads(out)
            if isinstance(parsed, dict):
                # προτίμηση σε manifest/url fields
                if parsed.get("url"):
                    return parsed.get("url")
                req = parsed.get("requested_formats")
                if isinstance(req, list) and req:
                    for f in req:
                        if f.get("url"):
                            return f.get("url")
                fmts = parsed.get("formats") or []
                # προτίμησε m3u8
                for f in fmts:
                    urlf = f.get("url")
                    if urlf and ('.m3u8' in (f.get("ext") or '') or 'm3u8' in (f.get("protocol") or '')):
                        return urlf
                for f in fmts:
                    if f.get("url"):
                        return f.get("url")
    except Exception:
        pass
    return None

# ---------- EXTINF builders ----------
def build_entry_extinf(meta_id: Optional[str], thumbnail: Optional[str], title: str) -> str:
    attrs = []
    if meta_id:
        attrs.append(f'tvg-id="{meta_id}"')
    if thumbnail:
        attrs.append(f'tvg-logo="{safe_attr(thumbnail)}"')
    attrs_str = (" " + " ".join(attrs)) if attrs else ""
    title_safe = title.replace("\n", " ").strip()
    return f'#EXTINF:-1{attrs_str},{title_safe}\n'

def build_special_extinf_only_expire(expire_str: str, rel_str: str, special_meta_id: Optional[str], special_thumbnail: Optional[str]) -> str:
    title = f"Λήξη συνδέσμων: {expire_str} ({rel_str})" if expire_str else "Λήξη συνδέσμων: μη διαθέσιμη"
    attrs = []
    if special_meta_id:
        attrs.append(f'tvg-id="{special_meta_id}"')
    if special_thumbnail:
        attrs.append(f'tvg-logo="{safe_attr(special_thumbnail)}"')
    attrs_str = (" " + " ".join(attrs)) if attrs else ""
    title_safe = title.replace("\n", " ").strip()
    return f'#EXTINF:-1{attrs_str},{title_safe}\n'

# ---------- buffered worker ----------
def check_url_entry_buffered(raw_line: str, fmt: str, timeout: int, cookies: Optional[str], cookies_from_browser: Optional[str], fallback_watch: bool, no_metadata: bool, full_metadata: bool, retries: int):
    line = raw_line.strip()
    if not line:
        return (line, None, None, None, None, None)
    watch_url = normalize_input_line(line)
    title = line
    thumbnail = None
    meta_id = None
    if not no_metadata:
        meta_id_m, title_m, thumbnail_m = get_minimal_meta(watch_url, timeout, cookies, cookies_from_browser, retries)
        if meta_id_m or title_m or thumbnail_m:
            meta_id = meta_id_m or meta_id
            title = title_m or title
            thumbnail = thumbnail_m or thumbnail
    try:
        stream_url = get_stream_url_with_ytdlp(watch_url, fmt, timeout, cookies, cookies_from_browser, retries=retries)
    except Exception:
        stream_url = None
    if not stream_url and fallback_watch:
        stream_url = watch_url
    expire_epoch = None
    if stream_url:
        m = EXPIRE_RE.search(stream_url)
        if m:
            try:
                expire_epoch = int(m.group(1))
            except Exception:
                expire_epoch = None
    return (line, meta_id, title, thumbnail, stream_url, expire_epoch)

# ---------- header ----------
def write_header(path: str, created_dt_local: datetime):
    with open(path, "w", encoding="utf-8") as f:
        f.write('#EXTM3U $BorpasFileFormat="1" $NestedGroupsSeparator="/"\n')
        f.write(f"# Δημιουργήθηκε στις {created_dt_local.strftime('%d-%m-%Y %H:%M:%S')}\n")

# ---------- main ----------
def main():
    parser = argparse.ArgumentParser(description="Generate M3U entries from YouTube URLs using yt-dlp.")
    parser.add_argument("-i", "--input", default="youtube_urls.txt", help="Αρχείο εισόδου (URLs/IDs).")
    parser.add_argument("-o", "--output", default="youtube_streams.m3u", help="Αρχείο εξόδου M3U.")
    parser.add_argument("-c", "--config", default="settings_youtube.json", help="Αρχείο ρυθμίσεων JSON.")
    parser.add_argument("--timestamp", action="store_true", help="Πρόσθεσε timestamp στο όνομα αρχείου εξόδου.")
    args = parser.parse_args()

    # Φορτώνουμε config JSON και εφαρμόζουμε προεπιλογές
    cfg = load_json_config(args.config)
    workers = int(cfg.get("workers", 6))
    timeout = int(cfg.get("timeout", 60))
    retries = int(cfg.get("retries", 2))
    no_metadata = bool(cfg.get("no_metadata", False))
    full_metadata = bool(cfg.get("full_metadata", False))
    fallback_watch = bool(cfg.get("fallback_watch_url", True))
    dedupe_by = cfg.get("dedupe_by", "id")
    video_quality = cfg.get("video_quality", "worst")
    audio_quality = cfg.get("audio_quality", "best")
    quality_custom_format = cfg.get("quality_custom_format", "")
    favicon_service = cfg.get("favicon_service", "https://www.google.com/s2/favicons?domain_url=https://www.youtube.com&sz=128")

    # Δημιουργία format string από config
    if video_quality != "custom" and audio_quality != "custom":
        fmt = f"{video_quality}video+{audio_quality}audio/best"
    elif video_quality == "custom" or audio_quality == "custom":
        fmt = quality_custom_format or "worstvideo+bestaudio/best"
    else:
        fmt = "worstvideo+bestaudio/best"

    # Φόρτωση εισόδου και αφαίρεση διπλοτύπων στην είσοδο
    try:
        with open(args.input, "r", encoding="utf-8") as f:
            raw_lines = [ln.strip() for ln in f if ln.strip()]
    except FileNotFoundError:
        print(colored(f"Το αρχείο {args.input} δεν βρέθηκε.", "red"), file=sys.stderr)
        sys.exit(1)

    unique_inputs: List[str] = []
    seen_input_lines: Set[str] = set()
    for ln in raw_lines:
        key = ln.strip().lower()
        if not key:
            continue
        if key in seen_input_lines:
            print(colored(f"Παρέλειψη διπλότυπης γραμμής εισόδου: {ln}", "yellow"))
            continue
        seen_input_lines.add(key)
        unique_inputs.append(ln)

    # timestamp στο όνομα αν ζητηθεί
    if args.timestamp:
        base, ext = os.path.splitext(args.output)
        args.output = f"{base}_{datetime.now().strftime('%Y%m%d_%H%M%S')}{ext or '.m3u'}"

    # χρόνος δημιουργίας (τοπική ώρα Αθήνας αν υπάρχει)
    try:
        if isinstance(ATHENS_TZ, timezone):
            created_dt_local = datetime.now(timezone.utc).astimezone(ATHENS_TZ)
        else:
            created_dt_local = datetime.now()
    except Exception:
        created_dt_local = datetime.now()

    # Εκτέλεση ελέγχων και buffering
    buffered: List[Tuple[str, Optional[str], Optional[str], Optional[str], Optional[str], Optional[int]]] = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=workers) as executor:
        futures = {
            executor.submit(
                check_url_entry_buffered,
                line,
                fmt,
                timeout,
                cfg.get("cookies"),
                cfg.get("cookies_from_browser"),
                fallback_watch,
                no_metadata,
                full_metadata,
                retries
            ): line for line in unique_inputs
        }
        for fut in tqdm(concurrent.futures.as_completed(futures), total=len(futures), desc="Έλεγχος YouTube URLs", ncols=120):
            try:
                res = fut.result()
                buffered.append(res)
                tqdm.write(str(f"Έλεγχος: {res[0]} - ολοκληρώθηκε"))
            except Exception as e:
                tqdm.write(colored(f"Σφάλμα σε future: {e}", "red"))

    # υπολογισμός max expiry
    expire_epochs = [r[5] for r in buffered if r[5]]
    expire_max = max(expire_epochs) if expire_epochs else None

    # γράψιμο header
    write_header(args.output, created_dt_local)

    # ειδικό favicon (πάντα Google s2 PNG για καλύτερη συμβατότητα)
    special_thumbnail = favicon_service
    special_meta_id = "info"

    # special URL: προτεραιότητα direct manifest, αλλιώς watch URL πρώτης εισόδου
    first_manifest = next((r[4] for r in buffered if r[4] and is_manifest_url(r[4])), None)
    first_watch = normalize_input_line(unique_inputs[0]) if unique_inputs else None
    special_url = first_manifest or first_watch

    # ετοιμασία expiry strings
    if expire_max:
        try:
            expire_dt_utc = datetime.fromtimestamp(expire_max, tz=timezone.utc)
            if isinstance(ATHENS_TZ, timezone):
                expire_dt_local = expire_dt_utc.astimezone(ATHENS_TZ)
                now_local = datetime.now(timezone.utc).astimezone(ATHENS_TZ)
            else:
                expire_dt_local = expire_dt_utc.astimezone(ATHENS_TZ)
                now_local = datetime.now().astimezone(ATHENS_TZ)
            expire_str = expire_dt_local.strftime('%d-%m-%Y %H:%M:%S')
            rel_str = human_readable_delta_greek_full(expire_dt_local - now_local)
        except Exception:
            expire_str = "μη διαθέσιμη"
            rel_str = "μη διαθέσιμη"
    else:
        expire_str = "μη διαθέσιμη"
        rel_str = "μη διαθέσιμη"

    special_extinf = build_special_extinf_only_expire(expire_str, rel_str, special_meta_id, special_thumbnail)

    # Γράψιμο εξόδου: ειδική εγγραφή (πρώτη) + υπόλοιπες (παράλειψη του URL που χρησιμοποιήθηκε ως special)
    with open(args.output, "a", encoding="utf-8") as out_f:
        out_f.write(special_extinf)
        if special_url:
            out_f.write(f"{special_url}\n")

        added_keys: Set[str] = set()
        added_urls: Set[str] = set()
        for (input_line, meta_id, title, thumbnail, stream_url, expire_epoch) in buffered:
            if not stream_url:
                continue
            # αν ειδική χρησιμοποίησε την ίδια URL, παράλειψη για να μην έχει duplicate
            if special_url and stream_url == special_url:
                continue
            item_logo = thumbnail or favicon_service
            key_norm = (meta_id or title or "").strip().lower()
            if (stream_url and stream_url in added_urls) or (key_norm and key_norm in added_keys):
                tqdm.write(colored(f"Παρέλειψη duplicate (εντός run) για {input_line}", "yellow"))
                continue
            extinf = build_entry_extinf(meta_id, item_logo, title or input_line)
            out_f.write(extinf)
            out_f.write(f"{stream_url}\n")
            out_f.flush()
            try:
                os.fsync(out_f.fileno())
            except Exception:
                pass
            if stream_url:
                added_urls.add(stream_url)
            if key_norm:
                added_keys.add(key_norm)
            tqdm.write(colored(f"Γράφηκε entry για {input_line} -> {args.output}", "green"))

    print(colored(f"Ολοκληρώθηκε. Το αρχείο εξόδου: {args.output}", "cyan"))

if __name__ == "__main__":
    main()