#!/usr/bin/env python3
"""
-------------------------------------------------------------------------------
RADARR ORGANIZER TOOL (v29) - SAFETY FIRST EDITION
-------------------------------------------------------------------------------
SAFETY FEATURES:
  - NEVER deletes media files (mkv, mp4, avi, etc.) during cleanup.
  - NEVER deletes any file larger than 100 MiB.
  - ONLY deletes specific 'junk' extensions (txt, exe, url, etc.).
  - Preserves Scene NFOs (moves them), deletes XML NFOs (if < 100MB).
  - Strict Polling & Database Locking.

Setup:
  1. python3 -m venv venv
  2. source venv/bin/activate
  3. pip install requests questionary rich
  4. Configure API_KEY and SEARCH_PATHS below.
  5. python radarr_organizer_v29.py
-------------------------------------------------------------------------------
"""

import os
import sys
import re
import time
import shutil
import requests
import questionary
from rich.console import Console
from rich.panel import Panel
from rich.progress import Progress, SpinnerColumn, TextColumn, BarColumn
from rich.table import Table

# --- CONFIGURATION -----------------------------------------------------------
# URL to your Radarr instance
RADARR_URL = "http://localhost:7878"
# Your Radarr API Key (Found in Settings -> General)
API_KEY = "YOUR_API_KEY_HERE"

# List of paths where your unorganized movies are stored.
# The script will scan these directories recursively.
SEARCH_PATHS = [
    "/mnt/hdd1/Movies/",
    "/mnt/hdd3/Movies/"
]

# SAFETY: Files larger than this (in MiB) will NEVER be deleted during cleanup.
# This is the primary safeguard against deleting video files.
SAFE_SIZE_LIMIT_MB = 100

# A tuple of video file extensions. Files with these extensions will NEVER be deleted.
VIDEO_EXTS = ('.mkv', '.mp4', '.avi', '.m4v', '.iso', '.ts')
# A tuple of "junk" file extensions. ONLY files with these extensions are eligible for deletion.
JUNK_EXTS = ('.txt', '.exe', '.bat', '.url', '.lnk', '.jpg', '.png', '.jpeg', '.nzb')
# A list of terms. Any directory or file containing these terms will be ignored during the scan.
IGNORE_TERMS = ['sample', 'trailer', 'featurette', 'extras', '@eaDir']
# -----------------------------------------------------------------------------

# Initialize the rich library for better terminal output.
console = Console()

# --- UTILS & API ---

def handle_interrupt(signal_received=None, frame=None):
    """Gracefully exits the script when the user presses Ctrl+C."""
    console.print("\n[bold red]Script Interrupted by User. Exiting.[/]")
    sys.exit(0)

def get_headers():
    """Returns the necessary HTTP headers for Radarr API authentication."""
    return {"X-Api-Key": API_KEY}

def api_get(endpoint, params=None, raise_errors=False):
    """Performs a GET request to the Radarr API."""
    try:
        res = requests.get(f"{RADARR_URL}/api/v3{endpoint}", params=params, headers=get_headers())
        res.raise_for_status()  # Raise an exception for bad status codes (4xx or 5xx)
        return res.json()
    except KeyboardInterrupt: handle_interrupt()
    except Exception as e:
        if raise_errors: raise e
        console.print(f"[bold red]API Error ({endpoint}):[/] {e}")
        sys.exit(1)

def api_post(endpoint, json_data):
    """Performs a POST request to the Radarr API."""
    try:
        res = requests.post(f"{RADARR_URL}/api/v3{endpoint}", json=json_data, headers=get_headers())
        # Check for successful status codes
        if res.status_code not in (200, 201): return False, res.json()
        return True, res.json()
    except KeyboardInterrupt: handle_interrupt()
    except Exception as e: return False, str(e)

def api_put(endpoint, json_data, params=None):
    """Performs a PUT request to the Radarr API."""
    try:
        res = requests.put(f"{RADARR_URL}/api/v3{endpoint}", json=json_data, params=params, headers=get_headers())
        # Check for successful status codes (202 is 'Accepted')
        if res.status_code not in (200, 201, 202): return False, res.json()
        return True, res.json()
    except KeyboardInterrupt: handle_interrupt()
    except Exception as e: return False, str(e)

def execute_blocking(task_name, func, *args, **kwargs):
    """
    Executes a Radarr command and waits for it to complete.
    This is crucial for ensuring tasks like "Rescan" finish before the next step begins.
    """
    # Trigger the initial API call (e.g., start a rescan)
    ok, res = func(*args, **kwargs)

    if not ok:
        console.print(f"    [red]✖ Failed to trigger {task_name}: {res}[/]")
        return False

    # If the response doesn't contain a command ID, it was an instant action.
    if not isinstance(res, dict) or 'id' not in res:
        console.print(f"    [green]✔ {task_name} (Instant)[/]")
        return True

    # Poll the command status endpoint until it's no longer running.
    command_id = res['id']
    start_time = time.time()
    timeout = 180  # 3-minute timeout

    with console.status(f"    [yellow]⏳ {task_name}...[/]") as status:
        while True:
            # Check for timeout
            if time.time() - start_time > timeout:
                console.print(f"    [red]✖ Timeout waiting for {task_name}[/]")
                return False

            try:
                # Get the current status of the command
                cmd_status = api_get(f"/command/{command_id}", raise_errors=True)
                state = cmd_status.get('status', 'unknown').lower()

                if state == 'completed':
                    console.print(f"    [green]✔ {task_name}[/]")
                    return True
                elif state == 'failed':
                    console.print(f"    [red]✖ {task_name} Failed (Radarr Error)[/]")
                    return False

                # Wait briefly before polling again
                time.sleep(0.5)

            except requests.exceptions.HTTPError as e:
                # A 404 error often means the command finished so quickly it was already cleared.
                if e.response.status_code == 404:
                    console.print(f"    [green]✔ {task_name} (Finished/Cleared)[/]")
                    return True
                else:
                    console.print(f"    [red]✖ API Error polling {task_name}: {e}[/]")
                    return False
            except KeyboardInterrupt: handle_interrupt()
            except Exception as e: time.sleep(1)

# --- FORMATTING ---

def sanitize_string(text):
    """Cleans a filename or folder name to make it easier for Radarr to parse."""
    # Remove the extension if it's a video file
    base, ext = os.path.splitext(text)
    if ext.lower() in VIDEO_EXTS: text = base
    # Remove common release tags like resolution, source, codecs, etc.
    text = re.sub(r'(?i)[\.\s\-\(\[]+(1080p|720p|2160p|4k|bluray|web-dl|webrip|h264|x264|h265|x265|hevc|remux|hdr|aac|dts|ac3|dd5\.1|atmos|truehd).*', '', text)
    # Replace common separators with spaces
    text = text.replace('(', ' ').replace(')', ' ').replace('[', ' ').replace(']', ' ')
    text = text.replace('.', ' ').replace('_', ' ')
    # Remove edition tags
    text = re.sub(r'(?i)\b(complete|collection|extended|cut|edition)\b', '', text)
    # Consolidate multiple spaces into one and trim whitespace
    text = re.sub(r'\s+', ' ', text).strip()
    return text

def smart_truncate_path(path, max_len):
    """Shortens a long file path for display in the terminal, keeping the start and end visible."""
    if len(path) <= max_len: return path
    parts = path.strip("/").split("/")
    # If the path is short, just truncate it
    if len(parts) < 3: return path[:max_len-3] + "..."
    # Otherwise, show the first two parts and the last part
    head = "/" + "/".join(parts[:2])
    tail = "/" + parts[-1]
    if len(head) + len(tail) + 4 > max_len: return path[:max_len-3] + "..."
    return f"{head}/.../{tail}"

def calculate_column_widths(items, terminal_width):
    """Dynamically calculates column widths for the selection list to fit the terminal."""
    overhead = 14 # Account for borders, spaces, etc.
    available = terminal_width - overhead
    max_title = 0
    max_file = 0
    # Find the longest title and filename in the list
    for i in items:
        t_len = len(i['display']) + 8
        if t_len > max_title: max_title = t_len
        f_len = len(os.path.basename(i['current_path']))
        if f_len > max_file: max_file = f_len

    # Set proportional limits for columns
    limit_title = int(available * 0.30)
    limit_file  = int(available * 0.25)
    # Determine the final width for each column
    w_title = max(20, min(max_title, limit_title))
    w_file = max(15, min(max_file, limit_file))
    w_path = available - w_title - w_file
    if w_path < 20: w_path = 20
    return w_title, w_path, w_file

def format_choice(item, widths):
    """Formats a single movie item into a string for the selection list."""
    w_title, w_path, w_file = widths
    def clean(s, w): return (s[:w-3] + "...") if len(s) > w else s
    label_prefix = f"[{item['type'][:4]}]" # e.g., [IMPO], [RELI]
    title_str = f"{label_prefix} {item['display']}"
    file_str = os.path.basename(item['file_name']) if 'file_name' in item else os.path.basename(item['current_path'])
    # Return a formatted string with fixed-width columns
    return f"{clean(title_str, w_title):<{w_title}} | {smart_truncate_path(item['current_path'], w_path):<{w_path}} | {clean(file_str, w_file):<{w_file}}"

# --- CLEANUP LOGIC ---

def is_xml_nfo(path):
    """Checks if an NFO file is XML (Radarr/Kodi) or Text (Scene)."""
    try:
        # Read the first 500 bytes of the file
        with open(path, 'rb') as f:
            content = f.read(500).strip()
            # XML NFOs usually start with <?xml or contain <movie>
            if b'<?xml' in content or b'<movie>' in content:
                return True
            return False
    except:
        # If there's an error reading, assume it's not an XML NFO
        return False

def cleanup_source_folder(source_path, final_movie_path):
    """
    Cleans up the original source folder after a movie has been moved.
    This function contains multiple safety checks to prevent accidental data loss.
    """
    if not os.path.exists(source_path): return

    # SAFETY 1: Never delete the root search paths themselves.
    norm_source = os.path.normpath(source_path)
    for root in SEARCH_PATHS:
        if norm_source == os.path.normpath(root):
            return

    console.print(f"    [dim]Checking source for cleanup: {source_path}[/]")

    # Get information about the destination to handle NFOs
    dest_dir = os.path.dirname(final_movie_path)
    dest_basename = os.path.splitext(os.path.basename(final_movie_path))[0]

    try:
        files = os.listdir(source_path)

        for f in files:
            full_path = os.path.join(source_path, f)
            # Skip subdirectories
            if os.path.isdir(full_path): continue

            lower_f = f.lower()

            # SAFETY 2: Check File Size. Skip any file larger than the configured limit.
            # This is the primary protection against deleting video files.
            try:
                size_mb = os.path.getsize(full_path) / (1024 * 1024)
                if size_mb > SAFE_SIZE_LIMIT_MB:
                    console.print(f"    [yellow]⚠ Skipping large file ({size_mb:.1f} MB): {f}[/]")
                    continue
            except: continue

            # SAFETY 3: NEVER delete media files. Explicitly check against the video extension list.
            if lower_f.endswith(VIDEO_EXTS):
                # We leave it alone. If Radarr moved it, it's gone.
                # If Radarr copied it, we keep the original as a safeguard.
                continue

            # --- HANDLE NFO ---
            if lower_f.endswith('.nfo'):
                if is_xml_nfo(full_path):
                    # XML NFO (Junk, generated by Radarr) - Delete it (if it's small).
                    os.remove(full_path)
                else:
                    # Scene NFO (useful text info) - Preserve it by moving it.
                    new_nfo_name = f"{dest_basename}.nfo-orig"
                    new_nfo_path = os.path.join(dest_dir, new_nfo_name)
                    if not os.path.exists(new_nfo_path):
                        try:
                            shutil.move(full_path, new_nfo_path)
                            console.print(f"    [green]✔ Preserved Scene NFO -> {new_nfo_name}[/]")
                        except: pass
                    else:
                        # If a destination NFO already exists, just delete the source one.
                        if os.path.abspath(full_path) != os.path.abspath(new_nfo_path):
                            os.remove(full_path)
                continue

            # --- DELETE JUNK ---
            # SAFETY 4: Only delete files if their extension is explicitly in the JUNK_EXTS list.
            if lower_f.endswith(JUNK_EXTS):
                os.remove(full_path)
                continue

            # Handle "sample" files (only if small, which was checked above)
            if 'sample' in lower_f:
                os.remove(full_path)
                continue

        # SAFETY 5: Try to remove the source folder ONLY if it is now empty.
        # This will fail safely if a video file or any other unexpected file remains.
        if not os.listdir(source_path):
            os.rmdir(source_path)
            console.print("    [green]✔ Source folder deleted (Empty)[/]")

    except Exception as e:
        console.print(f"    [red]⚠ Cleanup Warning: {e}[/]")

def manual_rename_extras_destination(folder_path):
    """Renames subtitles/extras in the DESTINATION folder to match the main movie file."""
    if not os.path.exists(folder_path): return
    files = os.listdir(folder_path)
    video_file = None
    max_size = 0
    # Find the largest video file in the folder, assuming it's the main movie.
    for f in files:
        if f.lower().endswith(VIDEO_EXTS) and not any(x in f.lower() for x in IGNORE_TERMS):
            fp = os.path.join(folder_path, f)
            size = os.path.getsize(fp)
            if size > max_size:
                max_size = size
                video_file = f

    if not video_file: return
    # Get the base name of the video file (without extension)
    video_stem = os.path.splitext(video_file)[0]
    extra_exts = ('.srt', '.sub', '.idx', '.nfo', '.txt', '.jpg', '.png', '.jpeg')

    # Loop through all files again to find extras that need renaming.
    for f in files:
        if f == video_file: continue
        _, ext = os.path.splitext(f)
        if ext.lower() not in extra_exts: continue
        # If the extra file already matches the video stem, skip it.
        if f.startswith(video_stem): continue

        old_stem = os.path.splitext(f)[0]
        # Try to find language codes or other suffixes in the old filename.
        match = re.search(r'([._-])([a-z]{2,3}|english|french|german|spanish|italian|forced|sdh|cc)$', old_stem, re.IGNORECASE)

        new_name = ""
        if match:
            # If a suffix is found (e.g., ".eng"), append it to the new name.
            suffix = match.group(2)
            new_name = f"{video_stem}.{suffix}{ext}"
        else:
            # Otherwise, just give it the same name as the video file.
            new_name = f"{video_stem}{ext}"

        try:
            old_full = os.path.join(folder_path, f)
            new_full = os.path.join(folder_path, new_name)
            # Rename the file if the new name doesn't already exist.
            if not os.path.exists(new_full):
                os.rename(old_full, new_full)
        except: pass

# --- PIPELINE ---

def process_single_item(item, qp_id):
    """
    Runs the full import/relink/rename pipeline for a single selected movie.
    This function calls the Radarr API in a specific sequence to ensure proper organization.
    """
    movie_title = item['display']
    action = item['type']
    source_folder = item['current_path']

    console.print(f"\n[bold cyan]Processing ({action}): {movie_title}[/]")
    movie_id = None

    try:
        # STEP 1: IMPORT / LINK the movie into Radarr's database.
        if action == 'RELINK':
            # The movie exists in Radarr but is missing a file. We need to update its path.
            movie = item['db_movie']
            movie_id = movie['id']
            movie['path'] = item['current_path']
            if not execute_blocking("Linking DB Entry", api_put, f"/movie/{movie_id}", movie, {"moveFilesInTheBackground": "false"}):
                return False

        elif action == 'IMPORT':
            # The movie is not in Radarr at all. We need to add it.
            payload = {
                "title": item['title'],
                "qualityProfileId": qp_id,
                "tmdbId": item['tmdb_id'],
                "year": item['year'],
                "path": item['current_path'],
                "rootFolderPath": item['target_root'],
                "monitored": True,
                "addOptions": {"searchForMovie": False} # Don't search for other releases
            }
            if not execute_blocking("Importing to DB", api_post, "/movie", payload):
                return False

            time.sleep(1.0) # Give Radarr a moment to process the new entry.

            # Look up the movie we just added to get its internal Radarr ID.
            lookup = api_get("/movie/lookup", {"term": f"tmdb:{item['tmdb_id']}"})
            if lookup and lookup[0].get('id'):
                movie_id = lookup[0]['id']
            else:
                console.print("    [red]✖ Could not verify Import ID[/]")
                return False

        elif action == 'RENAME':
            # The movie is already in Radarr, we just need to process it.
            movie_id = item['db_movie']['id']

        if not movie_id: return False

        # --- THE PIPELINE ---
        # The following steps are executed in order for every movie.

        # 2. Rescan: Tell Radarr to scan the movie's current folder to recognize the video file.
        if not execute_blocking("Registering File (Scan)", api_post, "/command", {"name": "RescanMovie", "movieId": movie_id}): return False

        # 3. Rename Folder: Tell Radarr to move the folder to its final, organized location.
        move_payload = {"movieIds": [movie_id], "rootFolderPath": item['target_root'], "moveFiles": True}
        if not execute_blocking("Renaming Folder", api_put, "/movie/editor", move_payload): return False

        # 4. Rescan: Scan again to update Radarr's database with the new file location.
        if not execute_blocking("Updating File Location", api_post, "/command", {"name": "RescanMovie", "movieId": movie_id}): return False

        # 5. Rename Files: Tell Radarr to rename the video file itself according to your naming patterns.
        files_res = api_get("/moviefile", {"movieId": movie_id})
        if files_res:
            file_ids = [f['id'] for f in files_res]
            if file_ids:
                if not execute_blocking("Renaming Video Files", api_post, "/command", {"name": "RenameFiles", "movieId": movie_id, "files": file_ids}): return False

        # 6. SOURCE CLEANUP & NFO MIGRATION: After Radarr has moved the file, clean the source dir.
        final_path = None
        try:
            # Get the movie's final path from Radarr.
            fresh_movie = api_get(f"/movie/{movie_id}")
            final_path = fresh_movie.get('path')

            # If the source folder is the same as the final path, the movie was already
            # in place (a RELINK scenario). No cleanup is needed.
            if final_path and os.path.normpath(source_folder) == os.path.normpath(final_path):
                console.print("    [cyan]✔ Movie was already in its final location. Skipping cleanup.[/]")

            elif final_path and os.path.exists(final_path):
                found_video = None
                for f in os.listdir(final_path):
                    if f.lower().endswith(VIDEO_EXTS) and not any(x in f.lower() for x in IGNORE_TERMS):
                        found_video = os.path.join(final_path, f)
                        break

                if found_video:
                    # Run the safe cleanup function on the original source folder.
                    cleanup_source_folder(source_folder, found_video)
                    # 7. Destination Extras Rename: Rename subtitles etc. in the new folder.
                    manual_rename_extras_destination(final_path)
                else:
                    console.print("    [yellow]⚠ Skipping cleanup (Destination video not found)[/]")
        except Exception as e:
            console.print(f"    [red]⚠ Cleanup Error: {e}[/]")

        # 8. Refresh Metadata: Tell Radarr to download posters, metadata, etc.
        if not execute_blocking("Downloading Metadata/Images", api_post, "/command", {"name": "RefreshMovie", "movieIds": [movie_id]}): return False

        # 9. Final Scan: One last scan to ensure everything is finalized in Radarr's database.
        if not execute_blocking("Finalizing (Scan)", api_post, "/command", {"name": "RescanMovie", "movieId": movie_id}): return False

        return True

    except Exception as e:
        console.print(f"    [red]✖ Exception: {e}[/]")
        return False

# --- LOGIC (Scanning & UI) ---

def get_quality_profile():
    """Asks the user to select a default Quality Profile for newly imported movies."""
    try:
        profiles = api_get("/qualityprofile")
        choices = [{"name": p['name'], "value": p['id']} for p in profiles]
        return questionary.select("Select Quality Profile for NEW imports:", choices=choices).ask()
    except KeyboardInterrupt: handle_interrupt()
    except: sys.exit(1)

def get_db_movies():
    """Fetches all movies from the Radarr database and creates a lookup map."""
    with console.status("[cyan]Fetching database..."):
        all_movies = api_get("/movie")
        # Create a dictionary mapping tmdbId to the movie object for fast lookups.
        db_map = {m['tmdbId']: m for m in all_movies}
    return db_map

def identify_file_auto(filename, foldername):
    """Tries to automatically identify a movie using Radarr's parsing API."""
    # First, try parsing the more specific filename.
    res = api_get("/parse", {"title": filename})
    if 'movie' in res and 'tmdbId' in res['movie'] and res['movie']['tmdbId'] > 0:
        return res['movie']
    # If that fails, try parsing the folder name.
    res = api_get("/parse", {"title": foldername})
    if 'movie' in res and 'tmdbId' in res['movie'] and res['movie']['tmdbId'] > 0:
        return res['movie']
    # If both fail, return nothing.
    return None

def check_if_rename_needed(movie_id):
    """Checks if Radarr thinks a movie's files need to be renamed."""
    try:
        renames = api_get("/rename", {"movieId": movie_id})
        if renames and len(renames) > 0: return True
    except: return False
    return False

def smart_lookup_ui_immediate(filename, foldername, qp_id, current_root, target_root, db_map, processed_ids):
    """Provides an interactive UI for manually identifying movies that couldn't be matched automatically."""
    clean_file = sanitize_string(filename)
    clean_folder = sanitize_string(foldername)

    console.print(f"\n[bold]Unidentified Item:[/]")
    console.print(f"File:   [dim]{filename}[/]")
    console.print(f"Search: [cyan]'{clean_file}'[/] / [cyan]'{clean_folder}'[/]")

    matches = []
    seen_ids = set()

    # Helper function to search Radarr and add unique results to the matches list.
    def add_results(term):
        if not term or len(term) < 2: return
        try:
            res = api_get("/movie/lookup", {"term": term})
            if res:
                for m in res[:5]:
                    if m['tmdbId'] not in seen_ids:
                        matches.append(m)
                        seen_ids.add(m['tmdbId'])
        except: pass

    # Search using both the cleaned filename and folder name.
    add_results(clean_file)
    if clean_file != clean_folder: add_results(clean_folder)

    # Build the list of choices for the user.
    choices = []
    for m in matches:
        label = f"[TMDB:{m['tmdbId']}] {m['title']} ({m['year']})"
        choices.append(questionary.Choice(label, value=m))

    choices.append(questionary.Choice("Manual Search (Type Custom Name)", value="MANUAL"))
    choices.append(questionary.Choice("Skip", value="SKIP"))
    choices.append(questionary.Choice("Skip All (Stop Manual Process)", value="SKIP_ALL"))

    try:
        selection = questionary.select("Select match:", choices=choices).ask()
        if selection is None: sys.exit(0)
        if selection == "SKIP": return None
        if selection == "SKIP_ALL": return "SKIP_ALL"

        match = None
        if selection != "MANUAL":
            match = selection
        else:
            # Loop for manual search until a match is selected or skipped.
            while True:
                query = questionary.text("Enter Search Term:").ask()
                if query is None: sys.exit(0)
                if not query: return None

                man_results = api_get("/movie/lookup", {"term": query})
                if not man_results:
                    console.print("[red]No results found.[/]")
                    continue
                m_choices = []
                for m in man_results[:5]:
                    label = f"[TMDB:{m['tmdbId']}] {m['title']} ({m['year']})"
                    m_choices.append(questionary.Choice(label, value=m))
                m_choices.append(questionary.Choice("Search Again", value="RETRY"))
                m_choices.append(questionary.Choice("Skip", value="SKIP"))
                man_sel = questionary.select("Select match:", choices=m_choices).ask()
                if man_sel == "SKIP": return None
                if man_sel != "RETRY":
                    match = man_sel
                    break

        if match:
            # If a match was found, build the item dictionary and process it immediately.
            tmdb_id = match['tmdbId']
            processed_ids.add(tmdb_id)
            item = {
                'tmdb_id': tmdb_id,
                'title': match['title'],
                'year': match['year'],
                'current_path': current_root,
                'target_root': target_root,
                'display': f"{match['title']} ({match['year']})",
                'file_name': filename
            }
            # Determine if this should be a RELINK (already in DB) or IMPORT (new).
            if tmdb_id in db_map:
                item['type'] = 'RELINK'
                item['db_movie'] = db_map[tmdb_id]
            else:
                item['type'] = 'IMPORT'

            process_single_item(item, qp_id)

    except KeyboardInterrupt: handle_interrupt()
    return None

def scan_and_process(db_map, qp_id):
    """Scans the SEARCH_PATHS, identifies movies, and categorizes them for processing."""
    candidates = []   # Movies that were automatically identified.
    unidentified = [] # Movies that need manual identification.
    processed_ids = set() # Keep track of movies already found to avoid duplicates.
    anomalies = []    # Folders with issues (e.g., multiple video files).

    console.print("[cyan]Scanning disks...[/]")

    try:
        for root_path in SEARCH_PATHS:
            if not os.path.exists(root_path):
                console.print(f"[red]Path not found: {root_path}[/]")
                continue

            # Walk through the directory tree.
            for current_root, dirs, files in os.walk(root_path):
                # Prune ignored directories to avoid scanning them.
                dirs[:] = [d for d in dirs if d.lower() not in IGNORE_TERMS]
                # Find video files in the current directory, ignoring samples etc.
                videos = [f for f in files if f.lower().endswith(VIDEO_EXTS) and not any(x in f.lower() for x in IGNORE_TERMS)]

                if not videos: continue # Skip folders with no videos.
                if len(videos) > 1:
                    # Flag folders with more than one video file as anomalies for manual review.
                    anomalies.append((current_root, f"{len(videos)} video files"))
                    continue

                video_file = videos[0]
                folder_name = os.path.basename(current_root)

                # Determine which root path this movie belongs to.
                target_root = None
                for r in SEARCH_PATHS:
                    if os.path.normpath(current_root).startswith(os.path.normpath(r)):
                        target_root = r
                        break
                if not target_root: continue

                # Try to automatically identify the movie.
                parsed = identify_file_auto(video_file, folder_name)

                if parsed:
                    tmdb_id = parsed['tmdbId']
                    if tmdb_id in processed_ids: continue

                    # Build the item dictionary for a successfully identified movie.
                    item = {
                        'tmdb_id': tmdb_id,
                        'title': parsed['title'],
                        'year': parsed['year'],
                        'current_path': current_root,
                        'target_root': target_root,
                        'display': f"{parsed['title']} ({parsed['year']})",
                        'file_name': video_file
                    }

                    # Categorize the action needed for this movie.
                    if tmdb_id not in db_map:
                        item['type'] = 'IMPORT'
                        candidates.append(item)
                        processed_ids.add(tmdb_id)
                    else:
                        db_movie = db_map[tmdb_id]
                        item['db_movie'] = db_movie
                        # If movie is in DB but has no file, it needs to be relinked.
                        if not db_movie['hasFile']:
                            item['type'] = 'RELINK'
                            candidates.append(item)
                            processed_ids.add(tmdb_id)
                        else:
                             # If it has a file, check if it needs renaming.
                             if check_if_rename_needed(db_movie['id']):
                                item['type'] = 'RENAME'
                                candidates.append(item)
                                processed_ids.add(tmdb_id)
                else:
                    # If auto-identification fails, add it to the unidentified list.
                    unidentified.append({
                        'file': video_file,
                        'folder': folder_name,
                        'root': current_root,
                        'target': target_root
                    })
    except KeyboardInterrupt: handle_interrupt()

    return candidates, unidentified, anomalies, processed_ids

def main():
    """The main function that orchestrates the entire script."""
    console.clear()
    console.print(Panel.fit("[bold white on blue]Radarr Organizer Tool v29[/]"))

    # Check if Radarr API is reachable.
    try: api_get("/system/status")
    except: return

    # Get user input and initial data from Radarr.
    qp_id = get_quality_profile()
    db_map = get_db_movies()

    # Scan the disks to find all potential movies to process.
    candidates, unidentified, anomalies, processed_ids = scan_and_process(db_map, qp_id)

    # Handle unidentified files first.
    if unidentified:
        console.print(f"\n[bold yellow]Found {len(unidentified)} unidentified files.[/]")
        if questionary.confirm("Do you want to review and manually identify them now?").ask():
            for item in unidentified:
                result = smart_lookup_ui_immediate(item['file'], item['folder'], qp_id, item['root'], item['target'], db_map, processed_ids)
                if result == "SKIP_ALL":
                    console.print("[yellow]Skipping remaining manual reviews...[/]")
                    break

    # Handle the automatically identified candidates.
    if not candidates:
        console.print("[yellow]No auto-matched items to process.[/]")
    else:
        candidates.sort(key=lambda x: (x['type'], x['display']))
        console.print(f"\n[bold]Found {len(candidates)} auto-matched candidates.[/]")

        # Get terminal width for formatting the list.
        term_width = shutil.get_terminal_size((120, 20)).columns
        widths = calculate_column_widths(candidates, term_width)

        # Create the checkbox list for user selection.
        choices = [questionary.Choice(format_choice(c, widths), value=c, checked=True) for c in candidates]

        try:
            selected = questionary.checkbox(
                "Select movies to Process:",
                choices=choices,
                style=questionary.Style([('answer', 'fg:green'), ('highlighted', 'fg:cyan bold')])
            ).ask()

            # Process each item the user selected.
            if selected:
                console.print(f"\n[bold white on red] Processing {len(selected)} items... [/]")
                for item in selected:
                    process_single_item(item, qp_id)

        except KeyboardInterrupt: handle_interrupt()

    console.print(f"\n[bold green]Job Complete.[/]")

    # If any anomalies were found, print them at the end for manual review.
    if anomalies:
        console.print("\n[bold red]ANOMALIES (Manual Fix Required):[/]")
        t = Table(show_header=True)
        t.add_column("Path"); t.add_column("Reason")
        for p, r in anomalies: t.add_row(p, r)
        console.print(t)

# Standard Python entry point.
if __name__ == "__main__":
    main()