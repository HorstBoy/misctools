#!/usr/bin/env python3
"""
TV Library Consolidator
=======================

This script scans multiple root directories (e.g., different hard drives) for TV series
and consolidates each series onto a single drive.

It is designed for media server setups (like Plex/Jellyfin) where a single TV show
might be split across multiple mount points due to storage expansion.

Logic:
1. Scans provided root paths for folders (TV Series).
2. Groups series by name (normalizing case/punctuation).
3. Identifies episodes (SxxExx format) and 'artifacts' (images, nfos, subtitles).
4. Determines the 'Target Disk' for each series:
   - The disk that already holds the most data for that series.
   - Must have enough free space (MIN_FREE_BUFFER + incoming size).
5. Moves content from other disks to the Target Disk.
6. Cleans up empty source directories and junk files.

Safety Features:
- DRY RUN by default. Use --execute to apply changes.
- Skips series if conflicting episode duplicates (different sizes) are found across disks.
- Checks for minimum free space before planning moves.

Usage:
    python3 tvconsolidator.py /mnt/disk1 /mnt/disk2 /mnt/disk3
    python3 tvconsolidator.py --execute /mnt/disk1 /mnt/disk2
"""

import os
import argparse
import re
import shutil
import sys
from collections import defaultdict

# --- Configuration ---

# Minimum free space required on the target drive to consider moving files there.
# Defaults to 100 GiB to prevent drives completely.
MIN_FREE_BUFFER = 100 * 1024 * 1024 * 1024  # 100 GiB

# Minimum size for a file to be considered a "main video file".
# Helps distinguish actual episodes from sample files or extras.
MIN_VIDEO_SIZE = 50 * 1024 * 1024           # 50 MB

# Regex pattern to identify Season/Episode numbers (e.g., S01E05).
PATTERN = re.compile(r'(?i)S(\d+)E(\d+)')

# Files to aggressively clean if they are left behind in an otherwise empty folder.
JUNK_FILES = {'.plexmatch', '.ds_store', 'thumbs.db', 'desktop.ini'}

def normalize_name(name):
    """
    Normalizes a show name for comparison.
    'The.Homers' -> 'the homers'
    'The_Homerss' -> 'the homers'
    """
    # Replace dots and underscores with spaces, strip, lowercase
    clean = re.sub(r'[._]', ' ', name).strip().lower()
    # Collapse multiple spaces
    return re.sub(r'\s+', ' ', clean)

def get_free_space(path):
    """Returns the number of free bytes on the filesystem containing path."""
    try:
        _, _, free = shutil.disk_usage(path)
        return free
    except OSError:
        return 0

def get_file_info(path):
    """Returns file size in bytes, or -1 on error."""
    try:
        return os.path.getsize(path)
    except OSError:
        return -1

# --- Action Functions ---

def safe_move(src, dest_dir, dry_run=True):
    """
    Moves a file to dest_dir, ensuring the destination doesn't already exist.
    """
    try:
        if not os.path.exists(src):
            print(f"  [ERROR] Source missing: {src}")
            return False

        dst_path = os.path.join(dest_dir, os.path.basename(src))

        if os.path.exists(dst_path):
            print(f"  [ABORT] Video collision: Target exists at {dst_path}")
            return False

        if not dry_run:
            os.makedirs(dest_dir, exist_ok=True)
            shutil.move(src, dst_path)
            print(f"  [MOVED] {src} -> {dst_path}")
        else:
            print(f"  [DRY-RUN] mv {src} -> {dest_dir}/")
        return True
    except Exception as e:
        print(f"  [CRITICAL ERROR] Failed moving {src}: {e}")
        return False

def force_move(src, dest_dir, dry_run=True):
    """
    Moves a file to dest_dir, overwriting if necessary.
    Used for 'companion' files (nfo, jpg) where the target version is preferred 
    or they are assumed identical/expendable.
    """
    try:
        if not os.path.exists(src): return False
        dst_path = os.path.join(dest_dir, os.path.basename(src))

        if not dry_run:
            os.makedirs(dest_dir, exist_ok=True)
            if os.path.exists(dst_path):
                os.remove(dst_path)
            shutil.move(src, dst_path)
            print(f"  [MERGED] {src} -> {dest_dir}/")
        else:
            if os.path.exists(dst_path):
                print(f"  [DRY-RUN] rm {dst_path} && mv {src} ...")
            else:
                print(f"  [DRY-RUN] mv {src} -> {dest_dir}/")
        return True
    except Exception as e:
        print(f"  [ERROR] Moving {src}: {e}")
        return False

def safe_delete(src, dry_run=True):
    """Deletes a file."""
    try:
        if not dry_run:
            if os.path.exists(src):
                os.remove(src)
                print(f"  [DELETED] {src}")
        else:
            print(f"  [DRY-RUN] rm {src}")
        return True
    except Exception as e:
        print(f"  [ERROR] Deleting {src}: {e}")
        return False

def cleanup_folder_tree(path, dry_run=True):
    """
    Recursively removes empty folders and cleans junk files from a directory tree.
    Traverses bottom-up to ensure nested empty directories are removed.
    """
    if not os.path.exists(path): return

    # Walk bottom-up
    for root, dirs, files in os.walk(path, topdown=False):
        # Check files in current dir
        is_empty = True
        for f in files:
            if f.lower() in JUNK_FILES:
                # It's junk. If we are cleaning, delete it.
                junk_path = os.path.join(root, f)
                safe_delete(junk_path, dry_run)
            else:
                # It's a real file (Video/Subtitle/etc). Folder is not empty.
                is_empty = False
        
        # If no real files remain (or we just deleted the junk), try to remove dir
        if is_empty:
            # Re-check actual content in case safe_delete was dry-run
            if not dry_run:
                try:
                    # Check if dirs are empty (they should be due to bottom-up walk)
                    if not os.listdir(root):
                        os.rmdir(root)
                        print(f"  [CLEANUP] Removed empty dir: {root}")
                except OSError:
                    pass # Dir probably not empty
            else:
                # In dry run, we assume we deleted the junk, so we print intent
                print(f"  [DRY-RUN] rmdir {root} (If empty)")

# --- Core Logic ---

def scan_library(roots):
    """
    Scans all root directories for TV series and episodes.
    Returns a dictionary structure grouping files by Series -> Season/Episode.
    
    Structure:
    library[norm_key] = {
      'display_name': "The Homers", (Best guess name)
      'disks': {
          '/mnt/hdd1': {'real_folder': 'The.Homers', 'total_size': 0},
          '/mnt/hdd2': {'real_folder': 'The Homers', 'total_size': 0}
      },
      'episodes': { (season, ep): [List of file entries] },
      'artifacts': [List of non-episode files]
    }
    """
    library = defaultdict(lambda: {
        'display_name': None,
        'disks': defaultdict(lambda: {'real_folder': '', 'total_size': 0}), 
        'episodes': defaultdict(list),
        'artifacts': []
    })
    
    print(f"--- Scanning {len(roots)} paths ---")

    for root_path in roots:
        real_root = os.path.abspath(root_path)
        if not os.path.exists(real_root):
            print(f"[!] Warning: Path not found: {root_path}")
            continue

        print(f"Scanning: {root_path}")
        
        try:
            series_dirs = [d for d in os.listdir(real_root) if os.path.isdir(os.path.join(real_root, d))]
        except OSError as e:
            print(f"[!] Error reading {root_path}: {e}")
            continue

        for folder_name in series_dirs:
            norm_key = normalize_name(folder_name)
            series_path = os.path.join(real_root, folder_name)
            
            # Store metadata about this specific variant
            lib_entry = library[norm_key]
            
            # Set display name if not set (or overwrite if this one looks "nicer" - strict heuristic omitted for simplicity)
            if not lib_entry['display_name']:
                lib_entry['display_name'] = folder_name
                
            lib_entry['disks'][root_path]['real_folder'] = folder_name

            # Walk files
            for dirpath, _, filenames in os.walk(series_path):
                # Calculate relative structure using the *current* folder name
                rel_dir = os.path.relpath(dirpath, series_path)
                if rel_dir == ".": rel_dir = ""

                # 1. Group SxxExx within this folder
                local_ep_groups = defaultdict(list)
                unmatched_files = []

                for f in filenames:
                    match = PATTERN.search(f)
                    if match:
                        s_num = int(match.group(1))
                        e_num = int(match.group(2))
                        f_path = os.path.join(dirpath, f)
                        size = get_file_info(f_path)
                        local_ep_groups[(s_num, e_num)].append({
                            'filename': f, 'path': f_path, 'size': size, 'match_end': match.end()
                        })
                    else:
                        unmatched_files.append(f)

                # 2. Process Groups (Local Highlander Logic)
                # If multiple files match SxxExx in this folder, pick the largest as the "Episode"
                # and attach others as "Companions" (or duplicates to be moved along).
                files_claimed = set()
                
                for (s, e), group in local_ep_groups.items():
                    group.sort(key=lambda x: x['size'], reverse=True)
                    candidate = group[0]
                    
                    if candidate['size'] > MIN_VIDEO_SIZE:
                        files_claimed.add(candidate['filename'])
                        
                        companions = []
                        total_size = candidate['size']
                        
                        # Add smaller SxxExx files as companions
                        for other in group[1:]:
                            files_claimed.add(other['filename'])
                            companions.append({'path': other['path'], 'size': other['size']})
                            total_size += other['size']

                        # Add prefix-matched unmatched files (e.g., subtitles with same basename)
                        prefix = candidate['filename'][:candidate['match_end']]
                        for um_f in unmatched_files:
                            if um_f in files_claimed: continue
                            if um_f.startswith(prefix):
                                um_path = os.path.join(dirpath, um_f)
                                um_size = get_file_info(um_path)
                                companions.append({'path': um_path, 'size': um_size})
                                total_size += um_size
                                files_claimed.add(um_f)

                        entry = {
                            'path': candidate['path'],
                            'size': candidate['size'],
                            'total_size': total_size,
                            'disk': root_path,
                            'rel_dir': rel_dir, # e.g. "Season 1"
                            'companions': companions
                        }
                        lib_entry['episodes'][(s, e)].append(entry)
                        
                        # Track total size for this disk/series combo
                        lib_entry['disks'][root_path]['total_size'] += total_size

                # 3. Artifacts (Images, NFOs, leftovers)
                for f in filenames:
                    if f not in files_claimed:
                        if f.startswith('.'): continue
                        f_path = os.path.join(dirpath, f)
                        size = get_file_info(f_path)
                        lib_entry['artifacts'].append({
                            'path': f_path, 'disk': root_path, 'rel_dir': rel_dir, 'size': size
                        })
                        lib_entry['disks'][root_path]['total_size'] += size

    return library

def process_consolidation(library, execute=False):
    """
    Analyzes the library structure and performs/simulates the consolidation.
    Returns a list of skipped series (log messages).
    """
    print("\n--- Starting Consolidation Analysis ---")
    if execute:
        print("!!! LIVE MODE - Files will be MOVED and DELETED !!!\n")
    else:
        print("!!! DRY RUN MODE - No files will be moved !!!\n")

    skipped_log = []
    
    for norm_key in sorted(library.keys()):
        data = library[norm_key]
        display_name = data['display_name']
        episodes = data['episodes']
        artifacts = data['artifacts']
        # Set of root paths involved
        involved_disks = list(data['disks'].keys())
        
        # Skip if entirely on one disk already
        if len(involved_disks) <= 1:
            continue

        print(f"Processing: {display_name} (Found on {len(involved_disks)} disks)...")

        # 1. Safety Check: Conflicting Duplicates
        # If the same episode exists on multiple disks with DIFFERENT sizes, we skip the series.
        # User must resolve this manually to avoid data loss.
        has_bad_dupe = False
        for (s, e), copies in episodes.items():
            if len(copies) > 1:
                sizes = set(c['size'] for c in copies)
                if len(sizes) > 1:
                    has_bad_dupe = True
                    break
        
        if has_bad_dupe:
            msg = f"SKIPPED {display_name}: Conflicting duplicates (different sizes) found."
            print(f"  [!] {msg}")
            skipped_log.append(msg)
            continue

        # 2. Select Target Disk
        # We want to move everything to the disk that already has the MOST content for this show.
        # Sort candidates by total size held (descending)
        candidates = sorted(data['disks'].items(), key=lambda x: x[1]['total_size'], reverse=True)
        
        target_disk = None
        target_series_folder_name = None
        plan_import_size = 0
        
        for disk_path, meta in candidates:
            # Calculate import size (how much we need to move TO this disk)
            bytes_needed = 0
            
            # Sum up episodes NOT on this disk
            for (s, e), copies in episodes.items():
                if not any(c['disk'] == disk_path for c in copies):
                    bytes_needed += copies[0]['total_size']
            
            # Sum up artifacts NOT on this disk
            for art in artifacts:
                if art['disk'] != disk_path:
                    bytes_needed += art['size']
            
            # Check free space
            if (get_free_space(disk_path) - bytes_needed) > MIN_FREE_BUFFER:
                target_disk = disk_path
                # CRITICAL: Use the folder name that ALREADY EXISTS on the target disk
                target_series_folder_name = meta['real_folder']
                plan_import_size = bytes_needed
                break
        
        if not target_disk:
            msg = f"SKIPPED {display_name}: No candidate disk has enough free space."
            print(f"  [!] {msg}")
            skipped_log.append(msg)
            continue

        # 3. Execute Actions
        print(f"  -> Consolidating to: {target_disk}/{target_series_folder_name}")
        target_root_full = os.path.join(target_disk, target_series_folder_name)

        # A) Episodes
        for (s, e), copies in episodes.items():
            on_target = [c for c in copies if c['disk'] == target_disk]
            others = [c for c in copies if c['disk'] != target_disk]
            
            if on_target:
                # The episode already exists on the target disk.
                # Since we passed the "Conflicting Duplicates" check, we know the 'others' are identical size.
                # We can safely delete the copies on other disks.
                for c in others:
                    safe_delete(c['path'], not execute)
                    for comp in c['companions']: safe_delete(comp['path'], not execute)
            else:
                # The episode is missing from target. Move it there.
                src = others[0]
                dest_dir = os.path.join(target_root_full, src['rel_dir'])
                
                if safe_move(src['path'], dest_dir, not execute):
                    # Move companions (subtitles etc) - force_move handles potential overwrites if junk exists
                    for comp in src['companions']:
                        force_move(comp['path'], dest_dir, not execute)
                    # If there were multiple copies on source disks (redundant), delete them now
                    for redundant in others[1:]:
                        safe_delete(redundant['path'], not execute)
                        for comp in redundant['companions']: safe_delete(comp['path'], not execute)
                else:
                    print(f"  [ABORT] Move failed for S{s}E{e}.")

        # B) Artifacts
        for art in artifacts:
            if art['disk'] != target_disk:
                dest_dir = os.path.join(target_root_full, art['rel_dir'])
                force_move(art['path'], dest_dir, not execute)

        # 4. Cleanup Source Folders
        # Iterate all disks that were NOT the target, and clean their specific folder for this series
        for disk_path in involved_disks:
            if disk_path == target_disk: continue
            
            # The folder name on THIS specific disk
            source_folder_name = data['disks'][disk_path]['real_folder']
            source_full_path = os.path.join(disk_path, source_folder_name)
            
            print(f"  -> Cleaning source: {source_full_path}")
            cleanup_folder_tree(source_full_path, not execute)

    return skipped_log

def main():
    parser = argparse.ArgumentParser(description="Consolidate media library.")
    parser.add_argument('paths', metavar='PATH', type=str, nargs='+', help='Root directories')
    parser.add_argument('--execute', action='store_true', help='Apply changes')
    args = parser.parse_args()
    
    library = scan_library(args.paths)
    skipped = process_consolidation(library, args.execute)
    
    if skipped:
        print("\n--- Skipped Series Report ---")
        for line in skipped:
            print(line)

if __name__ == "__main__":
    main()
