#!/usr/bin/env python3
"""
File Utility Functions
Helper functions for file operations (JSON, CSV, etc.)
"""

import json
from pathlib import Path
from typing import Any, List, Dict


def save_json(data: Any, filepath: Path, indent: int = 2):
    """
    Save data to JSON file.
    
    Args:
        data: Data to save
        filepath (Path): File path
        indent (int): JSON indentation
    """
    with open(filepath, 'w', encoding='utf-8') as f:
        json.dump(data, f, indent=indent, ensure_ascii=False)


def load_json(filepath: Path, default: Any = None) -> Any:
    """
    Load data from JSON file.
    
    Args:
        filepath (Path): File path
        default: Default value if file doesn't exist
        
    Returns:
        Loaded data or default
    """
    if not filepath.exists():
        return default
    
    try:
        with open(filepath, 'r', encoding='utf-8') as f:
            return json.load(f)
    except json.JSONDecodeError:
        return default


def append_to_jsonl(data: Dict, filepath: Path):
    """
    Append data to JSON Lines file.
    
    Args:
        data (dict): Data to append
        filepath (Path): File path
    """
    with open(filepath, 'a', encoding='utf-8') as f:
        f.write(json.dumps(data, ensure_ascii=False) + '\n')


def read_jsonl(filepath: Path) -> List[Dict]:
    """
    Read JSON Lines file.
    
    Args:
        filepath (Path): File path
        
    Returns:
        List of dictionaries
    """
    if not filepath.exists():
        return []
    
    data = []
    with open(filepath, 'r', encoding='utf-8') as f:
        for line in f:
            if line.strip():
                data.append(json.loads(line))
    return data


def ensure_dir(directory: Path):
    """
    Ensure directory exists.
    
    Args:
        directory (Path): Directory path
    """
    directory.mkdir(parents=True, exist_ok=True)