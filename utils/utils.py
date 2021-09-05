from typing import List, Dict, Set, Any

def extract_dict(
        dict_: Dict[Any, Any],
        wanted_keys: List[Any]):
    unwanted_keys : Set[Any] = set(dict_.keys()) - set(wanted_keys)
    for key in unwanted_keys:
        del dict_[key]

    return dict_