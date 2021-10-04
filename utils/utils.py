from typing import List, Dict, Set, Any

def extract_dict(
        dict_: Dict[Any, Any],
        wanted_keys: List[Any]) -> Dict[Any, Any]:
    """dictから不要なkey, valueを削除

    Parameters
    ----------
    dict_ : Dict[Any, Any]
        対象のdict
    wanted_keys : List[Any]
        残すkey

    Returns
    -------
    Dict[Any, Any]
        wanted_keysのみを残したdict
    """
    unwanted_keys : Set[Any] = set(dict_.keys()) - set(wanted_keys)  # 取り除くkey
    for key in unwanted_keys:
        del dict_[key]

    return dict_