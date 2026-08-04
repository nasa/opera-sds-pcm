import os

try:
    from s1reader.s1_reader import load_bursts as _load_bursts
except ImportError:
    def _load_bursts(*args, **kwargs):
        raise NotImplementedError('S1 Reader not available in environment')


def get_file_polarization_mode(file_path: str) -> str:
    safe_pol_mode = os.path.basename(file_path).split('_')[-6][2:]
    return safe_pol_mode


def mode_to_pols(mode: str) -> list[str]:
    return {
        'SH': ['HH'],
        'SV': ['VV'],
        'DH': ['HH', 'HV'],
        'DV': ['VV', 'VH'],
    }.get(mode, [])


def get_bursts(safe_path, orbit_path) -> list[str]:
    safe_pol_mode = get_file_polarization_mode(safe_path)
    pols = mode_to_pols(safe_pol_mode)

    i_subswaths = [1, 2, 3]
    pol_subswath_index_pairs = [(pol, i) for pol in pols for i in i_subswaths]

    bursts = set()

    for pol, i_subswath in pol_subswath_index_pairs:
        for burst in _load_bursts(safe_path, orbit_path, i_subswath, pol, flag_apply_eap=False):
            bursts.add(str(burst.burst_id))

    return list(bursts)
