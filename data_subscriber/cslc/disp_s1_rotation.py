"""Pure helpers for DISP-S1 compressed-CSLC (CCSLC) rotation bookkeeping.

Stdlib-only and side-effect-free so the logic can be unit-tested in isolation
(no ES, no PCM environment).
"""


def compute_projected_pending_boundaries(all_dates, published_last_dates, k, m,
                                          sensing_date):
    """Return YYYYMMDD k-boundary dates a KSC at ``sensing_date`` must still
    wait for, derived from the *expected* boundary positions in the actual
    date sequence rather than from which boundary KSCs happen to exist yet.

    This closes an out-of-order finalization hole: ``_get_pending_ccslc_boundaries``
    derives pending boundaries from earlier ``save_compressed_cslc`` KSCs that
    already exist. When a later KSC is evaluated before an earlier in-window
    boundary KSC has been created (parallel cascade), that earlier boundary is
    invisible, so the KSC finalizes (``compressed_cslc_final=True``) without
    waiting for the boundary's CCSLC -- and is then permanently locked out of
    every fix-up re-eval. Projecting expected boundaries from the date sequence
    makes the wait order-independent.

    Args:
        all_dates: full sorted YYYYMMDD sensing-date sequence for the frame
            (CSC + catalog dates), as ``_get_all_dates_sorted`` returns.
        published_last_dates: set/list of YYYYMMDD ``last_date`` values of
            CCSLCs already published for the frame.
        k: k-cycle window size.
        m: compressed-CSLC rotation; a KSC compresses the ``m-1`` most-recent
            CCSLCs.
        sensing_date: the KSC's YYYYMMDD sensing date.

    Returns:
        Sorted list of YYYYMMDD boundary dates that are expected to be in this
        KSC's ``m-1`` most-recent CCSLC set but whose CCSLC is not yet
        published. Empty when the rotation is (or will be) complete.

    Safety properties (so this can never strand a KSC on a CCSLC that will
    never publish):
      - Bounded to the ``m-1`` most-recent boundaries before ``sensing_date``;
        older (aged-out) boundaries are never returned.
      - Anchored on the most-recent *published* CCSLC, so superseded boundaries
        (which are superseded precisely because a CCSLC already exists at them,
        hence published) are excluded.
      - No published CCSLC anchor (greenfield/early window) -> returns [].
    """
    needed = m - 1
    if needed <= 0:
        return []

    published = {d for d in published_last_dates if d}
    pub_before = sorted(d for d in published if d < sensing_date)
    if not pub_before:
        # Greenfield / first ministack: no anchor to project from. Preserve
        # existing early-window behavior (no pending from projection).
        return []

    expected = set(pub_before)
    anchor = pub_before[-1]
    if anchor in all_dates:
        i = all_dates.index(anchor)
        j = i + k
        # Project forward in k-strides through the *actual* dates, so missed
        # acquisitions don't shift positions. Only boundaries strictly before
        # this KSC's sensing_date are its inputs.
        while j < len(all_dates) and all_dates[j] < sensing_date:
            expected.add(all_dates[j])
            j += k

    # A KSC compresses only the m-1 most-recent CCSLCs; older boundaries have
    # aged out of its window and must not block finalization.
    relevant = sorted(expected)[-needed:]
    return [d for d in relevant if d not in published]
