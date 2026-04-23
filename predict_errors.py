"""Shared exceptions raised by assessment `predict()` functions.

Mirrors `shared/predict_errors.py` in sil-ai/aqua-assessments. The module name
and class name must match the remote definitions so Modal can unpickle the
exception when it crosses the worker → API boundary. Without a locally
importable `predict_errors` module, unpickling fails and the original
exception surfaces as an opaque `modal.exception.ExecutionError`.

This module lives at the aqua-api repo root — not in a sub-package — because
the remote image places its copy at `/root/predict_errors.py` (top-level
`predict_errors` import). Moving this file into a sub-package would break the
pickle-resolution path unless the remote image layout changes in lockstep.
"""


class TrainingNotAvailableError(ValueError):
    """Raised by `predict()` when the assessment hasn't been trained yet.

    Subclass of `ValueError` so the message still reaches the caller via the
    existing error-passthrough path even if this module ever drifts from the
    assessments-side definition.
    """
