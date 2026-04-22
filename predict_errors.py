"""Shared exceptions raised by assessment `predict()` functions.

Mirrors `shared/predict_errors.py` in sil-ai/aqua-assessments. The module name
and class name must match the remote definitions so Modal can unpickle the
exception when it crosses the worker → API boundary. Without a locally
importable `predict_errors` module, unpickling fails and the original
exception surfaces as an opaque `modal.exception.ExecutionError`.
"""


class TrainingNotAvailableError(ValueError):
    """Raised by `predict()` when the assessment hasn't been trained yet.

    Subclass of `ValueError` so the message still reaches the caller via the
    existing error-passthrough path even if this module ever drifts from the
    assessments-side definition.
    """
