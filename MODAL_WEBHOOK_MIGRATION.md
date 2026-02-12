# Modal Webhook Endpoints for Realtime Assessments

## Overview

Add HTTP webhook endpoints to the semantic-similarity and text-lengths Modal apps to support the aqua-api migration from Modal SDK to webhooks.

## Context

The aqua-api currently calls Modal functions via the SDK's `.remote()` method (gRPC). We're migrating to HTTP webhooks for better async compatibility, reduced dependencies, and consistency with the existing assessment runner pattern.

**This change is backward compatible** — we're adding new `realtime_assess_http` functions while keeping the existing `realtime_assess` functions intact.

## Files to Modify

### 1. `assessments/semantic-similarity/app.py`

**Add imports** (if not already present):
```python
from pydantic import BaseModel
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from fastapi import Depends, HTTPException

auth_scheme = HTTPBearer()
```

**Add request model** (after imports):
```python
class RealtimeRequest(BaseModel):
    text1: str
    text2: str
```

**Add webhook endpoint** (after the existing `realtime_assess` function):
```python
@app.function(
    gpu="T4",
    scaledown_window=300,
    secrets=[
        modal.Secret.from_dict({"HF_HOME": CACHE_PATH}),
        modal.Secret.from_name("webhook-auth-token"),
    ],
    volumes={CACHE_PATH: cache_vol},
)
@modal.fastapi_endpoint(method="POST")
def realtime_assess_http(
    request: RealtimeRequest,
    credentials: HTTPAuthorizationCredentials = Depends(auth_scheme),
):
    """HTTP webhook endpoint for realtime semantic similarity assessment.

    Wraps the existing realtime_assess function with HTTP authentication.
    Uses the same webhook-auth-token Secret as the assessment runner.
    """
    import os
    if credentials.credentials != os.environ["AUTH_TOKEN"]:
        raise HTTPException(status_code=401, detail="Unauthorized")

    # Call the existing function locally (same container)
    return realtime_assess.local(text1=request.text1, text2=request.text2)
```

**Expected response format**: `{"score": float}` (range -1 to 1)

### 2. `assessments/text_lengths/app.py`

**Add imports** (if not already present):
```python
from pydantic import BaseModel
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from fastapi import Depends, HTTPException

auth_scheme = HTTPBearer()
```

**Add request model** (after imports):
```python
class RealtimeRequest(BaseModel):
    text1: str
    text2: str
```

**Add webhook endpoint** (after the existing `realtime_assess` function):
```python
@app.function(
    secrets=[modal.Secret.from_name("webhook-auth-token")],
)
@modal.fastapi_endpoint(method="POST")
def realtime_assess_http(
    request: RealtimeRequest,
    credentials: HTTPAuthorizationCredentials = Depends(auth_scheme),
):
    """HTTP webhook endpoint for realtime text lengths assessment.

    Wraps the existing realtime_assess function with HTTP authentication.
    Uses the same webhook-auth-token Secret as the assessment runner.
    """
    import os
    if credentials.credentials != os.environ["AUTH_TOKEN"]:
        raise HTTPException(status_code=401, detail="Unauthorized")

    # Call the existing function locally (same container)
    return realtime_assess.local(text1=request.text1, text2=request.text2)
```

**Expected response format**: `{"word_count_difference": int, "char_count_difference": int}`

## Implementation Notes

### Authentication
- Both endpoints use the existing `webhook-auth-token` Modal Secret (same as the assessment runner)
- The Secret must contain an `AUTH_TOKEN` environment variable
- Requests must include header: `Authorization: Bearer <token>`

### Function Naming
- New functions are named `realtime_assess_http` to distinguish from SDK-based `realtime_assess`
- Keep the original `realtime_assess` functions unchanged for backward compatibility

### URL Convention
After deployment, the webhooks will be available at:
- **Dev environment**:
  - `https://sil-ai-dev--semantic-similarity-realtime-assess-http.modal.run`
  - `https://sil-ai-dev--text-lengths-realtime-assess-http.modal.run`
- **Main environment**:
  - `https://sil-ai--semantic-similarity-realtime-assess-http.modal.run`
  - `https://sil-ai--text-lengths-realtime-assess-http.modal.run`

(Verify exact URLs from Modal dashboard after deployment)

### Performance
- Same GPU configuration (T4 for semantic-similarity)
- Same scaledown window (300s for semantic-similarity)
- Same cache volume for model weights
- Cold start time unchanged (~30s for LaBSE model loading)

### Error Handling
- 401 Unauthorized: Invalid or missing auth token
- 422 Unprocessable Entity: Invalid request body (missing text1/text2)
- 500 Internal Server Error: Model inference errors

## Testing

### 1. Deploy to Dev Environment
```bash
modal deploy app.py --env dev
```

### 2. Test with curl

**Semantic Similarity:**
```bash
curl -X POST "https://sil-ai-dev--semantic-similarity-realtime-assess-http.modal.run" \
  -H "Authorization: Bearer $MODAL_WEBHOOK_TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"text1": "In the beginning God created", "text2": "Au commencement Dieu créa"}'
```

Expected response:
```json
{"score": 0.85}
```

**Text Lengths:**
```bash
curl -X POST "https://sil-ai-dev--text-lengths-realtime-assess-http.modal.run" \
  -H "Authorization: Bearer $MODAL_WEBHOOK_TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"text1": "In the beginning God created the heaven and the earth", "text2": "In the beginning God created"}'
```

Expected response:
```json
{"word_count_difference": 6, "char_count_difference": 25}
```

### 3. Test Cold Start
First request will take ~30 seconds for semantic-similarity (LaBSE model loading). Subsequent requests should be fast (<1s).

### 4. Test Auth Failure
```bash
curl -X POST "https://sil-ai-dev--semantic-similarity-realtime-assess-http.modal.run" \
  -H "Authorization: Bearer invalid-token" \
  -H "Content-Type: application/json" \
  -d '{"text1": "test", "text2": "test"}'
```

Expected: 401 Unauthorized

## Deployment Order

1. ✅ **Deploy to dev** → Test with curl → Verify URLs
2. ✅ **Deploy aqua-api to dev** → Integration test via API
3. ✅ **Deploy to main** → Monitor for 1-2 days
4. ⏳ **Clean up old SDK functions** (after 1-2 weeks of stable operation)

## Verification Checklist

- [ ] Imports added (BaseModel, HTTPBearer, HTTPAuthorizationCredentials, Depends, HTTPException)
- [ ] `auth_scheme = HTTPBearer()` defined
- [ ] `RealtimeRequest` model defined in both files
- [ ] `realtime_assess_http` function added to semantic-similarity with GPU config
- [ ] `realtime_assess_http` function added to text_lengths without GPU
- [ ] Both functions use `modal.Secret.from_name("webhook-auth-token")`
- [ ] Both functions call `realtime_assess.local()` to reuse existing logic
- [ ] Deployed to dev environment
- [ ] Curl tests pass for both endpoints
- [ ] Auth failure test returns 401
- [ ] URLs confirmed in Modal dashboard
- [ ] aqua-api updated with correct URLs (see aqua-api branch: `add-semsim-tl-realtime-endpoints`)

## Rollback Plan

If issues arise:
1. The old `realtime_assess` SDK functions remain unchanged
2. Revert aqua-api to use SDK by checking out previous commit
3. No Modal changes needed (new endpoints don't affect existing SDK calls)

## Related Changes

See aqua-api branch `add-semsim-tl-realtime-endpoints` for the corresponding API changes:
- `assessment_routes/v3/realtime_routes.py` — Migrated from Modal SDK to httpx webhooks
- `test/test_assessment_routes/test_realtime_routes.py` — Updated all 22 tests to mock httpx

## Questions?

- Why not remove the old functions? → Backward compatibility during migration period
- Why reuse `webhook-auth-token`? → Consistency with existing assessment runner pattern
- Why `.local()` instead of `.remote()`? → We're already inside the Modal container, no need for RPC
- Why 60s timeout in API? → Cold starts take ~30s, need buffer for model loading
