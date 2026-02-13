"""
Tests for agent critique issue resolution functionality.
"""

prefix = "v3"


def _create_translation(client, token, assessment_id, vref, draft_text="test"):
    """Helper: create an agent translation and return its ID."""
    resp = client.post(
        f"{prefix}/agent/translation",
        json={
            "assessment_id": assessment_id,
            "vref": vref,
            "draft_text": draft_text,
        },
        headers={"Authorization": f"Bearer {token}"},
    )
    assert resp.status_code == 200, f"Failed to create translation: {resp.json()}"
    return resp.json()["id"]


def _create_critique(
    client, token, translation_id, omissions=None, additions=None, replacements=None
):
    """Helper: create critique issues for a translation and return the response."""
    return client.post(
        f"{prefix}/agent/critique",
        json={
            "agent_translation_id": translation_id,
            "omissions": omissions or [],
            "additions": additions or [],
            "replacements": replacements or [],
        },
        headers={"Authorization": f"Bearer {token}"},
    )


def test_resolve_critique_issue_success(client, regular_token1, test_assessment_id):
    """Test successfully resolving a critique issue."""
    # First create a translation and then a critique issue
    translation_id = _create_translation(
        client, regular_token1, test_assessment_id, "JHN 1:1"
    )

    create_response = _create_critique(
        client,
        regular_token1,
        translation_id,
        omissions=[{"source_text": "word", "comments": "missing word", "severity": 4}],
    )
    assert create_response.status_code == 200
    created_issues = create_response.json()
    issue_id = created_issues[0]["id"]

    # Now resolve the issue
    resolution_data = {"resolution_notes": "Fixed by updating translation"}
    resolve_response = client.patch(
        f"{prefix}/agent/critique/{issue_id}/resolve",
        json=resolution_data,
        headers={"Authorization": f"Bearer {regular_token1}"},
    )

    assert resolve_response.status_code == 200
    resolved_issue = resolve_response.json()

    # Verify resolution fields
    assert resolved_issue["is_resolved"] is True
    assert (
        resolved_issue["resolved_by_id"] is not None
    )  # testuser1's ID (varies by test run)
    assert resolved_issue["resolved_at"] is not None
    assert resolved_issue["resolution_notes"] == "Fixed by updating translation"


def test_unresolve_critique_issue_success(client, regular_token1, test_assessment_id):
    """Test successfully unresolving a previously resolved critique issue."""
    # Create a translation, then a critique issue
    translation_id = _create_translation(
        client, regular_token1, test_assessment_id, "JHN 1:3"
    )

    create_response = _create_critique(
        client,
        regular_token1,
        translation_id,
        omissions=[{"source_text": "made", "comments": "missing made", "severity": 3}],
    )
    issue_id = create_response.json()[0]["id"]

    # Resolve it first
    resolve_response = client.patch(
        f"{prefix}/agent/critique/{issue_id}/resolve",
        json={"resolution_notes": "Initially resolved"},
        headers={"Authorization": f"Bearer {regular_token1}"},
    )
    assert resolve_response.status_code == 200

    # Now unresolve it
    unresolve_response = client.patch(
        f"{prefix}/agent/critique/{issue_id}/unresolve",
        headers={"Authorization": f"Bearer {regular_token1}"},
    )

    assert unresolve_response.status_code == 200
    unresolved_issue = unresolve_response.json()

    # Verify it's unresolved
    assert unresolved_issue["is_resolved"] is False
    assert unresolved_issue["resolved_by_id"] is None
    assert unresolved_issue["resolved_at"] is None
    assert unresolved_issue["resolution_notes"] is None


def test_get_critique_issues_filter_by_resolved(
    client, regular_token1, test_assessment_id
):
    """Test filtering critique issues by resolution status."""
    # Create translations for the critique issues
    t1 = _create_translation(client, regular_token1, test_assessment_id, "JHN 2:1")
    t2 = _create_translation(client, regular_token1, test_assessment_id, "JHN 2:2")

    # Create both issues
    unresolved_response = _create_critique(
        client,
        regular_token1,
        t1,
        omissions=[
            {
                "source_text": "unresolved",
                "comments": "this stays unresolved",
                "severity": 2,
            }
        ],
    )
    resolved_response = _create_critique(
        client,
        regular_token1,
        t2,
        omissions=[
            {"source_text": "resolved", "comments": "this gets resolved", "severity": 3}
        ],
    )

    assert unresolved_response.status_code == 200
    assert resolved_response.status_code == 200

    resolved_issue_id = resolved_response.json()[0]["id"]

    # Resolve one of them
    client.patch(
        f"{prefix}/agent/critique/{resolved_issue_id}/resolve",
        json={"resolution_notes": "All good now"},
        headers={"Authorization": f"Bearer {regular_token1}"},
    )

    # Get only resolved issues
    resolved_filter_response = client.get(
        f"{prefix}/agent/critique?assessment_id={test_assessment_id}&is_resolved=true",
        headers={"Authorization": f"Bearer {regular_token1}"},
    )
    assert resolved_filter_response.status_code == 200
    resolved_issues = resolved_filter_response.json()

    # Should only contain the resolved issue from this test
    resolved_texts = [issue["source_text"] for issue in resolved_issues]
    assert "resolved" in resolved_texts
    assert all(issue["is_resolved"] for issue in resolved_issues)

    # Get only unresolved issues
    unresolved_filter_response = client.get(
        f"{prefix}/agent/critique?assessment_id={test_assessment_id}&is_resolved=false",
        headers={"Authorization": f"Bearer {regular_token1}"},
    )
    assert unresolved_filter_response.status_code == 200
    unresolved_issues = unresolved_filter_response.json()

    # Should contain all unresolved issues (including any from other tests)
    unresolved_texts = [issue["source_text"] for issue in unresolved_issues]
    assert "unresolved" in unresolved_texts
    assert all(not issue["is_resolved"] for issue in unresolved_issues)


def test_resolve_replacement_issue_success(client, regular_token1, test_assessment_id):
    """Test that replacement issues can be resolved just like omissions."""
    translation_id = _create_translation(
        client, regular_token1, test_assessment_id, "JHN 1:4"
    )

    create_response = _create_critique(
        client,
        regular_token1,
        translation_id,
        replacements=[
            {
                "source_text": "love",
                "draft_text": "like",
                "comments": "wrong term",
                "severity": 4,
            }
        ],
    )
    assert create_response.status_code == 200
    issue_id = create_response.json()[0]["id"]

    # Resolve it
    resolve_response = client.patch(
        f"{prefix}/agent/critique/{issue_id}/resolve",
        json={"resolution_notes": "Fixed translation to use 'love'"},
        headers={"Authorization": f"Bearer {regular_token1}"},
    )

    assert resolve_response.status_code == 200
    resolved = resolve_response.json()
    assert resolved["is_resolved"] is True
    assert resolved["resolution_notes"] == "Fixed translation to use 'love'"
    assert resolved["issue_type"] == "replacement"
    assert resolved["source_text"] == "love"
    assert resolved["draft_text"] == "like"


def test_resolve_requires_authentication(client):
    """Test that resolution endpoints require authentication."""
    # Try to resolve without auth headers
    response = client.patch(f"{prefix}/agent/critique/1/resolve", json={})
    assert response.status_code == 401

    # Try to unresolve without auth headers
    response = client.patch(f"{prefix}/agent/critique/1/unresolve")
    assert response.status_code == 401


def test_resolve_nonexistent_critique_issue(client, regular_token1):
    """Test attempting to resolve a non-existent critique issue."""
    resolution_data = {"resolution_notes": "This won't work"}
    response = client.patch(
        f"{prefix}/agent/critique/99999/resolve",
        json=resolution_data,
        headers={"Authorization": f"Bearer {regular_token1}"},
    )

    assert response.status_code == 404
    assert "Critique issue not found" in response.json()["detail"]
