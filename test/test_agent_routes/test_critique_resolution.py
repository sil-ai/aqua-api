"""
Tests for agent critique issue resolution functionality.
"""

prefix = "v3"


def test_resolve_critique_issue_success(client, regular_token1, test_assessment_id):
    """Test successfully resolving a critique issue."""
    # First create a critique issue
    critique_data = {
        "assessment_id": test_assessment_id,
        "vref": "JHN 1:1",
        "omissions": [{"text": "word", "comments": "missing word", "severity": 4}],
        "additions": [],
    }

    create_response = client.post(
        f"{prefix}/agent/critique",
        json=critique_data,
        headers={"Authorization": f"Bearer {regular_token1}"},
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
    # Create and resolve a critique issue
    critique_data = {
        "assessment_id": test_assessment_id,
        "vref": "JHN 1:3",
        "omissions": [{"text": "made", "comments": "missing made", "severity": 3}],
        "additions": [],
    }

    create_response = client.post(
        f"{prefix}/agent/critique",
        json=critique_data,
        headers={"Authorization": f"Bearer {regular_token1}"},
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
    # Create multiple critique issues
    unresolved_data = {
        "assessment_id": test_assessment_id,
        "vref": "JHN 2:1",
        "omissions": [
            {"text": "unresolved", "comments": "this stays unresolved", "severity": 2}
        ],
        "additions": [],
    }

    resolved_data = {
        "assessment_id": test_assessment_id,
        "vref": "JHN 2:2",
        "omissions": [
            {"text": "resolved", "comments": "this gets resolved", "severity": 3}
        ],
        "additions": [],
    }

    # Create both issues
    unresolved_response = client.post(
        f"{prefix}/agent/critique",
        json=unresolved_data,
        headers={"Authorization": f"Bearer {regular_token1}"},
    )
    resolved_response = client.post(
        f"{prefix}/agent/critique",
        json=resolved_data,
        headers={"Authorization": f"Bearer {regular_token1}"},
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
    resolved_texts = [issue["text"] for issue in resolved_issues]
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
    unresolved_texts = [issue["text"] for issue in unresolved_issues]
    assert "unresolved" in unresolved_texts
    assert all(not issue["is_resolved"] for issue in unresolved_issues)


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
