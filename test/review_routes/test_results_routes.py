import pandas as pd
from database.models import (
    Assessment,
    AssessmentResult,
    AssessmentAccess,
    BibleRevision,
    BibleVersion,
    UserDB,
    Group,
)


def setup_assessments_results(db_session):
    """Setup reference data and ISO codes."""
    # Load data from CSV files for assessments and assessment results
    assessment_df = pd.read_csv("fixtures/assessments.txt", sep="\t")
    assessment_result_df = pd.read_json(
        "fixtures/assessment_results.json", orient="records", lines=True
    )
    revision_df = pd.read_csv("fixtures/revision_for_assessment.txt", sep="\t")
    version_df = pd.read_csv("fixtures/version_for_assessment.txt", sep="\t")

    user = db_session.query(UserDB).filter(UserDB.username == "testuser1").first()
    user_id = user.id if user else None
    version_df["owner_id"] = user_id
    # Populate assessment and assessment results tables
    for _, row in version_df.iterrows():
        db_session.add(BibleVersion(**row.to_dict()))
    for _, row in revision_df.iterrows():
        db_session.add(BibleRevision(**row.to_dict()))
    for _, row in assessment_df.iterrows():
        db_session.add(Assessment(**row.to_dict()))
    for _, row in assessment_result_df.iterrows():
        db_session.add(AssessmentResult(**row.to_dict()))
    db_session.commit()

    assessments = db_session.query(Assessment.id).first()
    first_assessment_id = assessments[0]

    # update assessmentaccess so group 1 has access to the first assessment
    group = db_session.query(Group.id).first()
    new_access = AssessmentAccess(assessment_id=first_assessment_id, group_id=group[0])
    db_session.add(new_access)
    db_session.commit()
    return first_assessment_id


def test_regular_user_flow(client, regular_token1, regular_token2, test_db_session):
    first_assessment_id = setup_assessments_results(
        test_db_session
    )  # Use the fixture test_db_session
    # Define parameters for your request
    params = {
        "assessment_id": first_assessment_id,
        "aggregate": "chapter",
        "include_text": False,
        "reverse": False,
    }

    # Make the request to the endpoint
    response = client.get(
        "/v3/result",
        params=params,
        headers={"Authorization": f"Bearer {regular_token1}"},
    )

    # Check that the response is as expected
    assert response.status_code == 200
    response_data = response.json()

    assert response_data["total_count"] > 0
    assert response_data["results"], "No results found in response."
    assert (
        len(response_data["results"]) == response_data["total_count"]
    ), "Results length is not as expected."
    assert response_data["results"][0]["vref"]
    assert response_data["results"][0]["score"] >= 0
    assert response_data["results"][0]["score"] <= 1
    assert response_data["results"][0]["assessment_id"] == first_assessment_id

    # check that second user does not have access
    response = client.get(
        "/v3/result",
        params=params,
        headers={"Authorization": f"Bearer {regular_token2}"},
    )
    assert response.status_code == 403
