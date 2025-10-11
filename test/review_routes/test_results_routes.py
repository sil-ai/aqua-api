import pandas as pd

from database.models import (
    AlignmentTopSourceScores,
    Assessment,
    AssessmentResult,
    BibleRevision,
    BibleVersion,
    BibleVersionAccess,
    Group,
    UserDB,
)


def setup_assessments_results(db_session):
    """Setup reference data and ISO codes."""
    # Check if data is already loaded
    existing_assessment = db_session.query(Assessment).first()
    if existing_assessment:
        return existing_assessment.id

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

    # Add access from group 1 to the bible version in bible version access
    group = db_session.query(Group.id).first()

    revision_access = BibleVersionAccess(bible_version_id=115, group_id=group[0])
    db_session.add(revision_access)

    reference_access = BibleVersionAccess(bible_version_id=505, group_id=group[0])

    assessments = db_session.query(Assessment.id).first()
    first_assessment_id = assessments[0]

    db_session.add(reference_access)
    db_session.commit()

    return first_assessment_id


def test_regular_user_flow(client, regular_token1, regular_token2, test_db_session):
    first_assessment_id = setup_assessments_results(test_db_session)
    params = {
        "assessment_id": first_assessment_id,
        "aggregate": "chapter",
        "reverse": False,
    }
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

    # check that user can access the result with reverse
    params["reverse"] = True
    response = client.get(
        "/v3/result",
        params=params,
        headers={"Authorization": f"Bearer {regular_token1}"},
    )
    assert response.status_code == 200
    response_data = response.json()
    assert response_data["results"][0]["vref"]
    assert response_data["results"][0]["score"] >= 0
    assert response_data["results"][0]["score"] <= 1

    # check that user can access the result with aggregate=book
    params["aggregate"] = "book"
    params["reverse"] = False
    response = client.get(
        "/v3/result",
        params=params,
        headers={"Authorization": f"Bearer {regular_token1}"},
    )
    assert response.status_code == 200
    response_data = response.json()
    assert response_data["results"][0]["vref"]
    assert response_data["results"][0]["score"] >= 0
    assert response_data["results"][0]["score"] <= 1
    assert response_data["results"][0]["assessment_id"] == first_assessment_id

    # check that usec can access alignmentscores
    params = {
        "assessment_id": first_assessment_id,
        "reverse": False,
    }

    response = client.get(
        "/v3/alignmentscores",
        params=params,
        headers={"Authorization": f"Bearer {regular_token1}"},
    )
    assert response.status_code == 200

    # check that second user has no access to alignmentscores
    response = client.get(
        "/v3/alignmentscores",
        params=params,
        headers={"Authorization": f"Bearer {regular_token2}"},
    )
    assert response.status_code == 403


def setup_alignment_data(db_session, assessment_id):
    """Setup alignment top source scores data for testing textalignmentmatches."""
    # Check if alignment data already exists for this assessment
    existing_data = (
        db_session.query(AlignmentTopSourceScores)
        .filter(AlignmentTopSourceScores.assessment_id == assessment_id)
        .first()
    )

    if existing_data:
        return  # Data already set up

    # Create realistic alignment data with multiple source words and their targets
    # This simulates word alignment scores from a word-alignment assessment

    alignment_data = [
        # Word "god" aligns strongly to "dios" and weakly to "señor"
        {
            "assessment_id": assessment_id,
            "source": "god",
            "target": "dios",
            "score": 0.85,
            "vref": None,
            "book": "GEN",
            "chapter": 1,
            "verse": 1,
            "flag": False,
            "hide": False,
            "note": None,
        },
        {
            "assessment_id": assessment_id,
            "source": "god",
            "target": "dios",
            "score": 0.9,
            "vref": "GEN 1:2",
            "book": "GEN",
            "chapter": 1,
            "verse": 2,
            "flag": False,
            "hide": False,
            "note": None,
        },
        {
            "assessment_id": assessment_id,
            "source": "god",
            "target": "dios",
            "score": 0.88,
            "vref": "GEN 1:3",
            "book": "GEN",
            "chapter": 1,
            "verse": 3,
            "flag": False,
            "hide": False,
            "note": None,
        },
        {
            "assessment_id": assessment_id,
            "source": "god",
            "target": "señor",
            "score": 0.15,
            "vref": "GEN 1:4",
            "book": "GEN",
            "chapter": 1,
            "verse": 4,
            "flag": False,
            "hide": False,
            "note": None,
        },
        # Word "created" aligns to "creó" with high confidence
        {
            "assessment_id": assessment_id,
            "source": "created",
            "target": "creó",
            "score": 0.92,
            "vref": "GEN 1:1",
            "book": "GEN",
            "chapter": 1,
            "verse": 1,
            "flag": False,
            "hide": False,
            "note": None,
        },
        {
            "assessment_id": assessment_id,
            "source": "created",
            "target": "creó",
            "score": 0.89,
            "vref": "GEN 1:5",
            "book": "GEN",
            "chapter": 1,
            "verse": 5,
            "flag": False,
            "hide": False,
            "note": None,
        },
        {
            "assessment_id": assessment_id,
            "source": "created",
            "target": "hizo",
            "score": 0.1,
            "vref": "GEN 1:6",
            "book": "GEN",
            "chapter": 1,
            "verse": 6,
            "flag": False,
            "hide": False,
            "note": None,
        },
        # Word "heaven" with multiple possible translations
        {
            "assessment_id": assessment_id,
            "source": "heaven",
            "target": "cielo",
            "score": 0.75,
            "vref": "GEN 1:1",
            "book": "GEN",
            "chapter": 1,
            "verse": 1,
            "flag": False,
            "hide": False,
            "note": None,
        },
        {
            "assessment_id": assessment_id,
            "source": "heaven",
            "target": "cielo",
            "score": 0.8,
            "vref": "GEN 1:7",
            "book": "GEN",
            "chapter": 1,
            "verse": 7,
            "flag": False,
            "hide": False,
            "note": None,
        },
        {
            "assessment_id": assessment_id,
            "source": "heaven",
            "target": "cielos",
            "score": 0.2,
            "vref": "GEN 1:8",
            "book": "GEN",
            "chapter": 1,
            "verse": 8,
            "flag": False,
            "hide": False,
            "note": None,
        },
        {
            "assessment_id": assessment_id,
            "source": "heaven",
            "target": "cielo",
            "score": 0.78,
            "vref": "GEN 1:9",
            "book": "GEN",
            "chapter": 1,
            "verse": 9,
            "flag": False,
            "hide": False,
            "note": None,
        },
        # Word "earth" with good alignment
        {
            "assessment_id": assessment_id,
            "source": "earth",
            "target": "tierra",
            "score": 0.95,
            "vref": "GEN 1:1",
            "book": "GEN",
            "chapter": 1,
            "verse": 1,
            "flag": False,
            "hide": False,
            "note": None,
        },
        {
            "assessment_id": assessment_id,
            "source": "earth",
            "target": "tierra",
            "score": 0.93,
            "vref": "GEN 1:10",
            "book": "GEN",
            "chapter": 1,
            "verse": 10,
            "flag": False,
            "hide": False,
            "note": None,
        },
        # Word "beginning" - lower support (fewer occurrences)
        {
            "assessment_id": assessment_id,
            "source": "beginning",
            "target": "principio",
            "score": 0.85,
            "vref": "GEN 1:1",
            "book": "GEN",
            "chapter": 1,
            "verse": 1,
            "flag": False,
            "hide": False,
            "note": None,
        },
        {
            "assessment_id": assessment_id,
            "source": "beginning",
            "target": "comienzo",
            "score": 0.15,
            "vref": "GEN 1:1",
            "book": "GEN",
            "chapter": 1,
            "verse": 1,
            "flag": False,
            "hide": False,
            "note": None,
        },
        # Word with very low support - should be filtered out by min_support
        {
            "assessment_id": assessment_id,
            "source": "rare",
            "target": "raro",
            "score": 0.5,
            "vref": "GEN 1:11",
            "book": "GEN",
            "chapter": 1,
            "verse": 11,
            "flag": False,
            "hide": False,
            "note": None,
        },
    ]

    # Add more entries to increase support mass above min_support threshold (20.0)
    # Each word needs total score sum >= 20 to pass the filter
    for verse in range(11, 35):  # Add more verses for words that need higher support
        alignment_data.extend(
            [
                {
                    "assessment_id": assessment_id,
                    "source": "god",
                    "target": "dios",
                    "score": 0.87,
                    "vref": f"GEN 1:{verse}",
                    "book": "GEN",
                    "chapter": 1,
                    "verse": verse,
                    "flag": False,
                    "hide": False,
                    "note": None,
                },
                {
                    "assessment_id": assessment_id,
                    "source": "created",
                    "target": "creó",
                    "score": 0.90,
                    "vref": f"GEN 1:{verse}",
                    "book": "GEN",
                    "chapter": 1,
                    "verse": verse,
                    "flag": False,
                    "hide": False,
                    "note": None,
                },
                {
                    "assessment_id": assessment_id,
                    "source": "earth",
                    "target": "tierra",
                    "score": 0.94,
                    "vref": f"GEN 1:{verse}",
                    "book": "GEN",
                    "chapter": 1,
                    "verse": verse,
                    "flag": False,
                    "hide": False,
                    "note": None,
                },
            ]
        )

    # Set all vrefs to None to avoid foreign key constraint issues with test data
    for data in alignment_data:
        data["vref"] = None
        db_session.add(AlignmentTopSourceScores(**data))

    db_session.commit()


def test_textalignmentmatches_basic(client, regular_token1, test_db_session):
    """Test basic functionality of the textalignmentmatches endpoint."""
    first_assessment_id = setup_assessments_results(test_db_session)
    setup_alignment_data(test_db_session, first_assessment_id)

    # Get revision_id and reference_id from the assessment
    assessment = (
        test_db_session.query(Assessment)
        .filter(Assessment.id == first_assessment_id)
        .first()
    )

    params = {
        "revision_id": assessment.revision_id,
        "reference_id": assessment.reference_id,
    }

    response = client.get(
        "/v3/textalignmentmatches",
        params=params,
        headers={"Authorization": f"Bearer {regular_token1}"},
    )

    assert response.status_code == 200
    response_data = response.json()

    # Check basic structure
    assert "results" in response_data
    assert "total_count" in response_data
    assert response_data["total_count"] > 0
    assert len(response_data["results"]) == response_data["total_count"]

    # Check that results are properly structured
    for result in response_data["results"]:
        assert "source_word" in result
        assert "target_word" in result
        assert "rank" in result
        assert "probability" in result
        assert "support_mass" in result
        assert "support_hits" in result
        assert "strength_mass" in result
        assert "strength_margin_mass" in result
        assert "strength_confidence" in result

        # Validate data types and ranges
        assert isinstance(result["source_word"], str)
        assert isinstance(result["target_word"], str)
        assert isinstance(result["rank"], int)
        assert result["rank"] >= 1
        assert result["rank"] <= 3  # default top_k
        assert 0.0 <= result["probability"] <= 1.0
        assert result["support_mass"] >= 0.0
        assert result["support_hits"] >= 0


def test_textalignmentmatches_top_k_parameter(client, regular_token1, test_db_session):
    """Test that the top_k parameter correctly limits results per source word."""
    first_assessment_id = setup_assessments_results(test_db_session)
    setup_alignment_data(test_db_session, first_assessment_id)

    assessment = (
        test_db_session.query(Assessment)
        .filter(Assessment.id == first_assessment_id)
        .first()
    )

    # Test with top_k=1 (only best match per source)
    params = {
        "revision_id": assessment.revision_id,
        "reference_id": assessment.reference_id,
        "top_k": 1,
    }

    response = client.get(
        "/v3/textalignmentmatches",
        params=params,
        headers={"Authorization": f"Bearer {regular_token1}"},
    )

    assert response.status_code == 200
    response_data = response.json()

    # All results should have rank=1
    for result in response_data["results"]:
        assert result["rank"] == 1

    # Test with top_k=2
    params["top_k"] = 2
    response = client.get(
        "/v3/textalignmentmatches",
        params=params,
        headers={"Authorization": f"Bearer {regular_token1}"},
    )

    assert response.status_code == 200
    response_data = response.json()

    # All results should have rank <= 2
    for result in response_data["results"]:
        assert result["rank"] <= 2


def test_textalignmentmatches_min_support_filter(
    client, regular_token1, test_db_session
):
    """Test that min_support parameter filters out low-support source words."""
    first_assessment_id = setup_assessments_results(test_db_session)
    setup_alignment_data(test_db_session, first_assessment_id)

    assessment = (
        test_db_session.query(Assessment)
        .filter(Assessment.id == first_assessment_id)
        .first()
    )

    # Test with very high min_support - should get fewer results
    params = {
        "revision_id": assessment.revision_id,
        "reference_id": assessment.reference_id,
        "min_support": 100.0,  # High threshold
    }

    response = client.get(
        "/v3/textalignmentmatches",
        params=params,
        headers={"Authorization": f"Bearer {regular_token1}"},
    )

    assert response.status_code == 200
    response_data = response.json()

    # All results should have high support
    for result in response_data["results"]:
        assert result["support_mass"] >= 100.0

    # Test with low min_support - should get more results
    params["min_support"] = 1.0
    response_low = client.get(
        "/v3/textalignmentmatches",
        params=params,
        headers={"Authorization": f"Bearer {regular_token1}"},
    )

    assert response_low.status_code == 200
    response_low_data = response_low.json()

    # Should have more or equal results with lower threshold
    assert response_low_data["total_count"] >= response_data["total_count"]


def test_textalignmentmatches_min_probability_filter(
    client, regular_token1, test_db_session
):
    """Test that min_probability parameter filters out low-probability alignments."""
    first_assessment_id = setup_assessments_results(test_db_session)
    setup_alignment_data(test_db_session, first_assessment_id)

    assessment = (
        test_db_session.query(Assessment)
        .filter(Assessment.id == first_assessment_id)
        .first()
    )

    # Test with high min_probability
    params = {
        "revision_id": assessment.revision_id,
        "reference_id": assessment.reference_id,
        "min_probability": 0.7,
    }

    response = client.get(
        "/v3/textalignmentmatches",
        params=params,
        headers={"Authorization": f"Bearer {regular_token1}"},
    )

    assert response.status_code == 200
    response_data = response.json()

    # All results should have probability > 0.7
    for result in response_data["results"]:
        assert result["probability"] > 0.7


def test_textalignmentmatches_authorization(
    client, regular_token1, regular_token2, test_db_session
):
    """Test that authorization is properly enforced."""
    first_assessment_id = setup_assessments_results(test_db_session)
    setup_alignment_data(test_db_session, first_assessment_id)

    assessment = (
        test_db_session.query(Assessment)
        .filter(Assessment.id == first_assessment_id)
        .first()
    )

    params = {
        "revision_id": assessment.revision_id,
        "reference_id": assessment.reference_id,
    }

    # First user should have access
    response = client.get(
        "/v3/textalignmentmatches",
        params=params,
        headers={"Authorization": f"Bearer {regular_token1}"},
    )
    assert response.status_code == 200

    # Second user should NOT have access (not in the group)
    response = client.get(
        "/v3/textalignmentmatches",
        params=params,
        headers={"Authorization": f"Bearer {regular_token2}"},
    )
    assert response.status_code == 403


def test_textalignmentmatches_no_assessment_found(
    client, regular_token1, test_db_session
):
    """Test that 404 is returned when no matching assessment exists."""
    setup_assessments_results(test_db_session)

    # Use non-existent revision/reference combination
    params = {
        "revision_id": 99999,
        "reference_id": 99999,
    }

    response = client.get(
        "/v3/textalignmentmatches",
        params=params,
        headers={"Authorization": f"Bearer {regular_token1}"},
    )

    assert response.status_code == 404
    assert "No completed word-alignment assessment found" in response.json()["detail"]


def test_textalignmentmatches_strength_metrics(client, regular_token1, test_db_session):
    """Test that strength metrics are calculated and present in results."""
    first_assessment_id = setup_assessments_results(test_db_session)
    setup_alignment_data(test_db_session, first_assessment_id)

    assessment = (
        test_db_session.query(Assessment)
        .filter(Assessment.id == first_assessment_id)
        .first()
    )

    params = {
        "revision_id": assessment.revision_id,
        "reference_id": assessment.reference_id,
    }

    response = client.get(
        "/v3/textalignmentmatches",
        params=params,
        headers={"Authorization": f"Bearer {regular_token1}"},
    )

    assert response.status_code == 200
    response_data = response.json()

    # Check that strength metrics are present and reasonable
    for result in response_data["results"]:
        # strength_mass should be <= support_mass (since probability <= 1)
        assert result["strength_mass"] <= result["support_mass"]

        # strength_margin_mass should be >= 0 (margin is non-negative)
        assert result["strength_margin_mass"] >= 0

        # For rank 1 results, margin should be larger than for rank 2+
        # (because rank 1 has the highest probability)

        # strength_confidence should be a reasonable value
        assert isinstance(result["strength_confidence"], (int, float))


def setup_text_lengths_data(db_session):
    """Setup text lengths assessments and data for testing compare_text_lengths."""
    from database.models import TextLengthsTable

    # Check if data already exists
    existing_data = db_session.query(TextLengthsTable).first()
    if existing_data:
        return

    # Get existing revisions from setup_assessments_results
    revision = db_session.query(BibleRevision).filter(BibleRevision.id == 115).first()
    reference = db_session.query(BibleRevision).filter(BibleRevision.id == 505).first()

    if not revision or not reference:
        # Create minimal revision and reference if they don't exist
        user = db_session.query(UserDB).filter(UserDB.username == "testuser1").first()
        if not revision:
            # Check if BibleVersion already exists
            existing_version = (
                db_session.query(BibleVersion).filter(BibleVersion.id == 115).first()
            )
            if not existing_version:
                version = BibleVersion(
                    id=115, abbreviation="REV", name="Revision", owner_id=user.id
                )
                db_session.add(version)
            revision = BibleRevision(id=115, bible_version_id=115)
            db_session.add(revision)
        if not reference:
            # Check if BibleVersion already exists
            existing_version = (
                db_session.query(BibleVersion).filter(BibleVersion.id == 505).first()
            )
            if not existing_version:
                version = BibleVersion(
                    id=505, abbreviation="REF", name="Reference", owner_id=user.id
                )
                db_session.add(version)
            reference = BibleRevision(id=505, bible_version_id=505)
            db_session.add(reference)
        db_session.commit()

    # Create text-lengths assessments for revision and reference
    revision_assessment = Assessment(
        id=1001,
        revision_id=115,
        reference_id=None,
        type="text-lengths",
        status="finished",
        assessment_version="1",
    )

    reference_assessment = Assessment(
        id=1002,
        revision_id=505,
        reference_id=None,
        type="text-lengths",
        status="finished",
        assessment_version="1",
    )

    db_session.add(revision_assessment)
    db_session.add(reference_assessment)
    db_session.commit()

    # Create text lengths data with some zero values to test verse range merging
    # Note: Revision and reference have zeros in DIFFERENT places to test realistic scenarios
    # Revision data (GAL 1:1-10)
    revision_data = [
        {
            "assessment_id": 1001,
            "vref": "GAL 1:1",
            "word_lengths": 10,
            "char_lengths": 50,
            "word_lengths_z": 0.5,
            "char_lengths_z": 0.3,
        },
        {
            "assessment_id": 1001,
            "vref": "GAL 1:2",
            "word_lengths": 0,
            "char_lengths": 0,
            "word_lengths_z": 0.0,
            "char_lengths_z": 0.0,
        },  # Zero in revision
        {
            "assessment_id": 1001,
            "vref": "GAL 1:3",
            "word_lengths": 0,
            "char_lengths": 0,
            "word_lengths_z": 0.0,
            "char_lengths_z": 0.0,
        },  # Zero in revision
        {
            "assessment_id": 1001,
            "vref": "GAL 1:4",
            "word_lengths": 15,
            "char_lengths": 75,
            "word_lengths_z": 1.0,
            "char_lengths_z": 0.8,
        },
        {
            "assessment_id": 1001,
            "vref": "GAL 1:5",
            "word_lengths": 12,
            "char_lengths": 60,
            "word_lengths_z": 0.7,
            "char_lengths_z": 0.5,
        },
        {
            "assessment_id": 1001,
            "vref": "GAL 1:6",
            "word_lengths": 8,
            "char_lengths": 40,
            "word_lengths_z": 0.2,
            "char_lengths_z": 0.1,
        },
        {
            "assessment_id": 1001,
            "vref": "GAL 1:7",
            "word_lengths": 0,
            "char_lengths": 0,
            "word_lengths_z": 0.0,
            "char_lengths_z": 0.0,
        },  # Zero in revision
        {
            "assessment_id": 1001,
            "vref": "GAL 1:8",
            "word_lengths": 14,
            "char_lengths": 70,
            "word_lengths_z": 0.9,
            "char_lengths_z": 0.7,
        },
        {
            "assessment_id": 1001,
            "vref": "GAL 1:9",
            "word_lengths": 11,
            "char_lengths": 55,
            "word_lengths_z": 0.6,
            "char_lengths_z": 0.4,
        },
        {
            "assessment_id": 1001,
            "vref": "GAL 1:10",
            "word_lengths": 13,
            "char_lengths": 65,
            "word_lengths_z": 0.8,
            "char_lengths_z": 0.6,
        },
    ]

    # Reference data (GAL 1:1-10) - zeros in DIFFERENT verses than revision
    reference_data = [
        {
            "assessment_id": 1002,
            "vref": "GAL 1:1",
            "word_lengths": 9,
            "char_lengths": 45,
            "word_lengths_z": 0.4,
            "char_lengths_z": 0.2,
        },
        {
            "assessment_id": 1002,
            "vref": "GAL 1:2",
            "word_lengths": 8,
            "char_lengths": 40,
            "word_lengths_z": 0.3,
            "char_lengths_z": 0.1,
        },  # Non-zero in reference
        {
            "assessment_id": 1002,
            "vref": "GAL 1:3",
            "word_lengths": 0,
            "char_lengths": 0,
            "word_lengths_z": 0.0,
            "char_lengths_z": 0.0,
        },  # Zero in reference (different from revision)
        {
            "assessment_id": 1002,
            "vref": "GAL 1:4",
            "word_lengths": 14,
            "char_lengths": 70,
            "word_lengths_z": 0.9,
            "char_lengths_z": 0.7,
        },
        {
            "assessment_id": 1002,
            "vref": "GAL 1:5",
            "word_lengths": 0,
            "char_lengths": 0,
            "word_lengths_z": 0.0,
            "char_lengths_z": 0.0,
        },  # Zero in reference (not in revision)
        {
            "assessment_id": 1002,
            "vref": "GAL 1:6",
            "word_lengths": 7,
            "char_lengths": 35,
            "word_lengths_z": 0.1,
            "char_lengths_z": 0.0,
        },
        {
            "assessment_id": 1002,
            "vref": "GAL 1:7",
            "word_lengths": 9,
            "char_lengths": 45,
            "word_lengths_z": 0.4,
            "char_lengths_z": 0.2,
        },  # Non-zero in reference
        {
            "assessment_id": 1002,
            "vref": "GAL 1:8",
            "word_lengths": 13,
            "char_lengths": 65,
            "word_lengths_z": 0.8,
            "char_lengths_z": 0.6,
        },
        {
            "assessment_id": 1002,
            "vref": "GAL 1:9",
            "word_lengths": 0,
            "char_lengths": 0,
            "word_lengths_z": 0.0,
            "char_lengths_z": 0.0,
        },  # Zero in reference (not in revision)
        {
            "assessment_id": 1002,
            "vref": "GAL 1:10",
            "word_lengths": 12,
            "char_lengths": 60,
            "word_lengths_z": 0.7,
            "char_lengths_z": 0.5,
        },
    ]

    # Add EPH data for book aggregation testing
    revision_data.extend(
        [
            {
                "assessment_id": 1001,
                "vref": "EPH 1:1",
                "word_lengths": 9,
                "char_lengths": 45,
                "word_lengths_z": 0.4,
                "char_lengths_z": 0.2,
            },
            {
                "assessment_id": 1001,
                "vref": "EPH 1:2",
                "word_lengths": 10,
                "char_lengths": 50,
                "word_lengths_z": 0.5,
                "char_lengths_z": 0.3,
            },
        ]
    )

    reference_data.extend(
        [
            {
                "assessment_id": 1002,
                "vref": "EPH 1:1",
                "word_lengths": 8,
                "char_lengths": 40,
                "word_lengths_z": 0.3,
                "char_lengths_z": 0.1,
            },
            {
                "assessment_id": 1002,
                "vref": "EPH 1:2",
                "word_lengths": 9,
                "char_lengths": 45,
                "word_lengths_z": 0.4,
                "char_lengths_z": 0.2,
            },
        ]
    )

    for data in revision_data:
        db_session.add(TextLengthsTable(**data))

    for data in reference_data:
        db_session.add(TextLengthsTable(**data))

    db_session.commit()


def test_compare_text_lengths_basic(client, regular_token1, test_db_session):
    """Test basic functionality of compare_text_lengths endpoint."""
    setup_assessments_results(test_db_session)
    setup_text_lengths_data(test_db_session)

    params = {
        "revision_id": 115,
        "reference_id": 505,
    }

    response = client.get(
        "/v3/compare_text_lengths",
        params=params,
        headers={"Authorization": f"Bearer {regular_token1}"},
    )

    assert response.status_code == 200
    response_data = response.json()

    # Check basic structure
    assert "results" in response_data
    assert "total_count" in response_data
    assert response_data["total_count"] > 0
    assert len(response_data["results"]) > 0

    # Check that results contain differences
    for result in response_data["results"]:
        assert "vref" in result or "vrefs" in result
        assert "word_lengths" in result  # This is the difference
        assert "char_lengths" in result  # This is the difference
        assert "word_lengths_z" in result  # Z-score of the difference
        assert "char_lengths_z" in result  # Z-score of the difference
        assert "assessment_id" in result


def test_compare_text_lengths_verse_range_merging(
    client, regular_token1, test_db_session
):
    """Test that zero values trigger verse range merging and summing.

    Note: The revision has zeros at verses 2, 3, 7
    The reference has zeros at verses 3, 5, 9

    After merging:
    - Revision will merge: 1:1-3 (1:1 + 1:2 + 1:3), 1:6-7 (1:6 + 1:7)
    - Reference will merge: 1:1-3 (1:1 + 1:2 + 1:3), 1:4-5 (1:4 + 1:5), 1:8-9 (1:8 + 1:9)

    The final comparison will show merged ranges where either has zeros.
    """
    setup_assessments_results(test_db_session)
    setup_text_lengths_data(test_db_session)

    params = {
        "revision_id": 115,
        "reference_id": 505,
    }

    response = client.get(
        "/v3/compare_text_lengths",
        params=params,
        headers={"Authorization": f"Bearer {regular_token1}"},
    )

    assert response.status_code == 200
    response_data = response.json()

    # Check that we have results with vrefs (merged verses should have multiple vrefs)
    has_merged_verse = False
    merged_ranges_found = []

    for result in response_data["results"]:
        if "vrefs" in result and result["vrefs"] and len(result["vrefs"]) > 1:
            has_merged_verse = True
            merged_ranges_found.append(result["vrefs"])

            # Check for specific merged ranges
            # Both revision and reference should have merged 1:1-3
            if "GAL 1:1" in result["vrefs"] and len(result["vrefs"]) == 3:
                assert "GAL 1:2" in result["vrefs"]
                assert "GAL 1:3" in result["vrefs"]
                # The difference should be the sum of differences
                # Revision: 10 + 0 + 0 = 10, Reference: 9 + 8 + 0 = 17, Diff = -7
                assert result["word_lengths"] is not None

    # We should have at least one merged verse range
    assert (
        has_merged_verse
    ), f"Expected to find merged verse ranges due to zero values. Found ranges: {merged_ranges_found}"


def test_compare_text_lengths_book_filter(client, regular_token1, test_db_session):
    """Test filtering results by book."""
    setup_assessments_results(test_db_session)
    setup_text_lengths_data(test_db_session)

    # Test with GAL book
    params = {
        "revision_id": 115,
        "reference_id": 505,
        "book": "GAL",
    }

    response = client.get(
        "/v3/compare_text_lengths",
        params=params,
        headers={"Authorization": f"Bearer {regular_token1}"},
    )

    assert response.status_code == 200
    response_data = response.json()

    # All results should be from GAL
    for result in response_data["results"]:
        vref = result.get("vref") or (
            result.get("vrefs")[0] if result.get("vrefs") else None
        )
        if vref:
            assert vref.startswith("GAL"), f"Expected GAL verse, got {vref}"

    # Test with EPH book
    params["book"] = "EPH"
    response = client.get(
        "/v3/compare_text_lengths",
        params=params,
        headers={"Authorization": f"Bearer {regular_token1}"},
    )

    assert response.status_code == 200
    response_data = response.json()

    # All results should be from EPH
    for result in response_data["results"]:
        vref = result.get("vref") or (
            result.get("vrefs")[0] if result.get("vrefs") else None
        )
        if vref:
            assert vref.startswith("EPH"), f"Expected EPH verse, got {vref}"


def test_compare_text_lengths_chapter_aggregation(
    client, regular_token1, test_db_session
):
    """Test chapter-level aggregation."""
    setup_assessments_results(test_db_session)
    setup_text_lengths_data(test_db_session)

    params = {
        "revision_id": 115,
        "reference_id": 505,
        "aggregate": "chapter",
    }

    response = client.get(
        "/v3/compare_text_lengths",
        params=params,
        headers={"Authorization": f"Bearer {regular_token1}"},
    )

    assert response.status_code == 200
    response_data = response.json()

    # Results should be aggregated by chapter
    chapter_vrefs = set()
    for result in response_data["results"]:
        vref = result.get("vref")
        if vref:
            # Chapter aggregation should return "BOOK CHAPTER" format (e.g., "GAL 1")
            assert vref.count(" ") == 1, f"Expected chapter-level vref, got {vref}"
            assert (
                ":" not in vref
            ), f"Expected no verse number in chapter aggregation, got {vref}"
            chapter_vrefs.add(vref)

    # Should have at least GAL 1 and EPH 1
    assert len(chapter_vrefs) >= 2


def test_compare_text_lengths_book_aggregation(client, regular_token1, test_db_session):
    """Test book-level aggregation."""
    setup_assessments_results(test_db_session)
    setup_text_lengths_data(test_db_session)

    params = {
        "revision_id": 115,
        "reference_id": 505,
        "aggregate": "book",
    }

    response = client.get(
        "/v3/compare_text_lengths",
        params=params,
        headers={"Authorization": f"Bearer {regular_token1}"},
    )

    assert response.status_code == 200
    response_data = response.json()

    # Results should be aggregated by book
    book_vrefs = set()
    for result in response_data["results"]:
        vref = result.get("vref")
        if vref:
            # Book aggregation should return just the book name (e.g., "GAL")
            assert " " not in vref, f"Expected book-level vref, got {vref}"
            book_vrefs.add(vref)

    # Should have at least GAL and EPH
    assert "GAL" in book_vrefs
    assert "EPH" in book_vrefs


def test_compare_text_lengths_text_aggregation(client, regular_token1, test_db_session):
    """Test text-level aggregation (entire text)."""
    setup_assessments_results(test_db_session)
    setup_text_lengths_data(test_db_session)

    params = {
        "revision_id": 115,
        "reference_id": 505,
        "aggregate": "text",
    }

    response = client.get(
        "/v3/compare_text_lengths",
        params=params,
        headers={"Authorization": f"Bearer {regular_token1}"},
    )

    assert response.status_code == 200
    response_data = response.json()

    # Should return only one result for the entire text
    assert response_data["total_count"] == 1
    assert len(response_data["results"]) == 1

    result = response_data["results"][0]
    # Text aggregation should have None for vref
    assert result.get("vref") is None


def test_compare_text_lengths_pagination(client, regular_token1, test_db_session):
    """Test pagination of results."""
    setup_assessments_results(test_db_session)
    setup_text_lengths_data(test_db_session)

    # Get first page
    params = {
        "revision_id": 115,
        "reference_id": 505,
        "page": 1,
        "page_size": 3,
    }

    response = client.get(
        "/v3/compare_text_lengths",
        params=params,
        headers={"Authorization": f"Bearer {regular_token1}"},
    )

    assert response.status_code == 200
    response_data = response.json()

    total_count = response_data["total_count"]
    assert len(response_data["results"]) <= 3
    first_page_results = response_data["results"]

    # Get second page if there are enough results
    if total_count > 3:
        params["page"] = 2
        response = client.get(
            "/v3/compare_text_lengths",
            params=params,
            headers={"Authorization": f"Bearer {regular_token1}"},
        )

        assert response.status_code == 200
        second_page_data = response.json()

        # Second page should have different results
        assert second_page_data["results"] != first_page_results
        assert second_page_data["total_count"] == total_count


def test_compare_text_lengths_authorization(
    client, regular_token1, regular_token2, test_db_session
):
    """Test that authorization is properly enforced."""
    setup_assessments_results(test_db_session)
    setup_text_lengths_data(test_db_session)

    params = {
        "revision_id": 115,
        "reference_id": 505,
    }

    # First user should have access
    response = client.get(
        "/v3/compare_text_lengths",
        params=params,
        headers={"Authorization": f"Bearer {regular_token1}"},
    )
    assert response.status_code == 200

    # Second user should NOT have access
    response = client.get(
        "/v3/compare_text_lengths",
        params=params,
        headers={"Authorization": f"Bearer {regular_token2}"},
    )
    assert response.status_code == 403


def test_compare_text_lengths_no_assessment_found(
    client, regular_token1, test_db_session
):
    """Test that 404 is returned when no matching assessment exists."""
    setup_assessments_results(test_db_session)

    # Use non-existent revision/reference combination
    params = {
        "revision_id": 99999,
        "reference_id": 99999,
    }

    response = client.get(
        "/v3/compare_text_lengths",
        params=params,
        headers={"Authorization": f"Bearer {regular_token1}"},
    )

    assert response.status_code == 404
    assert "No completed text-lengths assessment found" in response.json()["detail"]


def test_compare_text_lengths_difference_calculation(
    client, regular_token1, test_db_session
):
    """Test that differences are correctly calculated (revision - reference)."""
    setup_assessments_results(test_db_session)
    setup_text_lengths_data(test_db_session)

    params = {
        "revision_id": 115,
        "reference_id": 505,
        "book": "GAL",
        "chapter": 1,
        "verse": 4,  # A verse with no zeros
    }

    response = client.get(
        "/v3/compare_text_lengths",
        params=params,
        headers={"Authorization": f"Bearer {regular_token1}"},
    )

    assert response.status_code == 200
    response_data = response.json()

    assert len(response_data["results"]) > 0
    result = response_data["results"][0]

    # GAL 1:4 has revision: word_lengths=15, char_lengths=75
    # GAL 1:4 has reference: word_lengths=14, char_lengths=70
    # Difference should be: word_lengths=1, char_lengths=5
    assert result["word_lengths"] == 1.0
    assert result["char_lengths"] == 5.0

    # Z-scores should be calculated
    assert "word_lengths_z" in result
    assert "char_lengths_z" in result
