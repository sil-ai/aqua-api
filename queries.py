def list_versions_query():
    list_version = 'SELECT * FROM "bibleVersion"'
        
    return list_version


def add_version_query():
    add_version = """
                INSERT INTO "bibleVersion" (
                    name, isolanguage, isoscript,
                    abbreviation, rights, forwardtranslation,
                    backtranslation, machinetranslation)
                  VALUES ((%s), (%s), (%s), (%s), (%s), (%s), (%s), (%s))
                  RETURNING 
                      id, name, isolanguage, isoscript,
                      abbreviation, rights, forwardtranslation,
                      backtranslation, machinetranslation;
                """

    return add_version


def delete_bible_version():
    delete_version = """
                    DELETE FROM  "bibleVersion"
                      WHERE id=(%s)
                    RETURNING name; 
                    """

    return delete_version


def delete_revision_mutation():
    delete_revision = """
                    DELETE FROM "bibleRevision"
                      WHERE id=(%s)
                    RETURNING id;
                    """

    return delete_revision


def check_revisions_query():
    check_revision = """
                    SELECT id FROM "bibleRevision";
                    """

    return check_revision


def delete_verses_mutation():
    delete_verses = """
                    DELETE FROM "verseText"
                      WHERE biblerevision=(%s);
                    """
    
    return delete_verses


def insert_bible_revision():
    bible_revise = """
                INSERT INTO "bibleRevision" (
                    bibleversion, name, date, published
                    )
                  VALUES ((%s), (%s), (%s), (%s))
                RETURNING 
                  id, name, date, published
                """
 
    return bible_revise


def fetch_bible_version_by_abbreviation():
    version_id = """
                SELECT * FROM "bibleVersion"
                  WHERE abbreviation=(%s);
                """
        
    return version_id


def list_all_revisions_query():
    list_revisions = """
                    SELECT * FROM "bibleRevision";
                    """

    return list_revisions


def list_revisions_query():
    list_revisions = """
                    SELECT * FROM "bibleRevision"
                      WHERE bibleversion=(%s);

                    SELECT id, abbreviation FROM "bibleVersion"
                      WHERE id=(%s); 
                    """

    return list_revisions


def fetch_version_data():
    fetch_version = """
                    SELECT abbreviation FROM bibleVersion
                      WHERE id=(%s);
                    """

    return fetch_version


def get_chapter_query():
    get_chapter = """
                SELECT * FROM "verseText"
                  WHERE biblerevision=(%s)
                    AND chapterreference=(%s);
                """

    return get_chapter


def get_verses_query():
    get_verses = """
                SELECT * FROM "verseText"
                  WHERE biblerevision=(%s)
                    AND versereference=(%s);
                """
    
    return get_verses


def get_book_query():
    get_book = """
            SELECT * FROM "verseText"
              WHERE biblerevision=(%s)
                AND bookreference=(%s);
            """

    return get_book


def get_text_query():
    get_text = """
            SELECT * FROM "verseText"
              WHERE biblerevision=(%s);
            """

    return get_text


def list_assessments_query():
    list_assessment = """
                    SELECT * FROM "assessment";
                    """
        
    return list_assessment


def add_assessment_query():
    add_assessment = """
                    INSERT INTO "assessment" (
                        revision, reference, type,
                        requested_time, status
                        )
                      VALUES ((%s), (%s), (%s), (%s), (%s))
                    RETURNING 
                      id, revision, reference, type, 
                      requested_time, status;
                    """

    return add_assessment


def check_assessments_query():
    check_assessment = """
                    SELECT id FROM "assessment";
                    """

    return check_assessment


def delete_assessment_mutation():
    delete_assessment = """
                        DELETE FROM "assessment"
                          WHERE id=(%s)
                        RETURNING id;
                        """

    return delete_assessment


def delete_assessment_results_mutation():
    delete_assessment_results = """
                        DELETE FROM "assessmentResult"
                          WHERE assessment=(%s);
                        """

    return delete_assessment_results


def get_results_query():
    get_results = """
                SELECT * FROM "assessmentResult"
                  WHERE assessment=(%s)
                  LIMIT (%s)
                  OFFSET (%s)

                query {{
                  assessmentResult(
                    limit: {}
                    offset: {}
                    where: {{
                      assessment: {{
                        _eq: {}
                      }}
                    }}
                  ) 
                  
                  {{
                    id
                    score
                    flag
                    note
                    vref
                    source
                    target
                    assessmentByAssessment {{
                      id
                    }}
                  }}
                  assessmentResult_aggregate(
                    where: {{
                        assessment: {{
                            _eq: {}
                        }}
                    }}
                  ) {{
    aggregate {{
      count
    }}
  }}
                }}
                """

    return get_results


def get_results_chapter_agg_query(assessment_id, limit='null', offset=0):
    get_results_chapter_agg = """
                query {{
                    group_results_chapter(
                        limit: {}
                        offset: {}
                        where: {{assessment: {{
                            _eq: {}
                            }}
                            }}
                            ) {{
                    score
                    vref_group
                    assessment
                    source
                    target
                    note
                    flag
                    }}
                
                group_results_chapter_aggregate(
                    where: {{
                        assessment: {{
                            _eq: {}
                        }}
                    }}
                  ) {{
    aggregate {{
      count
    }}
  }}
                }}
                """
    
    return get_results_chapter_agg
    

def get_results_book_agg_query(assessment_id, limit='null', offset=0):
    get_results_book_agg = """
                query {{
                    group_results_book(
                        limit: {}
                        offset: {}
                        where: {{assessment: {{
                            _eq: {}
                            }}
                            }}
                            ) {{
                    score
                    vref_group
                    assessment
                    source
                    target
                    note
                    flag
                    }}
                
                group_results_book_aggregate(
                    where: {{
                        assessment: {{
                            _eq: {}
                        }}
                    }}
                  ) {{
    aggregate {{
      count
    }}
  }}
                }}
                """

    return get_results_book_agg


def get_results_text_agg_query(assessment_id, limit='null', offset=0):
    get_results_text_agg = """
                query {{
                    group_results_text(
                        limit: {}
                        offset: {}
                        where: {{assessment: {{
                            _eq: {}
                            }}
                            }}
                            ) {{
                    score
                    assessment
                    source
                    target
                    note
                    flag
                    }}
                
                group_results_text_aggregate(
                    where: {{
                        assessment: {{
                            _eq: {}
                        }}
                    }}
                  ) {{
    aggregate {{
      count
    }}
  }}
                }}
                """

    return get_results_text_agg


def get_results_with_text_query(assessment_id, limit='null', offset=0):
    get_results_with_text = """
                SELECT * FROM "assessment_result_with_text"
                  WHERE assessment=(%s)
                  LIMIT (%s)
                  OFFSET (%s);

                SELECT COUNT(id)
                  FROM assessment_result_with_text
                    WHERE assessment=(%s);
                """

    return get_results_with_text


def get_scripts_query():
    iso_scripts = 'SELECT * FROM "isoScript"';
        
    return iso_scripts


def get_languages_query():
    iso_languages = 'SELECT * FROM "isoLanguage"';
         
    return iso_languages
