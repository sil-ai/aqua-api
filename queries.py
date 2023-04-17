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
                  id, date, bibleversion, published, name
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
                    """

    return list_revisions


def version_data_revisions_query():
    version_data_revisions = """
                    SELECT id, abbreviation FROM "bibleVersion"
                      WHERE id=(%s);
                    """

    return version_data_revisions


def fetch_version_data():
    fetch_version = """
                    SELECT abbreviation FROM "bibleVersion"
                      WHERE id=(%s);
                    """

    return fetch_version


def get_chapter_query(chapterReference):
    get_chapter = """
                SELECT * FROM "verseText" "vt"
                  INNER JOIN "verseReference" "vr" ON vt.versereference = fullverseid
                    WHERE vr.chapter = {};
                """.format(chapterReference)

    return get_chapter


def get_verses_query(verseReference):
    get_verses = """
                SELECT * FROM "verseText"
                  WHERE biblerevision=(%s)
                    AND versereference={};
                """.format(verseReference)
    
    return get_verses


def get_book_query():
    get_book = """
                SELECT * FROM "verseText" "vt"
                  INNER JOIN "verseReference" "vr" ON vt.versereference = fullverseid
                    WHERE vr.book = (%s);
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
                  WHERE assessment=($1)
                  LIMIT ($2)
                  OFFSET ($3);
                """

    return get_results


def get_results_agg_query():
    get_results_agg = """
                SELECT COUNT(id)
                  FROM "assessmentResult"
                    WHERE assessment=($1);
                """

    return get_results_agg


def get_results_chapter_query():
    get_results_chapter = """
                SELECT * FROM "group_results_chapter"
                  WHERE assessment=($1)
                  ORDER BY id
                  LIMIT ($2)
                  OFFSET ($3);
                """
    
    return get_results_chapter
    

def get_results_chapter_agg_query():
    get_results_chapter_agg = """
                SELECT COUNT(id)
                  FROM "group_results_chapter"
                    WHERE assessment=($1);
                """

    return get_results_chapter_agg


def get_results_book_query():
    get_results_book = """
                SELECT * FROM "group_results_book"
                  WHERE assessment=($1)
                  ORDER BY id
                  LIMIT ($2)
                  OFFSET ($3);
                """

    return get_results_book


def get_results_book_agg_query():
    get_results_book_agg = """
                SELECT COUNT(id)
                  FROM "group_results_book"
                    WHERE assessment=($1);
                """

    return get_results_book_agg


def get_results_text_query():
    get_results_text = """
                SELECT * FROM "group_results_text"
                   WHERE assessment=($1)
                    ORDER BY id
                   LIMIT ($2)
                   OFFSET ($3);
                """

    return get_results_text


def get_results_text_agg_query():
    get_results_text_agg = """
                SELECT COUNT(id)
                  FROM "group_results_text"
                    WHERE assessment=($1);
                """

    return get_results_text_agg


def get_results_with_text_query():
    get_results_with_text = """
                SELECT * FROM "assessment_result_with_text"
                  WHERE assessment=($1)
                  ORDER BY id
                  LIMIT ($2)
                  OFFSET ($3);
                """

    return get_results_with_text


def get_results_with_text_agg_query():
    get_results_with_text_agg = """
                SELECT COUNT(id)
                  FROM "assessment_result_with_text"
                    WHERE assessment=($1);
                """

    return get_results_with_text_agg


def get_scripts_query():
    iso_scripts = 'SELECT * FROM "isoScript";'
        
    return iso_scripts


def get_languages_query():
    iso_languages = 'SELECT * FROM "isoLanguage";'
         
    return iso_languages

#delete when v1 is removed
def get_results_query_v1():
    get_results = """
                SELECT * FROM "assessmentResult"
                  WHERE assessment=(%s);
                """

    return get_results


def get_results_chapter_query_v1():
    get_results_chapter = """
                SELECT * FROM "group_results_chapter"
                  WHERE assessment=(%s);
                """
    
    return get_results_chapter


def get_results_with_text_query_v1():
    get_results_with_text = """
                SELECT * FROM "assessment_result_with_text"
                  WHERE assessment=(%s);
                """

    return get_results_with_text
