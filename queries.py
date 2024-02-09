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


def update_version_name_query():
    update_version_name = """
                        UPDATE "bibleVersion"
                          SET name=(%s)
                          WHERE id=(%s)
                        RETURNING id, name, isolanguage, isoscript,
                    abbreviation, rights, forwardtranslation,
                    backtranslation, machinetranslation;
                        """

    return update_version_name

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
                    bibleversion, name, date, published, backTranslation, machineTranslation
                    )
                  VALUES ((%s), (%s), (%s), (%s), (%s), (%s))
                RETURNING 
                  id, date, bibleversion, published, name, backTranslation, machineTranslation;
                """
 
    return bible_revise


def update_revision_name_query():
    update_revision_name = """
                        UPDATE "bibleRevision"
                          SET name=(%s)
                          WHERE id=(%s)
                        RETURNING id, date, bibleversion, published,
                        name, backtranslation, machinetranslation;
                        """
    return update_revision_name


def fetch_bible_version_by_abbreviation():
    version_id = """
                SELECT * FROM "bibleVersion"
                  WHERE abbreviation=(%s);
                """
        
    return version_id


def list_all_revisions_query():
    list_revisions = """
                    SELECT br.id, br.date, br.bibleversion, br.published, br.name, br.backtranslation, br.machinetranslation, bv.isoLanguage, bv.abbreviation
                    FROM "bibleRevision" br
                    INNER JOIN "bibleVersion" bv ON br.bibleversion = bv.id
                    """

    return list_revisions


def list_revisions_query():
    list_revisions = """
                    SELECT br.id, br.date, br.bibleversion, br.published, br.name, br.backtranslation, br.machinetranslation, bv.isoLanguage, bv.abbreviation
                    FROM "bibleRevision" br
                    INNER JOIN "bibleVersion" bv ON br.bibleversion = bv.id
                      WHERE bibleversion=(%s)
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
                    SELECT abbreviation, backTranslation FROM "bibleVersion"
                      WHERE id=(%s);
                    """

    return fetch_version


def get_chapter_query(chapterReference):
    get_chapter = """
                SELECT * FROM "verseText" "vt"
                  INNER JOIN "verseReference" "vr" ON vt.versereference = fullverseid
                  WHERE biblerevision=(%s)
                    AND vr.chapter = {}
                    ORDER BY vt.id;
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
                    WHERE vt.biblerevision=(%s) and vt.book = (%s)
                    ORDER BY id;
                """

    return get_book


def get_text_query():
    get_text = """
            SELECT * FROM "verseText"
              WHERE biblerevision=(%s) ORDER BY id;
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
                  and vref LIKE ($4) || '%'
                  AND (source IS NULL) = ($5)
                  ORDER BY id
                  LIMIT ($2)
                  OFFSET ($3);
                """

    return get_results


def get_results_agg_query():
    get_results_agg = """
                SELECT COUNT(id)
                  FROM "assessmentResult"
                    WHERE assessment=($1)
                  AND (source IS NULL) = ($3)
                  and vref LIKE ($2) || '%';
                """

    return get_results_agg


def get_results_chapter_query():
    get_results_chapter = """
                SELECT * FROM "group_results_chapter"
                  WHERE assessment=($1)
                  and vref_group LIKE ($4) || '%'
                  AND (source IS NULL) = ($5)
                  ORDER BY id
                  LIMIT ($2)
                  OFFSET ($3);
                """
    
    return get_results_chapter
    

def get_results_chapter_agg_query():
    get_results_chapter_agg = """
                SELECT COUNT(id)
                  FROM "group_results_chapter"
                    WHERE assessment=($1)
                  and vref_group LIKE ($2) || '%'
                  AND (source IS NULL) = ($3)
                  ;
                """

    return get_results_chapter_agg


def get_results_book_query():
    get_results_book = """
                SELECT * FROM "group_results_book"
                  WHERE assessment=($1)
                  and vref_group LIKE ($4) || '%'
                  AND (source IS NULL) = ($5)
                  ORDER BY id
                  LIMIT ($2)
                  OFFSET ($3);
                """

    return get_results_book


def get_results_book_agg_query():
    get_results_book_agg = """
                SELECT COUNT(id)
                  FROM "group_results_book"
                    WHERE assessment=($1)
                  and vref_group LIKE ($2) || '%'
                  AND (source IS NULL) = ($3)
                  ;
                """

    return get_results_book_agg


def get_results_text_query():
    get_results_text = """
                SELECT * FROM "group_results_text"
                   WHERE assessment=($1)
                  AND (source IS NULL) = ($4)
                    ORDER BY id
                   LIMIT ($2)
                   OFFSET ($3);
                """

    return get_results_text


def get_results_text_agg_query():
    get_results_text_agg = """
                SELECT COUNT(id)
                  FROM "group_results_text"
                    WHERE assessment=($1)
                  AND (source IS NULL) = ($2);
                """

    return get_results_text_agg


def get_results_with_text_query():
    get_results_with_text = """
                SELECT * FROM "assessment_result_with_text"
                  WHERE assessment=($1) 
                  and vref LIKE ($4) || '%'
                  AND (source IS NULL) = ($5)
                  ORDER BY id
                  LIMIT ($2)
                  OFFSET ($3);
                """

    return get_results_with_text


def get_results_with_text_agg_query():
    get_results_with_text_agg = """
                SELECT COUNT(id)
                  FROM "assessment_result_with_text"
                    WHERE assessment=($1)
                  and vref LIKE ($2) || '%'
                  AND (source IS NULL) = ($3)
                  ;
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
                  WHERE assessment=(%s)
                  AND (source IS NULL) = (%s)
                  ;
                """

    return get_results


def get_results_chapter_query_v1():
    get_results_chapter = """
                SELECT * FROM "group_results_chapter"
                  WHERE assessment=(%s)
                  AND (source IS NULL) = (%s)
                  ;
                """
    
    return get_results_chapter


def get_results_with_text_query_v1():
    get_results_with_text = """
                SELECT * FROM "assessment_result_with_text"
                  WHERE assessment=(%s)
                  AND (source IS NULL) = (%s)
                  ;
                """

    return get_results_with_text


def get_alignment_scores_like_query():
    get_alignment_scores = """
                SELECT * FROM "alignmentTopSourceScores"
                  WHERE assessment=($1) 
                  and vref LIKE ($4) || '%'
                  ORDER BY id
                  LIMIT ($2)
                  OFFSET ($3);
                """

    return get_alignment_scores


def get_alignment_scores_exact_query():
    get_alignment_scores = """
                SELECT * FROM "alignmentTopSourceScores"
                  WHERE assessment=($1) 
                  and vref = ($4)
                  ORDER BY id
                  LIMIT ($2)
                  OFFSET ($3);
                """

    return get_alignment_scores


def get_alignment_scores_agg_like_query():
    get_alignment_scores_agg = """
                SELECT COUNT(id)
                  FROM "alignmentTopSourceScores"
                    WHERE assessment=($1)
                  and vref LIKE ($2) || '%';
                """

    return get_alignment_scores_agg


def get_alignment_scores_agg_exact_query():
    get_alignment_scores_agg = """
                SELECT COUNT(id)
                  FROM "alignmentTopSourceScores"
                    WHERE assessment=($1)
                  and vref = ($2);
                """

    return get_alignment_scores_agg


def get_word_alignments_query():
    get_word_alignments = """
                SELECT vt1.*, vt2.text AS "reference_text", als.target, als.score
                FROM "verseText" vt1
                JOIN "verseText" vt2 ON vt1.versereference = vt2.versereference
                JOIN "alignmentTopSourceScores" als ON vt1.versereference = als.vref
                WHERE vt1.biblerevision = ($1)
                AND vt2.biblerevision = ($2)
                AND als.assessment = ($3)
                AND als.source = ($4)
                AND vt1.text ~* ($5)
                ORDER BY vt1.id
                """

    return get_word_alignments