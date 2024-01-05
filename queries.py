def list_versions_query():
    list_version = 'SELECT * FROM "bible_version"'
    return list_version
  
def add_chapter_reference():
    chapter_reference_sql = """
                INSERT INTO "chapter_reference" (
                    full_chapter_id, number, book_reference)
                  VALUES ((%s), (%s), (%s))
                  RETURNING 
                    full_chapter_id, number, book_reference
                """
    return chapter_reference_sql
   
  
def add_book_reference():
    book_reference_sql = """
                INSERT INTO "book_reference" (
                    abbreviation, name, number)
                  VALUES ((%s), (%s), (%s))
                  RETURNING 
                    abbreviation, name, number
                """
    return book_reference_sql

def add_verse_reference():
    verse_reference_sql = """
                INSERT INTO "verse_reference" (
                    full_verse_id, number, chapter, book_reference )
                  VALUES ((%s), (%s), (%s), (%s))
                  RETURNING 
                    full_verse_id, number, chapter, book_reference
                """
    return verse_reference_sql


def add_iso_language():
    iso_language = """
                INSERT INTO "iso_language" (
                    iso639, name )
                  VALUES ((%s), (%s))
                  RETURNING 
                    iso639, name
                """
    return iso_language

def add_iso_script():
    iso_script = """
                INSERT INTO "iso_script" (
                    iso15924, name )
                  VALUES ((%s), (%s))
                  RETURNING 
                    iso15924, name
                """
    return iso_script



def add_version_query():
    add_version = """
              INSERT INTO "bible_version" (
                    name, iso_language, iso_script,
                    abbreviation, rights, forward_translation_id,
                    back_translation_id, machine_translation)
                  VALUES ((%s), (%s), (%s), (%s), (%s), (%s), (%s), (%s))
                  RETURNING 
                      id, name, iso_language, iso_script,
                      abbreviation, rights, forward_translation_id,
                      back_translation_id, machine_translation;
                """

    return add_version


def delete_bible_version():
    delete_version = """
                    DELETE FROM  "bible_version"
                      WHERE id=(%s)
                    RETURNING name; 
                    """

    return delete_version


def delete_revision_mutation():
    delete_revision = """
                    DELETE FROM "bible_revision"
                      WHERE id=(%s)
                    RETURNING id;
                    """

    return delete_revision


def check_revisions_query():
    check_revision = """
                    SELECT id FROM "bible_revision";
                    """

    return check_revision


def delete_verses_mutation():
    delete_verses = """
                    DELETE FROM "verse_text"
                      WHERE bible_revision=(%s);
                    """
    
    return delete_verses

def insert_bible_revision():
    bible_revise = """
                INSERT INTO "bible_revision" (
                  date, 
                  bible_version_id, 
                  published, 
                  name, 
                  back_translation_id, 
                  machine_translation
                )
                VALUES (
                  (%s), 
                  (%s), 
                  (%s), 
                  (%s), 
                  (%s), 
                  (%s)
                )
                RETURNING 
                  id, 
                  date, 
                  bible_version_id, 
                  published, 
                  name, 
                  back_translation_id, 
                  machine_translation;
                """
 
    return bible_revise

def fetch_bible_version_by_abbreviation():
    version_id = """
                SELECT * FROM "bible_version"
                  WHERE abbreviation=(%s);
                """
        
    return version_id


def list_all_revisions_query():
    list_revisions = """
                    SELECT br.id, br.date, br.bible_version, br.published, br.name, br.back_translation_id, br.back_translation_id, bv.iso_language, bv.abbreviation
                    FROM "bible_revision" br
                    INNER JOIN "bible_version" bv ON br.bible_version = bv.id
                    """

    return list_revisions


def list_revisions_query():
    list_revisions = """
                    SELECT br.id, br.date, br.bible_version, br.published, br.name, br.back_translation_id, br.machine_translation, bv.iso_language, bv.abbreviation
                    FROM "bible_revision" br
                    INNER JOIN "bible_version" bv ON br.bible_version = bv.id
                      WHERE bible_version=(%s)
                    """

    return list_revisions


def version_data_revisions_query():
    version_data_revisions = """
                    SELECT id, abbreviation FROM "bible_version"
                      WHERE id=(%s);
                    """

    return version_data_revisions


def fetch_version_data():
    fetch_version = """
                    SELECT abbreviation, back_translation_id FROM "bible_version"
                      WHERE id=(%s);
                    """

    return fetch_version


def get_chapter_query(chapter_reference):
    get_chapter = """
                SELECT * FROM "verse_text" "vt"
                  INNER JOIN "verse_reference" "vr" ON vt.verse_reference = fullverseid
                  WHERE bible_revision=(%s)
                    AND vr.chapter = {}
                    ORDER BY vt.id;
                """.format(chapter_reference)

    return get_chapter


def get_verses_query(verse_reference):
    get_verses = """
                SELECT * FROM "verse_text"
                  WHERE bible_revision=(%s)
                    AND verse_reference={};
                """.format(verse_reference)
    
    return get_verses


def get_book_query():
    get_book = """
                SELECT * FROM "verse_text" "vt"
                  INNER JOIN "verse_reference" "vr" ON vt.verse_reference = fullverseid
                    WHERE vt.bible_revision=(%s) and vt.book = (%s)
                    ORDER BY id;
                """

    return get_book


def get_text_query():
    get_text = """
            SELECT * FROM "verse_text"
              WHERE bible_revision=(%s) ORDER BY id;
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
                        DELETE FROM "assessment_result"
                          WHERE assessment=(%s);
                        """

    return delete_assessment_results


def get_results_query():
    get_results = """
                SELECT * FROM "assessment_result"
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
                  FROM "assessment_result"
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
    iso_scripts = 'SELECT * FROM "iso_script";'
        
    return iso_scripts


def get_languages_query():
    iso_languages = 'SELECT * FROM "iso_language";'
         
    return iso_languages

#delete when v1 is removed
def get_results_query_v1():
    get_results = """
                SELECT * FROM "assessment_result"
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
                SELECT * FROM "alignment_top_source_scores"
                  WHERE assessment=($1) 
                  and vref LIKE ($4) || '%'
                  ORDER BY id
                  LIMIT ($2)
                  OFFSET ($3);
                """

    return get_alignment_scores


def get_alignment_scores_exact_query():
    get_alignment_scores = """
                SELECT * FROM "alignment_top_source_scores"
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
                  FROM "alignment_top_source_scores"
                    WHERE assessment=($1)
                  and vref LIKE ($2) || '%';
                """

    return get_alignment_scores_agg


def get_alignment_scores_agg_exact_query():
    get_alignment_scores_agg = """
                SELECT COUNT(id)
                  FROM "alignment_top_source_scores"
                    WHERE assessment=($1)
                  and vref = ($2);
                """

    return get_alignment_scores_agg