def list_versions_query():
    list_version = """
                query MyQuery {
                  bibleVersion {
                    id
                    name
                    abbreviation
                    language {
                      iso639
                    }
                    script {
                      iso15924
                    }
                    rights
                  }
                }
                """
        
    return list_version


def insert_bible_revision(date):
    bible_revise = """
                mutation MyMutation {{
                  insert_bibleRevision(objects: {{
                    bibleVersion: 1, date: {}, published: false
                    }}) {{
                    returning {{
                      id
                    }}
                  }}
                }}
                """.format(date)
        
    return bible_revise


def list_revisions_query(bibleVersion):
    list_revisions = """
                  query MyQuery {{
                    bibleRevision(where: {{
                      version: {{
                        abbreviation: {{_eq: {}}}}}}}) {{
                      id
                      date
                      version {{
                        name
                      }}
                    }}
                  }}
    """.format(bibleVersion)

    return list_revisions


def get_chapter_query(revision, chapterReference):
    get_chapter = """
                query MyQuery {{
                  verseText(where: {{bibleRevision: {{_eq: {}}}, 
                    verseReferenceByVersereference: {{
                      chapterReference: {{
                        fullChapterId: {{_eq: {}}}}}}}}}) {{
                    id
                    text
                    verseReference
                    revision {{
                      date
                      version {{
                        name
                      }}
                    }}
                  }}
                }}
                """.format(revision, chapterReference)

    return get_chapter


def get_verses_query(revision, verseReference):
    get_verses = """
              query MyQuery {{
                verseText(where: {{bibleRevision: {{_eq: {}}}, 
                  verseReference: {{_eq: {}}}}}) {{
                  id
                  text
                  verseReference
                  revision {{
                    date
                    version {{
                      name
                    }}
                  }}
                }}
              }}
              """.format(revision, verseReference)
    
    return get_verses
