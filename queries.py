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


def list_revisions_query(revision):
    list_revisions = """
    """.format(revision)

    return get_revisions


def get_chapters_query(revision, book, chapter):
    

    get_chapters = """
    """.format(revision, verseReference)

    return get_chapter


def get_verses_query(revision, book, chapter, verse):
    verseReference = book + " " + str(chapter) + ":" + str(verse)

    get_verses = """
    query MyQuery {
      verseText(where: {bibleRevision: {_eq: {}}, verseReference: {_eq: {}}}) {
        id
        text
        verseReference
        revision {
          bibleVersion
          date
          version {
            name
          }
        }
      }
    }
    """.format(revision, verseReference)
    
    return get_verses
