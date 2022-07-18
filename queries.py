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
