def fetch_bible_version_from_revision(revision_id):
    version_id = """
                query MyQuery {{
                  bibleVersion(where: {{
                    bibleRevisions: {{
                      id: {{
                        _eq: {}
                        }}
                        }}
                        }}) {{
                    abbreviation
                  }}
                }}
                """.format(revision_id)
    return version_id
