def list_versions_query():
    list_version = """
                query {
                  bibleVersion {
                    id
                    name
                    abbreviation
                    isoLanguageByIsolanguage {
                      iso639
                    }
                    isoScriptByIsoscript {
                      iso15924
                    }
                    rights
                  }
                }
                """
        
    return list_version


def add_version_query(name, isoLanguage, isoScript, 
                abbreviation, rights, forwardTranslation, 
                backTranslation, machineTranslation):
    
    add_version = """
                  mutation {{
                    insert_bibleVersion(objects: {{
                      name: {}, isoLanguage: {}, isoScript: {}, 
                      abbreviation: {}, rights: {}, 
                      forwardTranslation: {}, backTranslation: {}, 
                      machineTranslation: {}
                    }}) {{
                      returning {{
                        id
                        name
                        abbreviation
                        isoLanguageByIsolanguage {{
                          name
                        }}
                        rights
                      }}
                    }}
                  }}
                  """.format(name, isoLanguage, 
                          isoScript, abbreviation, rights, 
                          forwardTranslation, backTranslation,
                          machineTranslation
                          )

    return add_version

def check_version_query():
    check_version = """
                    query {
                      bibleVersion {
                        abbreviation
                      }
                    }
                    """
    
    return check_version


def delete_bible_version(version_abbv):
    delete_version = """
                     mutation {{
                       delete_bibleVersion(where: {{
                         abbreviation: {{
                           _eq: {}
                         }}
                       }}) {{
                         affected_rows
                         returning {{
                           name
                         }}
                       }}
                     }}
                     """.format(version_abbv)

    return delete_version


def delete_revision_mutation(bibleRevision):
    delete_revision = """
                    mutation {{
                      delete_bibleRevision(where: {{
                        id: {{
                          _eq: {}
                        }}
                      }}) {{
                        affected_rows
                        returning {{
                          id
                        }}
                      }}
                    }}
                    """.format(bibleRevision)

    return delete_revision


def delete_verses_mutation(bibleRevision):
    delete_verses = """ 
                    mutation {{
                      delete_verseText(where: {{
                        bibleRevision: {{
                          _eq: {}
                        }}
                      }}) {{
                        affected_rows
                      }}
                    }}
                    """.format(bibleRevision)
    
    return delete_verses


def insert_bible_revision(version, date, published):
    bible_revise = """
                mutation {{
                  insert_bibleRevision(objects: {{
                    bibleVersion: {}, date: {}, published: {}
                    }}) {{
                    returning {{
                      id
                    }}
                  }}
                }}
                """.format(version, date, published)
        
    return bible_revise


def fetch_bible_version(abbreviation):
    version_id = """
                query {{
                  bibleVersion(where: {{
                    abbreviation: {{
                      _eq: {}
                    }}
                  }}) {{
                    id
                    }}
                }}
                """.format(abbreviation)
        
    return version_id


def list_revisions_query(bibleVersion):
    list_revisions = """
                  query {{
                    bibleRevision(where: {{
                      bibleVersionByBibleversion: {{
                        abbreviation: {{
                          _eq: {}
                        }}
                      }}
                    }}) {{
                      id
                      date
                      bibleVersionByBibleversion {{
                        name
                      }}
                    }}
                  }}
                  """.format(bibleVersion)

    return list_revisions


def get_chapter_query(revision, chapterReference):
    get_chapter = """
                query {{
                  verseText(where: {{
                    bibleRevision: {{
                      _eq: {}
                    }}, 
                    verseReferenceByVersereference: {{
                      chapterReference: {{
                        fullChapterId: {{
                          _eq: {}
                        }}
                      }}
                    }}
                  }}) {{
                    id
                    text
                    verseReference
                    bibleRevisionByBiblerevision {{
                      date
                      bibleVersionByBibleversion {{
                        name
                      }}
                    }}
                  }}
                }}
                """.format(revision, chapterReference)

    return get_chapter


def get_verses_query(revision, verseReference):
    get_verses = """
              query {{
                verseText(where: {{
                  bibleRevision: {{
                    _eq: {}
                  }}, 
                  verseReference: {{
                    _eq: {}
                  }}
                }}) {{
                  id
                  text
                  verseReference
                  bibleRevisionByBiblerevision {{
                    date
                    bibleVersionByBibleversion {{
                      name
                    }}
                  }}
                }}
              }}
              """.format(revision, verseReference)
    
    return get_verses
