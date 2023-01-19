def list_versions_query():
    list_version = """
                query MyQuery {
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
                  mutation MyMutation {{
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
                    query MyQuery {
                      bibleVersion {
                        abbreviation
                      }
                    }
                    """
    
    return check_version


def delete_bible_version(version_abbv):
    delete_version = """
                     mutation MyMutation {{
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


def delete_verses_mutation(bibleRevision):
    delete_verses = """ 
                    mutation MyMutation {{
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


def delete_revisions_mutation(bibleRevision):
    delete_revision = """
                    mutation MyMutation {{
                      delete_bibleRevision(where: {{
                        id: {{
                          _eq: {}
                        }}
                      }}) {{
                        affected_rows
                      }}
                    }}
                    """.format(bibleRevision)

    return delete_revision


def insert_bible_revision(version, date, published):
    bible_revise = """
                mutation MyMutation {{
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
                query MyQuery {{
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
                  query MyQuery {{
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
                query MyQuery {{
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
              query MyQuery {{
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


def list_assessments_query():
    list_assessment = """
                query MyQuery {
                  assessment {
                    id
                    revision
                    reference
                    type
                    requested_time
                    start_time
                    end_time
                    status
                  }
                }
                """
        
    return list_assessment


def add_assessment_query(revision, assessment_type, requested_time, status, reference):
    
    add_assessment = """
                  mutation MyMutation {{
                    insert_assessment(objects: {{
                      revision: {}, type: {}, requested_time: {}, 
                      status: {}, reference: {}
                    }}) {{
                      returning {{
                        id
                        revision
                        type
                        requested_time
                        status
                        reference
                      }}
                    }}
                  }}
                  """.format(revision, assessment_type, 
                          requested_time, status, reference
                          )

    return add_assessment