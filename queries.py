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


def check_revisions_query():
    check_revision = """
                    query {
                      bibleRevision {
                        id
                      }
                    }
                    """

    return check_revision


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


def insert_bible_revision(version, name, date, published):
    bible_revise = """
                mutation {{
                  insert_bibleRevision(objects: {{
                    bibleVersion: {}, name: {}, date: {}, published: {}
                    }}) {{
                    returning {{
                      id
                    }}
                  }}
                }}
                """.format(version, name, date, published)
        
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


def list_all_revisions_query():
    list_revisions = """
                  query {
                    bibleRevision {
                      id
                      date
                      bibleVersionByBibleversion {
                        name
                      }
                      name
                    }
                  }
                  """

    return list_revisions


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
                      name
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


def get_book_query(revision, bookReference):
    get_book = """
            query {{
              verseText(where: {{
                bibleRevision: {{
                  _eq: {}
                }}, _and: {{
                  verseReferenceByVersereference: {{
                    chapterReference: {{
                      bookReference: {{
                        _eq: {}
                      }}
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
            """.format(revision, bookReference)

    return get_book


def get_text_query(revision):
    get_text = """
            query {{
              verseText(where: {{
                bibleRevision: {{
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
            """.format(revision)

    return get_text


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


def add_assessment_query(revision, reference, assessment_type, requested_time, status):
    
    add_assessment = """
                  mutation MyMutation {{
                    insert_assessment(objects: {{
                      revision: {}, reference: {}, type: {},
                      requested_time: {}, status: {}
                    }}) {{
                      returning {{
                        id
                        revision
                        reference
                        type
                        requested_time
                        status
                      }}
                    }}
                  }}
                  """.format(revision, reference, assessment_type, 
                          requested_time, status 
                          )

    return add_assessment


def check_assessments_query():
    check_assessment = """
                    query {
                      assessment {
                        id
                      }
                    }
                    """

    return check_assessment


def delete_assessment_mutation(assessment):
    delete_assessment = """
                    mutation {{
                      delete_assessment(where: {{
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
                    """.format(assessment)

    return delete_assessment


def delete_assessment_results_mutation(assessment):
    delete_assessment_results = """ 
                    mutation {{
                      delete_assessmentResult(where: {{
                        assessment: {{
                          _eq: {}
                        }}
                      }}) {{
                        affected_rows
                      }}
                    }}
                    """.format(assessment)
    
    return delete_assessment_results


def get_results_query(assessment_id):
    get_results = """
                query {{
                  assessmentResult(
                    where: {{
                      assessment: {{
                        _eq: {}
                      }}
                    }}
                  ) {{
                    id
                    score
                    flag
                    note
                    vref
                    assessmentByAssessment {{
                      reference
                      type
                    }}
                  }}
                }}
                """.format(assessment_id)

    return get_results


def get_missing_words_query(assessment_id):
    get_results = """
                query {{
                  assessmentMissingWords(
                    where: {{
                      assessment: {{
                        _eq: {}
                      }}
                    }}
                  ) {{
                    id
                    score
                    flag
                    note
                    vref
                    source
                    target
                    assessmentByAssessment {{
                      reference
                      type
                    }}
                  }}
                }}
                """.format(assessment_id)

    return get_results


def get_scripts_query():
    iso_scripts = """
        query list_scripts {
          isoScript {
            iso15924
            name
          }
        }
        """

    return iso_scripts


def get_languages_query():
    iso_languages = """
        query list_languages {
          isoLanguage {
            iso639
            name
          }
        }
        """
    
    return iso_languages
