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
                        language {{
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
                       }}
                     }}
                     """.format(version_abbv)

    return delete_version


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


def delete_bibleRevision_text(revision_id):
    revision_delete = """
                      mutation MyMutation {{
                        delete_verseText(where: {{
                          bibleRevision: {{
                            _eq: {}
                          }}
                        }}) {{
                          affected_rows
                        }}
                        delete_bibleRevision(where: {{
                          id: {{
                            _eq: {}
                          }}
                        }}) {{
                          affected_rows
                        }}
                      }}
                      """.format(revision_id, revision_id)

    return revision_delete


def list_revisions_query(bibleVersion):
    list_revisions = """
                  query MyQuery {{
                    bibleRevision(where: {{
                      version: {{
                        abbreviation: {{
                          _eq: {}
                        }}
                      }}
                    }}) {{
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
