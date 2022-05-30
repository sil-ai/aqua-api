class all_queries():
    def list_versions_query():
        list_version = """
                    query MyQuery {
                        queryBibleVersion {
                            id
                            name
                            abbreviation
                            isoLanguage {
                                iso639
                            }
                            isoScript {
                                iso15924
                            }
                            rights
                        }
                    }
                    """
        return list_version
