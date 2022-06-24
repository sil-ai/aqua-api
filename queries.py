class all_queries():
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

    def bible_loading():

