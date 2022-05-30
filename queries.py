class all_queries():
    def list_versions_query():
        list_version = """
                    query MyQuery {
                        bible_version {
                            id
                            name
                            abbreviation
                            iso_language {
                                iso639
                            }
                            iso_script {
                                iso15924
                            }
                            rights
                        }
                    }
                    """
        return list_version
