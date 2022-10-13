import os


def sql_query():
    sql_query = {"type": "run_sql", "args": {"source": "default", "sql": """
    SET check_function_bodies = false;
    CREATE TABLE public.assessment (
        id integer NOT NULL,
        revision integer NOT NULL,
        reference integer,
        type text NOT NULL,
        finished boolean NOT NULL
    );
    COMMENT ON TABLE public.assessment IS 'Info about assessments';
    CREATE TABLE public."assessmentResult" (
        id integer NOT NULL,
        assessment integer NOT NULL,
        score numeric NOT NULL,
        flag boolean NOT NULL,
        note text NOT NULL
    );
    COMMENT ON TABLE public."assessmentResult" IS 'Results of assessments';
    CREATE SEQUENCE public."assessmentResult_id_seq"
        AS integer
        START WITH 1
        INCREMENT BY 1
        NO MINVALUE
        NO MAXVALUE
        CACHE 1;
    ALTER SEQUENCE public."assessmentResult_id_seq" OWNED BY public."assessmentResult".id;
    CREATE SEQUENCE public.assessment_id_seq
        AS integer
        START WITH 1
        INCREMENT BY 1
        NO MINVALUE
        NO MAXVALUE
        CACHE 1;
    ALTER SEQUENCE public.assessment_id_seq OWNED BY public.assessment.id;
    CREATE TABLE public."bibleRevision" (
        id integer NOT NULL,
        date date NOT NULL,
        "bibleVersion" integer NOT NULL,
        published boolean NOT NULL
    );
    COMMENT ON TABLE public."bibleRevision" IS 'Bible revisions (used for various drafts or published revisions)';
    CREATE SEQUENCE public."bibleRevision_id_seq"
        AS integer
        START WITH 1
        INCREMENT BY 1
        NO MINVALUE
        NO MAXVALUE
        CACHE 1;
    ALTER SEQUENCE public."bibleRevision_id_seq" OWNED BY public."bibleRevision".id;
    CREATE TABLE public."bibleVersion" (
        id integer NOT NULL,
        name text NOT NULL,
        "isoLanguage" text NOT NULL,
        "isoScript" text NOT NULL,
        abbreviation text NOT NULL,
        rights text,
        "forwardTranslation" integer,
        "backTranslation" integer,
        "machineTranslation" boolean NOT NULL
    );
    COMMENT ON TABLE public."bibleVersion" IS 'Bible Versions (NIV, ONAV, etc.)';
    CREATE SEQUENCE public."bibleVersion_id_seq"
        AS integer
        START WITH 1
        INCREMENT BY 1
        NO MINVALUE
        NO MAXVALUE
        CACHE 1;
    ALTER SEQUENCE public."bibleVersion_id_seq" OWNED BY public."bibleVersion".id;
    CREATE TABLE public."bookReference" (
        abbreviation text NOT NULL,
        name text NOT NULL,
        number integer NOT NULL
    );
    COMMENT ON TABLE public."bookReference" IS 'All the books of the Bible';
    CREATE TABLE public."chapterReference" (
        "fullChapterId" text NOT NULL,
        number integer NOT NULL,
        "bookReference" text NOT NULL
    );
    COMMENT ON TABLE public."chapterReference" IS 'Chapter references for all of the Bible';
    CREATE TABLE public."isoLanguage" (
        id integer NOT NULL,
        iso639 text NOT NULL,
        name text NOT NULL
    );
    COMMENT ON TABLE public."isoLanguage" IS 'ISO639-3 Language';
    CREATE TABLE public."isoScript" (
        id integer NOT NULL,
        iso15924 text NOT NULL,
        name text NOT NULL
    );
    COMMENT ON TABLE public."isoScript" IS 'ISO15924 Writing System Scripts';
    CREATE SEQUENCE public.iso_language_id_seq
        AS integer
        START WITH 1
        INCREMENT BY 1
        NO MINVALUE
        NO MAXVALUE
        CACHE 1;
    ALTER SEQUENCE public.iso_language_id_seq OWNED BY public."isoLanguage".id;
    CREATE SEQUENCE public.iso_script_id_seq
        AS integer
        START WITH 1
        INCREMENT BY 1
        NO MINVALUE
        NO MAXVALUE
        CACHE 1;
    ALTER SEQUENCE public.iso_script_id_seq OWNED BY public."isoScript".id;
    CREATE TABLE public."localizedBookName" (
        id integer NOT NULL,
        name text NOT NULL,
        "bookReference" integer NOT NULL,
        "bibleVersion" integer NOT NULL
    );
    COMMENT ON TABLE public."localizedBookName" IS 'Localized names of book linked to book references and bible versions';
    CREATE SEQUENCE public.localized_book_name_id_seq
        AS integer
        START WITH 1
        INCREMENT BY 1
        NO MINVALUE
        NO MAXVALUE
        CACHE 1;
    ALTER SEQUENCE public.localized_book_name_id_seq OWNED BY public."localizedBookName".id;
    CREATE TABLE public.question (
        id integer NOT NULL,
        start_verse text NOT NULL,
        end_verse text NOT NULL,
        language text NOT NULL,
        question text NOT NULL,
        answer text NOT NULL
    );
    COMMENT ON TABLE public.question IS 'For comprehension questions';
    CREATE SEQUENCE public.question_id_seq
        AS integer
        START WITH 1
        INCREMENT BY 1
        NO MINVALUE
        NO MAXVALUE
        CACHE 1;
    ALTER SEQUENCE public.question_id_seq OWNED BY public.question.id;
    CREATE TABLE public."verseReference" (
        "fullVerseId" text NOT NULL,
        number integer NOT NULL,
        chapter text NOT NULL
    );
    COMMENT ON TABLE public."verseReference" IS 'Verse references for all of the Bible';
    CREATE TABLE public."verseText" (
        id integer NOT NULL,
        text text NOT NULL,
        "bibleRevision" integer NOT NULL,
        "verseReference" text NOT NULL
    );
    COMMENT ON TABLE public."verseText" IS 'Text from the various Bible revisions in the graph';
    CREATE SEQUENCE public."verseText_id_seq"
        AS integer
        START WITH 1
        INCREMENT BY 1
        NO MINVALUE
        NO MAXVALUE
        CACHE 1;
    ALTER SEQUENCE public."verseText_id_seq" OWNED BY public."verseText".id;
    ALTER TABLE ONLY public.assessment ALTER COLUMN id SET DEFAULT nextval('public.assessment_id_seq'::regclass);
    ALTER TABLE ONLY public."assessmentResult" ALTER COLUMN id SET DEFAULT nextval('public."assessmentResult_id_seq"'::regclass);
    ALTER TABLE ONLY public."bibleRevision" ALTER COLUMN id SET DEFAULT nextval('public."bibleRevision_id_seq"'::regclass);
    ALTER TABLE ONLY public."bibleVersion" ALTER COLUMN id SET DEFAULT nextval('public."bibleVersion_id_seq"'::regclass);
    ALTER TABLE ONLY public."isoLanguage" ALTER COLUMN id SET DEFAULT nextval('public.iso_language_id_seq'::regclass);
    ALTER TABLE ONLY public."isoScript" ALTER COLUMN id SET DEFAULT nextval('public.iso_script_id_seq'::regclass);
    ALTER TABLE ONLY public."localizedBookName" ALTER COLUMN id SET DEFAULT nextval('public.localized_book_name_id_seq'::regclass);
    ALTER TABLE ONLY public.question ALTER COLUMN id SET DEFAULT nextval('public.question_id_seq'::regclass);
    ALTER TABLE ONLY public."verseText" ALTER COLUMN id SET DEFAULT nextval('public."verseText_id_seq"'::regclass);
    ALTER TABLE ONLY public."assessmentResult"
        ADD CONSTRAINT "assessmentResult_pkey" PRIMARY KEY (id);
    ALTER TABLE ONLY public.assessment
        ADD CONSTRAINT assessment_pkey PRIMARY KEY (id);
    ALTER TABLE ONLY public."bibleRevision"
        ADD CONSTRAINT "bibleRevision_pkey" PRIMARY KEY (id);
    ALTER TABLE ONLY public."bibleVersion"
        ADD CONSTRAINT "bibleVersion_pkey" PRIMARY KEY (id);
    ALTER TABLE ONLY public."bookReference"
        ADD CONSTRAINT "bookReference_pkey" PRIMARY KEY (abbreviation);
    ALTER TABLE ONLY public."chapterReference"
        ADD CONSTRAINT "chapterReference_pkey" PRIMARY KEY ("fullChapterId");
    ALTER TABLE ONLY public."isoLanguage"
        ADD CONSTRAINT iso_language_pkey PRIMARY KEY (iso639);
    ALTER TABLE ONLY public."isoScript"
        ADD CONSTRAINT iso_script_pkey PRIMARY KEY (iso15924);
    ALTER TABLE ONLY public."localizedBookName"
        ADD CONSTRAINT localized_book_name_pkey PRIMARY KEY (id);
    ALTER TABLE ONLY public.question
        ADD CONSTRAINT question_pkey PRIMARY KEY (id);
    ALTER TABLE ONLY public."verseReference"
        ADD CONSTRAINT "verseReference_pkey" PRIMARY KEY ("fullVerseId");
    ALTER TABLE ONLY public."verseText"
        ADD CONSTRAINT "verseText_pkey" PRIMARY KEY (id);
    ALTER TABLE ONLY public."assessmentResult"
        ADD CONSTRAINT "assessmentResult_assessment_fkey" FOREIGN KEY (assessment) REFERENCES public.assessment(id) ON UPDATE RESTRICT ON DELETE RESTRICT;
    ALTER TABLE ONLY public.assessment
        ADD CONSTRAINT assessment_reference_fkey FOREIGN KEY (reference) REFERENCES public."bibleRevision"(id) ON UPDATE RESTRICT ON DELETE RESTRICT;
    ALTER TABLE ONLY public.assessment
        ADD CONSTRAINT assessment_revision_fkey FOREIGN KEY (revision) REFERENCES public."bibleRevision"(id) ON UPDATE RESTRICT ON DELETE RESTRICT;
    ALTER TABLE ONLY public."bibleRevision"
        ADD CONSTRAINT "bibleRevision_bibleVersion_fkey" FOREIGN KEY ("bibleVersion") REFERENCES public."bibleVersion"(id) ON UPDATE RESTRICT ON DELETE RESTRICT;
    ALTER TABLE ONLY public."bibleVersion"
        ADD CONSTRAINT "bibleVersion_isoLanguage_fkey" FOREIGN KEY ("isoLanguage") REFERENCES public."isoLanguage"(iso639) ON UPDATE RESTRICT ON DELETE RESTRICT;
    ALTER TABLE ONLY public."bibleVersion"
        ADD CONSTRAINT "bibleVersion_isoScript_fkey" FOREIGN KEY ("isoScript") REFERENCES public."isoScript"(iso15924) ON UPDATE RESTRICT ON DELETE RESTRICT;
    ALTER TABLE ONLY public."chapterReference"
        ADD CONSTRAINT "chapterReference_bookReference_fkey" FOREIGN KEY ("bookReference") REFERENCES public."bookReference"(abbreviation) ON UPDATE RESTRICT ON DELETE RESTRICT;
    ALTER TABLE ONLY public.question
        ADD CONSTRAINT question_end_verse_fkey FOREIGN KEY (end_verse) REFERENCES public."verseReference"("fullVerseId") ON UPDATE RESTRICT ON DELETE RESTRICT;
    ALTER TABLE ONLY public.question
        ADD CONSTRAINT question_language_fkey FOREIGN KEY (language) REFERENCES public."isoLanguage"(iso639) ON UPDATE RESTRICT ON DELETE RESTRICT;
    ALTER TABLE ONLY public.question
        ADD CONSTRAINT question_start_verse_fkey FOREIGN KEY (start_verse) REFERENCES public."verseReference"("fullVerseId") ON UPDATE RESTRICT ON DELETE RESTRICT;
    ALTER TABLE ONLY public."verseReference"
        ADD CONSTRAINT "verseReference_chapter_fkey" FOREIGN KEY (chapter) REFERENCES public."chapterReference"("fullChapterId") ON UPDATE RESTRICT ON DELETE RESTRICT;
    ALTER TABLE ONLY public."verseText"
        ADD CONSTRAINT "verseText_bibleRevision_fkey" FOREIGN KEY ("bibleRevision") REFERENCES public."bibleRevision"(id) ON UPDATE RESTRICT ON DELETE RESTRICT;
    ALTER TABLE ONLY public."verseText"
        ADD CONSTRAINT "verseText_verseReference_fkey" FOREIGN KEY ("verseReference") REFERENCES public."verseReference"("fullVerseId") ON UPDATE RESTRICT ON DELETE RESTRICT;
    """}}

    return sql_query


def db_connection(db_url):
    db_con = {
        "type": "pg_add_source",
        "args": {
          "name": "default",
          "configuration": {
            "connection_info": {
              "database_url": db_url,
              "pool_settings": {
                "retries": 1,
                "idle_timeout": 180,
                "max_connections": 50
                }
              }
            }
          }
        }

    return db_con
