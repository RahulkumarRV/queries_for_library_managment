-- Drop table with foreign key constraints first
DROP MATERIALIZED VIEW IF EXISTS student_semester_summary;
-- DROP VIEW IF EXISTS student_semester_summary;
DROP TABLE IF EXISTS student_dept_change;
DROP MATERIALIZED VIEW IF EXISTS course_eval;
DROP TABLE IF EXISTS student_courses ;
DROP TABLE IF EXISTS valid_entry;
DROP TABLE IF EXISTS course_offers;
DROP TABLE IF EXISTS professor;
-- Drop other tables
DROP TABLE IF EXISTS courses;
DROP TABLE IF EXISTS student;
DROP TABLE IF EXISTS department;

CREATE TABLE department (
    dept_id CHAR(3) PRIMARY KEY,
    dept_name VARCHAR(40) NOT NULL UNIQUE
);
CREATE TABLE student (
    first_name VARCHAR(40) NOT NULL,
    last_name VARCHAR(40),
    student_id CHAR(11) PRIMARY KEY,
    address VARCHAR(100),
    contact_number CHAR(10) NOT NULL UNIQUE,
    email_id VARCHAR(50) UNIQUE,
    tot_credits INTEGER NOT NULL CHECK (tot_credits >= 0),
    dept_id CHAR(3) REFERENCES department(dept_id)
);
CREATE OR REPLACE FUNCTION is_valid_course_id(course_id CHAR(6))
RETURNS BOOLEAN AS $$
BEGIN
    RETURN SUBSTRING(course_id, 1, 3) LIKE ANY (SELECT dept_id || '%' FROM department) AND SUBSTRING(course_id, 4, 3) ~ '^\d{3}$';
END;
$$ LANGUAGE plpgsql;

CREATE TABLE courses (
    course_id CHAR(6) PRIMARY KEY CHECK (is_valid_course_id(course_id)),
    course_name VARCHAR(20) NOT NULL UNIQUE,
    course_desc TEXT,
    credits NUMERIC NOT NULL CHECK (credits > 0),
    dept_id CHAR(11) REFERENCES department(dept_id)
);

CREATE TABLE professor (
    professor_id VARCHAR(10) PRIMARY KEY,
    professor_first_name VARCHAR(40) NOT NULL,
    professor_last_name VARCHAR(40) NOT NULL,
    office_number VARCHAR(20),
    contact_number CHAR(10) NOT NULL,
    start_year INTEGER CHECK (start_year <= resign_year),
    resign_year INTEGER,
    dept_id CHAR(11) REFERENCES department(dept_id)
);

CREATE TABLE course_offers (
    course_id CHAR(6) REFERENCES courses(course_id),
    session VARCHAR(9),
    semester INTEGER NOT NULL CHECK (semester IN (1, 2)),
    professor_id CHAR(11) REFERENCES professor(professor_id),
    capacity INTEGER,
    enrollments INTEGER,
    PRIMARY KEY (course_id, session, semester)
);

CREATE TABLE student_courses (
    student_id CHAR(11) REFERENCES student(student_id),
    course_id CHAR(6),
    session VARCHAR(9),
    semester INTEGER  NOT NULL CHECK (semester IN (1, 2)) ,
    grade NUMERIC NOT NULL CHECK (grade >= 0 AND grade <= 10),
    PRIMARY KEY (student_id, course_id, session, semester),
    FOREIGN KEY (course_id, session, semester) REFERENCES course_offers(course_id, session, semester)
);


CREATE TABLE valid_entry (
    dept_id CHAR(11) REFERENCES department(dept_id),
    entry_year INTEGER NOT NULL,
    seq_number INTEGER NOT NULL,
    PRIMARY KEY (dept_id, entry_year, seq_number)
);

CREATE TABLE student_dept_change (
    old_student_id CHAR(11) PRIMARY KEY,
    old_dept_id CHAR(3) NOT NULL,
    new_dept_id CHAR(3),
    new_student_id CHAR(11)
);

-- 2.1.1
-- check if student_id and email_id is valid
CREATE OR REPLACE FUNCTION validate_student_id_function()
RETURNS TRIGGER AS $$
DECLARE
    v_entry_year   INTEGER;
    v_dept_id      CHAR(3);
    v_seq_number   INTEGER;
    v_valid_seq_number INTEGER;
    v_email_prefix VARCHAR(50);
    v_email_domain VARCHAR(50);
BEGIN
    -- Check if the length of student_id is at least 10 characters
    IF LENGTH(NEW.student_id) < 10 THEN
        RAISE EXCEPTION 'invalid';
    END IF;

    -- Extract information from student_id
    v_entry_year := CAST(SUBSTRING(NEW.student_id FROM 1 FOR 4) AS INTEGER);
    v_dept_id := SUBSTRING(NEW.student_id FROM 5 FOR 3);
    v_seq_number := CAST(SUBSTRING(NEW.student_id FROM 8 FOR 3) AS INTEGER);

    -- Retrieve the sequence number from valid_entry
    SELECT seq_number INTO v_valid_seq_number
    FROM valid_entry
    WHERE dept_id = v_dept_id AND entry_year = v_entry_year;

    -- Check if the entry year and dept id are valid in the valid_entry table
    IF v_valid_seq_number IS NULL THEN
        RAISE EXCEPTION 'invalid';
    END IF;

    -- Check if the extracted seq_number matches the one from valid_entry
    IF v_seq_number != v_valid_seq_number THEN
        RAISE EXCEPTION 'invalid';
    END IF;

    -- v_email_prefix := FORMAT('%s%s%03d', v_entry_year, v_dept_id, v_seq_number);
    v_email_domain := FORMAT('%s.iitd.ac.in', v_dept_id);

    IF NEW.email_id IS NOT NULL AND NEW.email_id !~ FORMAT('%s@%s', SUBSTRING(NEW.student_id FROM 1 FOR 10), v_email_domain) THEN
        RAISE EXCEPTION 'invalid';
    END IF;

    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Trigger to validate student ID before insert
CREATE OR REPLACE TRIGGER validate_student_id
BEFORE INSERT
ON student
FOR EACH ROW
EXECUTE FUNCTION validate_student_id_function();

-- 2.1.2
CREATE OR REPLACE FUNCTION update_seq_number_function()
RETURNS TRIGGER AS $$
BEGIN
    UPDATE valid_entry
    SET seq_number = seq_number + 1
    WHERE dept_id = (SUBSTRING(NEW.student_id FROM 5 FOR 3)) 
    AND entry_year = CAST((SUBSTRING(NEW.student_id FROM 1 FOR 4)) AS INTEGER)
    AND seq_number = CAST((SUBSTRING(NEW.student_id FROM 8 FOR 3)) AS INTEGER);

    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE TRIGGER update_seq_number
AFTER INSERT 
ON student
FOR EACH ROW
EXECUTE FUNCTION update_seq_number_function();

-- 2.1.4
CREATE OR REPLACE FUNCTION log_student_dept_change()
RETURNS TRIGGER AS $$
DECLARE
    student_entry_year INTEGER;
    avg_grade INTEGER;
    new_student_id CHAR(11);
    new_seq_number INTEGER;
BEGIN
    student_entry_year := CAST(SUBSTRING(NEW.student_id FROM 1 FOR 4) AS INTEGER);

    IF old.dept_id <> new.dept_id THEN
        IF student_entry_year < 2022 THEN 
            RAISE EXCEPTION 'entry year must be >= 2022';
        END IF;

        SELECT AVG(grade)
        INTO avg_grade
        FROM student_courses
        WHERE student_id = OLD.student_id;

        IF avg_grade IS NULL OR avg_grade < 8.5 THEN
            RAISE EXCEPTION 'Low Grade';
        END IF;

        SELECT seq_number INTO new_seq_number
        FROM valid_entry
        WHERE dept_id = new.dept_id AND entry_year = student_entry_year;

        IF new_seq_number IS NULL THEN
            RAISE EXCEPTION 'sequence number not found';
        END IF;

        new_student_id := student_entry_year::TEXT || new.dept_id || new_seq_number::TEXT;

        UPDATE student
        SET student_id = new_student_id
        WHERE student_id = old.student_id;
    END IF;

    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION log_student_id_change()
RETURNS TRIGGER AS $$
DECLARE
    v_entry_year INTEGER;
    v_dept_id CHAR(3);
    v_seq_number INTEGER;
    v_valid_seq_number INTEGER;
    new_email_id VARCHAR(50);
BEGIN
    IF old.student_id <> new.student_id THEN
        IF LENGTH(NEW.student_id) < 10 THEN
            RAISE EXCEPTION 'invalid';
        END IF;

        -- Extract information from student_id
        v_entry_year := CAST(SUBSTRING(NEW.student_id FROM 1 FOR 4) AS INTEGER);
        v_dept_id := SUBSTRING(NEW.student_id FROM 5 FOR 3);
        v_seq_number := CAST(SUBSTRING(NEW.student_id FROM 8 FOR 3) AS INTEGER);

        -- Retrieve the sequence number from valid_entry
        SELECT seq_number INTO v_valid_seq_number
        FROM valid_entry
        WHERE dept_id = v_dept_id AND entry_year = v_entry_year;

        -- Check if the entry year and dept id are valid in the valid_entry table
        IF v_valid_seq_number IS NULL OR v_seq_number != v_valid_seq_number THEN
            RAISE EXCEPTION 'invalid';
        END IF;

        new_email_id := FORMAT('%s@%s.iitd.ac.in', new.student_id, old.dept_id);

        UPDATE valid_entry
        SET seq_number = seq_number + 1
        WHERE dept_id = old.dept_id
        AND entry_year = v_entry_year
        AND seq_number = v_seq_number;
    END IF;

    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE TRIGGER student_id_change
BEFORE UPDATE ON student
FOR EACH ROW
EXECUTE FUNCTION log_student_id_change();

CREATE OR REPLACE TRIGGER student_dept_change
BEFORE UPDATE ON student
FOR EACH ROW
EXECUTE FUNCTION log_student_dept_change();

-- 2.2.1
CREATE MATERIALIZED VIEW course_eval AS
SELECT
    course_id,
    session,
    semester,
    COUNT(sc.student_id) AS number_of_students,
    ROUND(AVG(sc.grade),2) AS average_grade,
    MAX(sc.grade) AS max_grade,
    MIN(sc.grade) AS min_grade
FROM
    student_courses sc
GROUP BY
    course_id, session, semester;

CREATE OR REPLACE FUNCTION refresh_course_eval_function()
RETURNS TRIGGER AS $$
BEGIN
    REFRESH MATERIALIZED VIEW course_eval;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Create trigger to call the trigger function before insert or update in student_courses
CREATE TRIGGER refresh_course_eval_trigger
AFTER INSERT OR UPDATE ON student_courses
FOR EACH STATEMENT
EXECUTE FUNCTION refresh_course_eval_function();

-- 2.2.4

CREATE OR REPLACE FUNCTION check_course_credit_limit_function()
RETURNS TRIGGER AS $$
DECLARE
    v_student_first_year INTEGER;
    v_session_year INTEGER;
    v_course_credits NUMERIC;
BEGIN
    v_student_first_year := CAST((SUBSTRING(NEW.student_id FROM 1 FOR 4)) AS INTEGER);
    v_session_year := CAST((SUBSTRING(NEW.session FROM 1 FOR 4)) AS INTEGER);
    IF EXISTS (
        SELECT 1
        FROM courses
        WHERE course_id = NEW.course_id
        AND v_session_year != v_student_first_year
        AND credits = 5
    )   THEN
        RAISE EXCEPTION 'invalid';
    END IF;

    RETURN NEW;
END;
$$ LANGUAGE plpgsql;


CREATE OR REPLACE TRIGGER check_course_credit_limit
BEFORE INSERT OR UPDATE
ON student_courses
FOR EACH ROW
EXECUTE FUNCTION check_course_credit_limit_function();


CREATE OR REPLACE FUNCTION check_course_enrollment_function()
RETURNS TRIGGER AS $$
DECLARE
    v_number_of_courses INTEGER;
    v_tot_credits NUMERIC;
    v_new_credits NUMERIC;
    v_semester_credits NUMERIC;
    v_current_enrollments INTEGER;
    v_course_capacity INTEGER;
    v_student_id VARCHAR(10);
BEGIN

    v_student_id := NEW.student_id;

    SELECT count(*) INTO v_number_of_courses
    FROM student_courses
    WHERE student_id = v_student_id
    AND session = NEW.session
    AND semester = NEW.semester;

    IF v_number_of_courses + 1 > 5 THEN
        RAISE EXCEPTION 'invalid';
    END IF;

    SELECT tot_credits INTO v_tot_credits
    FROM student
    WHERE student_id = v_student_id;

    SELECT credits INTO v_new_credits
    FROM courses
    WHERE course_id = NEW.course_id;

    IF COALESCE(v_tot_credits, 0) + COALESCE(v_new_credits, 0) > 60 THEN
        RAISE EXCEPTION 'invalid';
    END IF;

    SELECT COALESCE(SUM(credits), 0)
    INTO v_semester_credits
    FROM student_semester_summary
    WHERE student_id = v_student_id AND session = NEW.session AND semester = NEW.semester;

    IF v_semester_credits + COALESCE(v_new_credits, 0) > 26 THEN
        RAISE EXCEPTION 'invalid';
    END IF;

    SELECT enrollments, capacity
    INTO v_current_enrollments, v_course_capacity
    FROM course_offers
    WHERE course_id = NEW.course_id
        AND session = NEW.session
        AND semester = NEW.semester;

    -- Check if the capacity is full
    IF v_current_enrollments >= v_course_capacity THEN
        RAISE EXCEPTION 'course is full';
    END IF;
    
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;


CREATE OR REPLACE TRIGGER check_course_enrollment
BEFORE INSERT
ON student_courses
FOR EACH ROW
EXECUTE FUNCTION check_course_enrollment_function();

-- Create student semester summary view
CREATE MATERIALIZED VIEW student_semester_summary AS
SELECT
    sc.student_id,
    sc.session,
    sc.semester,
    ROUND(SUM(COALESCE(c.credits, 0) * sc.grade) / SUM(COALESCE(c.credits, 0)), 2) AS sgpa,
    SUM(COALESCE(c.credits, 0)) AS credits
FROM
    student_courses sc
LEFT JOIN
    courses c ON sc.course_id = c.course_id
WHERE
    sc.grade >= 5.0
GROUP BY
    sc.student_id, sc.session, sc.semester;

-- 2.2.5
CREATE OR REPLACE FUNCTION delete_student_semester_summary()
RETURNS TRIGGER AS $$
DECLARE
    new_credits INTEGER;
BEGIN
    SELECT credits INTO new_credits
    FROM courses
    WHERE course_id = OLD.course_id;

    REFRESH MATERIALIZED VIEW student_semester_summary;
    RAISE NOTICE 'credits: %', new_credits;
    RAISE NOTICE 'course id: %', OLD.course_id;

    UPDATE student
    SET tot_credits = tot_credits - COALESCE(new_credits, 0)
    WHERE student_id = OLD.student_id;

    RETURN OLD;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION update_student_semester_summary()
RETURNS TRIGGER AS $$
DECLARE
    new_credits INTEGER;
BEGIN
    SELECT credits INTO new_credits
    FROM courses
    WHERE course_id = NEW.course_id;

    IF TG_OP = 'INSERT' THEN
        REFRESH MATERIALIZED VIEW student_semester_summary;
        RAISE NOTICE 'credits: %', new_credits;
        RAISE NOTICE 'STUDENT id: %', NEW.student_id;
        UPDATE student
        SET tot_credits = tot_credits + new_credits
        WHERE student_id = NEW.student_id;

    ELSIF TG_OP = 'UPDATE' THEN
        REFRESH MATERIALIZED VIEW student_semester_summary;

    -- ELSIF TG_OP = 'DELETE' THEN
    --     PERFORM delete_student_semester_summary(); -- Call the separate function for DELETE
    END IF;

    RETURN OLD;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE TRIGGER student_courses_insert
AFTER INSERT ON student_courses
FOR EACH ROW
EXECUTE FUNCTION update_student_semester_summary();

CREATE OR REPLACE TRIGGER student_courses_update
AFTER UPDATE ON student_courses
FOR EACH ROW
EXECUTE FUNCTION update_student_semester_summary();

CREATE OR REPLACE TRIGGER student_courses_delete
BEFORE DELETE ON student_courses
FOR EACH ROW
EXECUTE FUNCTION delete_student_semester_summary();

-- 2.3.1 and 2.3.2

CREATE OR REPLACE FUNCTION check_course_professor_function()
RETURNS TRIGGER AS $$
DECLARE
    v_professor_courses_count INTEGER;
    v_professor_resign_year INTEGER;
BEGIN
    -- Check if course_id exists in courses table
    IF NOT EXISTS (
        SELECT 1
        FROM courses
        WHERE course_id = NEW.course_id
    ) THEN
        RAISE EXCEPTION 'invalid';
    END IF;

    -- Check if professor_id exists in professor table
    IF NOT EXISTS (
        SELECT 1
        FROM professor
        WHERE professor_id = NEW.professor_id
    ) THEN
        RAISE EXCEPTION 'invalid';
    END IF;

    SELECT COUNT(*) INTO v_professor_courses_count
    FROM course_offers
    WHERE professor_id = NEW.professor_id
        AND session = NEW.session;

    IF v_professor_courses_count + 1 > 4 THEN
        RAISE EXCEPTION 'invalid';
    END IF;

    -- Check if the course is being offered before the associated professor resigns
    SELECT resign_year INTO v_professor_resign_year
    FROM professor
    WHERE professor_id = NEW.professor_id;

    IF v_professor_resign_year IS NOT NULL AND v_professor_resign_year < SUBSTRING(NEW.session FROM 6 FOR 9)::INTEGER THEN
        RAISE EXCEPTION 'invalid';
    END IF;

    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE TRIGGER check_course_and_professor_exist
BEFORE INSERT
ON course_offers
FOR EACH ROW
EXECUTE FUNCTION check_course_professor_function();


-- 2.4.1
-- double full need to verify
CREATE OR REPLACE FUNCTION update_department()
RETURNS TRIGGER AS $$
BEGIN
    IF OLD.dept_id <> NEW.dept_id THEN
        -- Insert new entries with updated department ID

        IF EXISTS (
            SELECT * FROM 
            student_courses AS sc
            JOIN student AS s 
            ON sc.student_id = s.student_id AND s.dept_id = OLD.dept_id
            
        ) THEN
            RAISE EXCEPTION 'Department has students';
        END IF;

        INSERT INTO valid_entry (dept_id, entry_year, seq_number) 
        SELECT NEW.dept_id, entry_year, 1
        FROM valid_entry
        WHERE dept_id = OLD.dept_id;

        INSERT INTO course_offers (course_id, session, semester, professor_id, capacity, enrollments)
        SELECT NEW.dept_id || SUBSTRING(course_id FROM 4), session, semester, professor_id, capacity, enrollments
        FROM course_offers
        WHERE course_id LIKE OLD.dept_id || '%';

        INSERT INTO courses (course_id, course_name, course_desc, credits, dept_id)
        SELECT NEW.dept_id || SUBSTRING(course_id FROM 4), course_name, course_desc, credits, NEW.dept_id
        FROM courses
        WHERE course_id LIKE OLD.dept_id || '%';

        INSERT INTO student_courses (student_id, course_id, session, semester, grade)
        SELECT student_id, NEW.dept_id || SUBSTRING(course_id FROM 4), session, semester, grade
        FROM student_courses
        WHERE course_id LIKE OLD.dept_id || '%';

        -- Update professor and student tables
        UPDATE professor
        SET dept_id = NEW.dept_id
        WHERE dept_id = OLD.dept_id;

        UPDATE student
        SET dept_id = NEW.dept_id
        WHERE dept_id = OLD.dept_id;

        -- Remove old entries
        DELETE FROM course_offers WHERE course_id LIKE OLD.dept_id || '%';
        DELETE FROM courses WHERE course_id LIKE OLD.dept_id || '%';
        DELETE FROM student_courses WHERE course_id LIKE OLD.dept_id || '%';
        DELETE FROM valid_entry WHERE dept_id LIKE OLD.dept_id;
        
    END IF;

    RETURN NULL;
END;
$$ LANGUAGE plpgsql;

-- Create the trigger
CREATE OR REPLACE TRIGGER department_update
AFTER UPDATE on department
FOR EACH ROW
EXECUTE FUNCTION update_department();

