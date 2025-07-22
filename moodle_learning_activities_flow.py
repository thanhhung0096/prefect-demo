import random
import time
from datetime import datetime, timedelta
from typing import List, Dict, Any
import pandas as pd
from prefect import flow, task, get_run_logger
from prefect.futures import wait


# Mock data generators for different activity types
def generate_mock_assignment_data(num_records: int = 100) -> List[Dict[str, Any]]:
    """Generate mock assignment activity data"""
    mock_data = []
    for i in range(num_records):
        base_time = datetime.now() - timedelta(days=random.randint(1, 90))
        mock_data.append({
            'lms_la_lms_course_id': str(random.randint(1000, 9999)),
            'lms_la_lms_student_id': str(random.randint(10000, 99999)),
            'lms_la_activity_id': f'assign_{random.randint(100, 999)}_{random.randint(0, 5)}',
            'lms_la_activity_type': 'assignment',
            'lms_la_title': f'Assignment {i + 1}: {random.choice(["Essay", "Project", "Report", "Analysis"])}',
            'lms_la_status': random.choice(['published', 'unpublished']),
            'lms_la_published_date': base_time.isoformat(),
            'lms_la_unlocked_date': (base_time + timedelta(hours=1)).isoformat(),
            'lms_la_locked_date': (base_time + timedelta(days=14)).isoformat(),
            'lms_la_due_date': (base_time + timedelta(days=7)).isoformat(),
            'lms_la_time_limit': random.choice([None, 60, 120, 180]),
            'lms_la_scoring_policy': random.choice(['single_attempt', 'manual', 'until_pass']),
            'lms_la_points_possible': random.choice([10, 20, 50, 100]),
            'lms_la_allowed_attempts': random.choice([1, 3, 5, None]),
            'lms_la_result_id': str(random.randint(1000, 9999)),
            'lms_la_score': round(random.uniform(0, 100), 2),
            'lms_la_kept_score': round(random.uniform(60, 100), 2),
            'lms_la_attempt': random.randint(1, 3),
            'lms_la_grade_viewable': (base_time + timedelta(days=8)).isoformat(),
            'lms_la_has_seen_results': None,
            'lms_la_total_attempts': random.randint(1, 5),
            'lms_la_started_date': (base_time + timedelta(days=2)).isoformat(),
            'lms_la_finished_date': (base_time + timedelta(days=6)).isoformat(),
            'lms_la_time_taken': round(random.uniform(30, 240), 2)
        })
    return mock_data


def generate_mock_quiz_data(num_records: int = 150) -> List[Dict[str, Any]]:
    """Generate mock quiz activity data"""
    mock_data = []
    for i in range(num_records):
        base_time = datetime.now() - timedelta(days=random.randint(1, 60))
        mock_data.append({
            'lms_la_lms_course_id': str(random.randint(1000, 9999)),
            'lms_la_lms_student_id': str(random.randint(10000, 99999)),
            'lms_la_activity_id': f'quiz_{random.randint(100, 999)}_{random.randint(1, 3)}',
            'lms_la_activity_type': 'quiz',
            'lms_la_title': f'Quiz {i + 1}: {random.choice(["Chapter Test", "Midterm", "Final", "Practice Quiz"])}',
            'lms_la_status': random.choice(['published', 'unpublished']),
            'lms_la_published_date': base_time.isoformat(),
            'lms_la_unlocked_date': base_time.isoformat(),
            'lms_la_locked_date': (base_time + timedelta(days=7)).isoformat(),
            'lms_la_due_date': (base_time + timedelta(days=7)).isoformat(),
            'lms_la_time_limit': random.choice([30, 60, 90, 120]),
            'lms_la_scoring_policy': random.choice(['highest', 'average', 'first', 'last']),
            'lms_la_points_possible': random.choice([20, 50, 100]),
            'lms_la_allowed_attempts': random.choice([1, 2, 3, None]),
            'lms_la_result_id': str(random.randint(1000, 9999)),
            'lms_la_score': round(random.uniform(0, 100), 2),
            'lms_la_kept_score': round(random.uniform(70, 100), 2),
            'lms_la_attempt': random.randint(1, 3),
            'lms_la_grade_viewable': (base_time + timedelta(days=1)).isoformat(),
            'lms_la_has_seen_results': None,
            'lms_la_total_attempts': random.randint(1, 3),
            'lms_la_started_date': (base_time + timedelta(hours=2)).isoformat(),
            'lms_la_finished_date': (base_time + timedelta(hours=3)).isoformat(),
            'lms_la_time_taken': round(random.uniform(15, 120), 2)
        })
    return mock_data


def generate_mock_lesson_data(num_records: int = 80) -> List[Dict[str, Any]]:
    """Generate mock lesson activity data"""
    mock_data = []
    for i in range(num_records):
        base_time = datetime.now() - timedelta(days=random.randint(1, 45))
        mock_data.append({
            'lms_la_lms_course_id': str(random.randint(1000, 9999)),
            'lms_la_lms_student_id': str(random.randint(10000, 99999)),
            'lms_la_activity_id': f'lesson_{random.randint(100, 999)}_{random.randint(0, 2)}',
            'lms_la_activity_type': 'lesson',
            'lms_la_title': f'Lesson {i + 1}: {random.choice(["Introduction", "Advanced Topics", "Case Study", "Review"])}',
            'lms_la_status': random.choice(['published', 'unpublished']),
            'lms_la_published_date': base_time.isoformat(),
            'lms_la_unlocked_date': base_time.isoformat(),
            'lms_la_locked_date': (base_time + timedelta(days=30)).isoformat(),
            'lms_la_due_date': (base_time + timedelta(days=30)).isoformat(),
            'lms_la_time_limit': random.choice([None, 45, 60, 90]),
            'lms_la_scoring_policy': random.choice(['retake_allowed', 'single_attempt']),
            'lms_la_points_possible': random.choice([10, 20, 30]),
            'lms_la_allowed_attempts': random.choice([1, None]),
            'lms_la_result_id': str(random.randint(1000, 9999)),
            'lms_la_score': round(random.uniform(0, 30), 2),
            'lms_la_kept_score': round(random.uniform(20, 30), 2),
            'lms_la_attempt': random.randint(1, 2),
            'lms_la_grade_viewable': (base_time + timedelta(hours=1)).isoformat(),
            'lms_la_has_seen_results': None,
            'lms_la_total_attempts': random.randint(1, 2),
            'lms_la_started_date': base_time.isoformat(),
            'lms_la_finished_date': (base_time + timedelta(hours=1)).isoformat(),
            'lms_la_time_taken': round(random.uniform(30, 90), 2)
        })
    return mock_data


def generate_mock_h5p_data(num_records: int = 60) -> List[Dict[str, Any]]:
    """Generate mock H5P activity data"""
    mock_data = []
    for i in range(num_records):
        base_time = datetime.now() - timedelta(days=random.randint(1, 30))
        mock_data.append({
            'lms_la_lms_course_id': str(random.randint(1000, 9999)),
            'lms_la_lms_student_id': str(random.randint(10000, 99999)),
            'lms_la_activity_id': f'h5p_{random.randint(100, 999)}_{random.randint(1, 5)}',
            'lms_la_activity_type': 'h5p',
            'lms_la_title': f'Interactive Content {i + 1}: {random.choice(["Video Quiz", "Interactive Presentation", "Memory Game", "Timeline"])}',
            'lms_la_status': 'published',
            'lms_la_published_date': base_time.isoformat(),
            'lms_la_unlocked_date': None,
            'lms_la_locked_date': None,
            'lms_la_due_date': None,
            'lms_la_time_limit': None,
            'lms_la_scoring_policy': 'keep_highest',
            'lms_la_points_possible': random.choice([5, 10, 15]),
            'lms_la_allowed_attempts': None,
            'lms_la_result_id': str(random.randint(1000, 9999)),
            'lms_la_score': round(random.uniform(0, 15), 2),
            'lms_la_kept_score': round(random.uniform(10, 15), 2),
            'lms_la_attempt': random.randint(1, 5),
            'lms_la_grade_viewable': base_time.isoformat(),
            'lms_la_has_seen_results': None,
            'lms_la_total_attempts': random.randint(1, 5),
            'lms_la_started_date': base_time.isoformat(),
            'lms_la_finished_date': (base_time + timedelta(minutes=random.randint(5, 30))).isoformat(),
            'lms_la_time_taken': round(random.uniform(5, 30), 2)
        })
    return mock_data


def generate_mock_other_data(num_records: int = 40) -> List[Dict[str, Any]]:
    """Generate mock other gradeable activities data"""
    mock_data = []
    activity_types = ['forum', 'workshop', 'glossary', 'wiki', 'choice']
    for i in range(num_records):
        activity_type = random.choice(activity_types)
        base_time = datetime.now() - timedelta(days=random.randint(1, 60))
        mock_data.append({
            'lms_la_lms_course_id': str(random.randint(1000, 9999)),
            'lms_la_lms_student_id': str(random.randint(10000, 99999)),
            'lms_la_activity_id': f'{activity_type}_{random.randint(100, 999)}_{random.randint(10000, 99999)}',
            'lms_la_activity_type': activity_type,
            'lms_la_title': f'{activity_type.title()} Activity {i + 1}',
            'lms_la_status': random.choice(['published', 'unpublished']),
            'lms_la_published_date': base_time.isoformat(),
            'lms_la_unlocked_date': None,
            'lms_la_locked_date': None,
            'lms_la_due_date': None,
            'lms_la_time_limit': None,
            'lms_la_scoring_policy': None,
            'lms_la_points_possible': random.choice([5, 10, 20]),
            'lms_la_allowed_attempts': None,
            'lms_la_result_id': str(random.randint(1000, 9999)),
            'lms_la_score': round(random.uniform(0, 20), 2),
            'lms_la_kept_score': round(random.uniform(10, 20), 2),
            'lms_la_attempt': 1,
            'lms_la_grade_viewable': base_time.isoformat(),
            'lms_la_has_seen_results': None,
            'lms_la_total_attempts': 1,
            'lms_la_started_date': base_time.isoformat(),
            'lms_la_finished_date': (base_time + timedelta(hours=random.randint(1, 24))).isoformat(),
            'lms_la_time_taken': None
        })
    return mock_data


# SQL Queries for logging and monitoring
SQL_QUERIES = {
    "assignments": """
-- Assignment Activities with improved JOIN logic
SELECT 
    CAST(c.id AS CHAR(50)) as lms_la_lms_course_id,
    CAST(COALESCE(asub.userid, ag.userid) AS CHAR(50)) as lms_la_lms_student_id,
    CONCAT('assign_', CAST(a.id AS CHAR(20)), '_', CAST(COALESCE(asub.attemptnumber, ag.attemptnumber, 0) AS CHAR(10))) as lms_la_activity_id,
    'assignment' as lms_la_activity_type,
    a.name as lms_la_title,
    CASE 
        WHEN cm.visible = 1 THEN 'published'
        WHEN cm.visible = 0 THEN 'unpublished'
        ELSE 'unknown'
    END as lms_la_status,
    CASE WHEN a.timemodified > 0 THEN FROM_UNIXTIME(a.timemodified) ELSE NULL END as lms_la_published_date,
    CASE WHEN a.allowsubmissionsfromdate > 0 THEN FROM_UNIXTIME(a.allowsubmissionsfromdate) ELSE NULL END as lms_la_unlocked_date,
    CASE WHEN a.cutoffdate > 0 THEN FROM_UNIXTIME(a.cutoffdate) ELSE NULL END as lms_la_locked_date,
    CASE WHEN a.duedate > 0 THEN FROM_UNIXTIME(a.duedate) ELSE NULL END as lms_la_due_date,
    CASE WHEN a.timelimit > 0 THEN ROUND(a.timelimit / 60.0, 2) ELSE NULL END as lms_la_time_limit,
    CASE 
        WHEN a.attemptreopenmethod = 'manual' THEN 'manual'
        WHEN a.attemptreopenmethod = 'untilpass' THEN 'until_pass'
        WHEN a.attemptreopenmethod = 'none' THEN 'single_attempt'
        ELSE COALESCE(a.attemptreopenmethod, 'single_attempt')
    END as lms_la_scoring_policy,
    a.grade as lms_la_points_possible,
    CASE WHEN a.maxattempts = -1 THEN NULL ELSE a.maxattempts END as lms_la_allowed_attempts,
    ag.id as lms_la_result_id,
    ag.grade as lms_la_score,
    ag.grade as lms_la_kept_score,
    COALESCE(asub.attemptnumber, ag.attemptnumber, 0) as lms_la_attempt,
    CASE WHEN ag.timemodified > 0 THEN FROM_UNIXTIME(ag.timemodified) ELSE NULL END as lms_la_grade_viewable,
    NULL as lms_la_has_seen_results,
    asub_stats.total_attempts as lms_la_total_attempts,
    CASE WHEN asub.timestarted > 0 THEN FROM_UNIXTIME(asub.timestarted) ELSE NULL END as lms_la_started_date,
    CASE WHEN asub.timemodified > 0 THEN FROM_UNIXTIME(asub.timemodified) ELSE NULL END as lms_la_finished_date,
    CASE 
        WHEN asub.timestarted > 0 AND asub.timemodified > asub.timestarted 
        THEN ROUND((asub.timemodified - asub.timestarted) / 60.0, 2)
        ELSE NULL 
    END as lms_la_time_taken
FROM mdl_assign a
JOIN mdl_course_modules cm ON cm.instance = a.id 
JOIN mdl_modules m ON m.id = cm.module AND m.name = 'assign'
JOIN mdl_course c ON c.id = a.course
LEFT JOIN mdl_assign_submission asub ON asub.assignment = a.id AND asub.latest = 1
LEFT JOIN mdl_assign_grades ag ON ag.assignment = a.id AND ag.userid = asub.userid AND ag.attemptnumber = asub.attemptnumber
LEFT JOIN (
    SELECT assignment, userid, COUNT(*) as total_attempts
    FROM mdl_assign_submission 
    GROUP BY assignment, userid
) asub_stats ON asub_stats.assignment = a.id AND asub_stats.userid = COALESCE(asub.userid, ag.userid)
WHERE (asub.id IS NOT NULL OR ag.id IS NOT NULL);
""",

    "quizzes": """
-- Quiz Activities with optimized subqueries
SELECT 
    CAST(c.id AS CHAR(50)) as lms_la_lms_course_id,
    CAST(qa.userid AS CHAR(50)) as lms_la_lms_student_id,
    CONCAT('quiz_', CAST(q.id AS CHAR(20)), '_', CAST(qa.attempt AS CHAR(10))) as lms_la_activity_id,
    'quiz' as lms_la_activity_type,
    q.name as lms_la_title,
    CASE 
        WHEN cm.visible = 1 THEN 'published'
        WHEN cm.visible = 0 THEN 'unpublished'
        ELSE 'unknown'
    END as lms_la_status,
    CASE WHEN q.timecreated > 0 THEN FROM_UNIXTIME(q.timecreated) ELSE NULL END as lms_la_published_date,
    CASE WHEN q.timeopen > 0 THEN FROM_UNIXTIME(q.timeopen) ELSE NULL END as lms_la_unlocked_date,
    CASE WHEN q.timeclose > 0 THEN FROM_UNIXTIME(q.timeclose) ELSE NULL END as lms_la_locked_date,
    CASE WHEN q.timeclose > 0 THEN FROM_UNIXTIME(q.timeclose) ELSE NULL END as lms_la_due_date,
    CASE WHEN q.timelimit > 0 THEN ROUND(q.timelimit / 60.0, 2) ELSE NULL END as lms_la_time_limit,
    CASE 
        WHEN q.grademethod = 1 THEN 'highest'
        WHEN q.grademethod = 2 THEN 'average'
        WHEN q.grademethod = 3 THEN 'first'
        WHEN q.grademethod = 4 THEN 'last'
        ELSE 'highest'
    END as lms_la_scoring_policy,
    q.grade as lms_la_points_possible,
    CASE WHEN q.attempts = 0 THEN NULL ELSE q.attempts END as lms_la_allowed_attempts,
    qa.id as lms_la_result_id,
    qa.sumgrades as lms_la_score,
    qa_stats.kept_score as lms_la_kept_score,
    qa.attempt as lms_la_attempt,
    CASE WHEN qa.timefinish > 0 THEN FROM_UNIXTIME(qa.timefinish) ELSE NULL END as lms_la_grade_viewable,
    NULL as lms_la_has_seen_results,
    qa_stats.total_attempts as lms_la_total_attempts,
    CASE WHEN qa.timestart > 0 THEN FROM_UNIXTIME(qa.timestart) ELSE NULL END as lms_la_started_date,
    CASE WHEN qa.timefinish > 0 THEN FROM_UNIXTIME(qa.timefinish) ELSE NULL END as lms_la_finished_date,
    CASE 
        WHEN qa.timestart > 0 AND qa.timefinish > qa.timestart 
        THEN ROUND((qa.timefinish - qa.timestart) / 60.0, 2)
        ELSE NULL 
    END as lms_la_time_taken
FROM mdl_quiz q
JOIN mdl_course_modules cm ON cm.instance = q.id
JOIN mdl_modules m ON m.id = cm.module AND m.name = 'quiz'
JOIN mdl_course c ON c.id = q.course
JOIN mdl_quiz_attempts qa ON qa.quiz = q.id AND qa.state = 'finished'
LEFT JOIN (
    SELECT qa2.quiz, qa2.userid, COUNT(*) as total_attempts,
           CASE 
               WHEN MAX(CASE WHEN q2.grademethod = 1 THEN qa2.sumgrades END) IS NOT NULL THEN MAX(qa2.sumgrades)
               WHEN MAX(CASE WHEN q2.grademethod = 2 THEN qa2.sumgrades END) IS NOT NULL THEN AVG(qa2.sumgrades)
               WHEN MAX(CASE WHEN q2.grademethod = 3 THEN qa2.sumgrades END) IS NOT NULL THEN MIN(qa2.sumgrades)
               WHEN MAX(CASE WHEN q2.grademethod = 4 THEN qa2.sumgrades END) IS NOT NULL THEN MAX(qa2.sumgrades)
               ELSE MAX(qa2.sumgrades)
           END as kept_score
    FROM mdl_quiz_attempts qa2
    JOIN mdl_quiz q2 ON q2.id = qa2.quiz
    WHERE qa2.state = 'finished'
    GROUP BY qa2.quiz, qa2.userid
) qa_stats ON qa_stats.quiz = q.id AND qa_stats.userid = qa.userid;
""",

    "lessons": """
-- Lesson Activities with improved performance
SELECT 
    CAST(c.id AS CHAR(50)) as lms_la_lms_course_id,
    CAST(lg.userid AS CHAR(50)) as lms_la_lms_student_id,
    CONCAT('lesson_', CAST(l.id AS CHAR(20)), '_', CAST(COALESCE(lesson_stats.max_retry, 0) AS CHAR(10))) as lms_la_activity_id,
    'lesson' as lms_la_activity_type,
    l.name as lms_la_title,
    CASE 
        WHEN cm.visible = 1 THEN 'published'
        WHEN cm.visible = 0 THEN 'unpublished'
        ELSE 'unknown'
    END as lms_la_status,
    CASE WHEN l.available > 0 THEN FROM_UNIXTIME(l.available) ELSE NULL END as lms_la_published_date,
    CASE WHEN l.available > 0 THEN FROM_UNIXTIME(l.available) ELSE NULL END as lms_la_unlocked_date,
    CASE WHEN l.deadline > 0 THEN FROM_UNIXTIME(l.deadline) ELSE NULL END as lms_la_locked_date,
    CASE WHEN l.deadline > 0 THEN FROM_UNIXTIME(l.deadline) ELSE NULL END as lms_la_due_date,
    CASE WHEN l.timelimit > 0 THEN ROUND(l.timelimit / 60.0, 2) ELSE NULL END as lms_la_time_limit,
    CASE 
        WHEN l.retake = 1 THEN 'retake_allowed'
        ELSE 'single_attempt'
    END as lms_la_scoring_policy,
    l.grade as lms_la_points_possible,
    CASE WHEN l.retake = 1 THEN NULL ELSE 1 END as lms_la_allowed_attempts,
    lg.id as lms_la_result_id,
    lg.grade as lms_la_score,
    lg.grade as lms_la_kept_score,
    COALESCE(lesson_stats.max_retry, 0) + 1 as lms_la_attempt,
    CASE WHEN lg.completed > 0 THEN FROM_UNIXTIME(lg.completed) ELSE NULL END as lms_la_grade_viewable,
    NULL as lms_la_has_seen_results,
    COALESCE(lesson_stats.total_attempts, 1) as lms_la_total_attempts,
    CASE WHEN lesson_stats.first_attempt > 0 THEN FROM_UNIXTIME(lesson_stats.first_attempt) ELSE NULL END as lms_la_started_date,
    CASE WHEN lg.completed > 0 THEN FROM_UNIXTIME(lg.completed) ELSE NULL END as lms_la_finished_date,
    CASE 
        WHEN lesson_stats.first_attempt > 0 AND lg.completed > lesson_stats.first_attempt 
        THEN ROUND((lg.completed - lesson_stats.first_attempt) / 60.0, 2)
        ELSE NULL 
    END as lms_la_time_taken
FROM mdl_lesson l
JOIN mdl_course_modules cm ON cm.instance = l.id
JOIN mdl_modules m ON m.id = cm.module AND m.name = 'lesson'
JOIN mdl_course c ON c.id = l.course
JOIN mdl_lesson_grades lg ON lg.lessonid = l.id
LEFT JOIN (
    SELECT lessonid, userid, 
           MAX(retry) as max_retry,
           COUNT(DISTINCT retry) as total_attempts,
           MIN(timeseen) as first_attempt
    FROM mdl_lesson_attempts
    GROUP BY lessonid, userid
) lesson_stats ON lesson_stats.lessonid = l.id AND lesson_stats.userid = lg.userid;
""",

    "h5p": """
-- H5P Activities with standardized structure
SELECT 
    CAST(c.id AS CHAR(50)) as lms_la_lms_course_id,
    CAST(ha.userid AS CHAR(50)) as lms_la_lms_student_id,
    CONCAT('h5p_', CAST(h.id AS CHAR(20)), '_', CAST(ha.attempt AS CHAR(10))) as lms_la_activity_id,
    'h5p' as lms_la_activity_type,
    h.name as lms_la_title,
    CASE 
        WHEN cm.visible = 1 THEN 'published'
        WHEN cm.visible = 0 THEN 'unpublished'
        ELSE 'unknown'
    END as lms_la_status,
    CASE WHEN h.timecreated > 0 THEN FROM_UNIXTIME(h.timecreated) ELSE NULL END as lms_la_published_date,
    NULL as lms_la_unlocked_date,
    NULL as lms_la_locked_date,
    NULL as lms_la_due_date,
    NULL as lms_la_time_limit,
    'keep_highest' as lms_la_scoring_policy,
    h.grade as lms_la_points_possible,
    NULL as lms_la_allowed_attempts,
    ha.id as lms_la_result_id,
    ha.rawscore as lms_la_score,
    ha_stats.kept_score as lms_la_kept_score,
    ha.attempt as lms_la_attempt,
    CASE WHEN ha.timemodified > 0 THEN FROM_UNIXTIME(ha.timemodified) ELSE NULL END as lms_la_grade_viewable,
    NULL as lms_la_has_seen_results,
    ha_stats.total_attempts as lms_la_total_attempts,
    CASE WHEN ha.timecreated > 0 THEN FROM_UNIXTIME(ha.timecreated) ELSE NULL END as lms_la_started_date,
    CASE WHEN ha.timemodified > 0 THEN FROM_UNIXTIME(ha.timemodified) ELSE NULL END as lms_la_finished_date,
    CASE WHEN ha.duration > 0 THEN ROUND(ha.duration / 60.0, 2) ELSE NULL END as lms_la_time_taken
FROM mdl_h5pactivity h
JOIN mdl_course_modules cm ON cm.instance = h.id
JOIN mdl_modules m ON m.id = cm.module AND m.name = 'h5pactivity'
JOIN mdl_course c ON c.id = h.course
JOIN mdl_h5pactivity_attempts ha ON ha.h5pactivityid = h.id
LEFT JOIN (
    SELECT h5pactivityid, userid, COUNT(*) as total_attempts, MAX(rawscore) as kept_score
    FROM mdl_h5pactivity_attempts
    GROUP BY h5pactivityid, userid
) ha_stats ON ha_stats.h5pactivityid = h.id AND ha_stats.userid = ha.userid;
""",

    "other_activities": """
-- Other Gradeable Activities with better filtering
SELECT 
    CAST(c.id AS CHAR(50)) as lms_la_lms_course_id,
    CAST(gg.userid AS CHAR(50)) as lms_la_lms_student_id,
    CONCAT(COALESCE(gi.itemmodule, 'unknown'), '_', CAST(gi.iteminstance AS CHAR(20)), '_', CAST(gg.userid AS CHAR(20))) as lms_la_activity_id,
    COALESCE(gi.itemmodule, 'unknown') as lms_la_activity_type,
    COALESCE(gi.itemname, 'Unnamed Activity') as lms_la_title,
    CASE 
        WHEN cm.visible = 1 THEN 'published'
        WHEN cm.visible = 0 THEN 'unpublished'
        ELSE 'unknown'
    END as lms_la_status,
    CASE WHEN gi.timecreated > 0 THEN FROM_UNIXTIME(gi.timecreated) ELSE NULL END as lms_la_published_date,
    NULL as lms_la_unlocked_date,
    NULL as lms_la_locked_date,
    NULL as lms_la_due_date,
    NULL as lms_la_time_limit,
    NULL as lms_la_scoring_policy,
    gi.grademax as lms_la_points_possible,
    NULL as lms_la_allowed_attempts,
    gg.id as lms_la_result_id,
    gg.finalgrade as lms_la_score,
    gg.finalgrade as lms_la_kept_score,
    1 as lms_la_attempt,
    CASE WHEN gg.timemodified > 0 THEN FROM_UNIXTIME(gg.timemodified) ELSE NULL END as lms_la_grade_viewable,
    NULL as lms_la_has_seen_results,
    1 as lms_la_total_attempts,
    CASE WHEN gg.timecreated > 0 THEN FROM_UNIXTIME(gg.timecreated) ELSE NULL END as lms_la_started_date,
    CASE WHEN gg.timemodified > 0 THEN FROM_UNIXTIME(gg.timemodified) ELSE NULL END as lms_la_finished_date,
    NULL as lms_la_time_taken
FROM mdl_grade_items gi
JOIN mdl_grade_grades gg ON gg.itemid = gi.id
JOIN mdl_course c ON c.id = gi.courseid
LEFT JOIN mdl_course_modules cm ON cm.instance = gi.iteminstance 
LEFT JOIN mdl_modules m ON m.id = cm.module AND m.name = gi.itemmodule
WHERE gi.itemtype = 'mod' 
  AND gi.itemmodule IS NOT NULL
  AND gi.itemmodule NOT IN ('assign', 'quiz', 'lesson', 'h5pactivity', 'scorm') 
  AND gg.finalgrade IS NOT NULL
  AND gg.finalgrade > 0;
"""
}


@task(name="Extract Assignment Data",
      description="Extract assignment activities with submission and grading data")
def extract_assignment_data() -> List[Dict[str, Any]]:
    """Extract assignment activity data with SQL logging and mock implementation"""
    logger = get_run_logger()

    # Log the SQL query being executed
    logger.info("🔍 SQL Query for Assignment Data:")
    logger.info("=" * 80)
    logger.info(SQL_QUERIES["assignments"])
    logger.info("=" * 80)

    # Random sleep to simulate database query execution time
    sleep_time = random.uniform(2, 8)
    logger.info(f"⏳ Executing assignment data extraction... (simulated processing time: {sleep_time:.2f}s)")
    time.sleep(sleep_time)

    # Generate mock data
    data = generate_mock_assignment_data(100)
    logger.info(f"✅ Successfully extracted {len(data)} assignment records")

    return data


@task(name="Extract Quiz Data",
      description="Extract quiz activities with attempt tracking and scoring")
def extract_quiz_data() -> List[Dict[str, Any]]:
    """Extract quiz activity data with SQL logging and mock implementation"""
    logger = get_run_logger()

    # Log the SQL query being executed
    logger.info("🔍 SQL Query for Quiz Data:")
    logger.info("=" * 80)
    logger.info(SQL_QUERIES["quizzes"])
    logger.info("=" * 80)

    sleep_time = random.uniform(3, 10)
    logger.info(f"⏳ Executing quiz data extraction... (simulated processing time: {sleep_time:.2f}s)")
    time.sleep(sleep_time)

    data = generate_mock_quiz_data(150)
    logger.info(f"✅ Successfully extracted {len(data)} quiz records")

    return data


@task(name="Extract Lesson Data",
      description="Extract lesson activities with completion and retry tracking")
def extract_lesson_data() -> List[Dict[str, Any]]:
    """Extract lesson activity data with SQL logging and mock implementation"""
    logger = get_run_logger()

    # Log the SQL query being executed
    logger.info("🔍 SQL Query for Lesson Data:")
    logger.info("=" * 80)
    logger.info(SQL_QUERIES["lessons"])
    logger.info("=" * 80)

    sleep_time = random.uniform(1, 6)
    logger.info(f"⏳ Executing lesson data extraction... (simulated processing time: {sleep_time:.2f}s)")
    time.sleep(sleep_time)

    data = generate_mock_lesson_data(80)
    logger.info(f"✅ Successfully extracted {len(data)} lesson records")

    return data


@task(name="Extract H5P Data",
      description="Extract H5P interactive content activities with attempt data")
def extract_h5p_data() -> List[Dict[str, Any]]:
    """Extract H5P activity data with SQL logging and mock implementation"""
    logger = get_run_logger()

    # Log the SQL query being executed
    logger.info("🔍 SQL Query for H5P Data:")
    logger.info("=" * 80)
    logger.info(SQL_QUERIES["h5p"])
    logger.info("=" * 80)

    sleep_time = random.uniform(1, 5)
    logger.info(f"⏳ Executing H5P data extraction... (simulated processing time: {sleep_time:.2f}s)")
    time.sleep(sleep_time)

    data = generate_mock_h5p_data(60)
    logger.info(f"✅ Successfully extracted {len(data)} H5P records")

    return data


@task(name="Extract Other Activities Data",
      description="Extract other gradeable activities (forums, workshops, etc.)")
def extract_other_activities_data() -> List[Dict[str, Any]]:
    """Extract other gradeable activities data with SQL logging and mock implementation"""
    logger = get_run_logger()

    # Log the SQL query being executed
    logger.info("🔍 SQL Query for Other Activities Data:")
    logger.info("=" * 80)
    logger.info(SQL_QUERIES["other_activities"])
    logger.info("=" * 80)

    sleep_time = random.uniform(1, 4)
    logger.info(f"⏳ Executing other activities data extraction... (simulated processing time: {sleep_time:.2f}s)")
    time.sleep(sleep_time)

    data = generate_mock_other_data(40)
    logger.info(f"✅ Successfully extracted {len(data)} other activity records")

    return data


@task(name="Combine and Process Data",
      description="Combine all extracted activity data and perform data quality checks")
def combine_and_process_data(
        assignments: List[Dict[str, Any]],
        quizzes: List[Dict[str, Any]],
        lessons: List[Dict[str, Any]],
        h5p: List[Dict[str, Any]],
        others: List[Dict[str, Any]]
) -> Dict[str, Any]:
    """Combine all activity data and generate summary statistics"""
    logger = get_run_logger()

    logger.info("🔄 Combining and processing all activity data...")

    # Combine all data
    all_data = assignments + quizzes + lessons + h5p + others

    # Convert to DataFrame for analysis
    df = pd.DataFrame(all_data)

    # Generate summary statistics
    summary = {
        "total_records": len(all_data),
        "activity_type_counts": df['lms_la_activity_type'].value_counts().to_dict(),
        "course_count": df['lms_la_lms_course_id'].nunique(),
        "student_count": df['lms_la_lms_student_id'].nunique(),
        "status_distribution": df['lms_la_status'].value_counts().to_dict(),
        "average_score": df['lms_la_score'].mean(),
        "score_statistics": {
            "min": df['lms_la_score'].min(),
            "max": df['lms_la_score'].max(),
            "mean": df['lms_la_score'].mean(),
            "median": df['lms_la_score'].median()
        },
        "extraction_timestamp": datetime.now().isoformat(),
        "data_quality_checks": {
            "null_scores": df['lms_la_score'].isnull().sum(),
            "null_titles": df['lms_la_title'].isnull().sum(),
            "duplicate_activity_ids": df['lms_la_activity_id'].duplicated().sum()
        }
    }

    logger.info(f"✅ Data processing completed. Total records: {summary['total_records']}")
    logger.info(f"📊 Activity type distribution: {summary['activity_type_counts']}")

    return {
        "data": all_data,
        "summary": summary,
        "sql_queries": SQL_QUERIES
    }


@flow(
    name="Moodle Learning Activities Data Pipeline - Concurrent",
    description="Concurrent extraction and processing of Moodle learning activities data using submit()",
    log_prints=True
)
def moodle_learning_activities_flow() -> Dict[str, Any]:
    """
    Main flow for extracting Moodle learning activities data using concurrent execution with submit().

    This flow demonstrates:
    - Concurrent execution using Prefect's submit() function
    - SQL query logging for monitoring and debugging
    - Mock data generation for testing and development
    - Comprehensive activity type coverage (assignments, quizzes, lessons, H5P, others)
    - Data quality checks and summary statistics
    """
    logger = get_run_logger()

    logger.info("🚀 Starting Moodle Learning Activities Data Pipeline (Concurrent)...")
    logger.info(
        "📊 This pipeline extracts data for: Assignments, Quizzes, Lessons, H5P Activities, and Other Gradeable Activities")
    logger.info("⚡ Using Prefect's submit() function for concurrent task execution")

    start_time = time.time()

    # Submit all extraction tasks concurrently using submit()
    logger.info("🔄 Submitting concurrent data extraction tasks...")

    assignments_future = extract_assignment_data.submit()
    quizzes_future = extract_quiz_data.submit()
    lessons_future = extract_lesson_data.submit()
    h5p_future = extract_h5p_data.submit()
    others_future = extract_other_activities_data.submit()

    # Wait for all futures to complete and get results
    logger.info("⏳ Waiting for all extraction tasks to complete...")

    assignments_data = assignments_future.result()
    quizzes_data = quizzes_future.result()
    lessons_data = lessons_future.result()
    h5p_data = h5p_future.result()
    others_data = others_future.result()

    logger.info("✅ All extraction tasks completed successfully!")

    # Process and combine all data
    logger.info("🔄 Processing and combining extracted data...")
    result = combine_and_process_data(
        assignments_data,
        quizzes_data,
        lessons_data,
        h5p_data,
        others_data
    )

    execution_time = time.time() - start_time

    logger.info(f"🎉 Pipeline completed successfully in {execution_time:.2f} seconds!")
    logger.info(f"📈 Processed {result['summary']['total_records']} total learning activity records")
    logger.info(
        f"🏫 Data from {result['summary']['course_count']} courses and {result['summary']['student_count']} students")

    # Add execution metadata
    result['execution_metadata'] = {
        "execution_time_seconds": execution_time,
        "pipeline_version": "2.0.0-concurrent",
        "execution_timestamp": datetime.now().isoformat(),
        "concurrency_method": "submit_function"
    }

    return result


if __name__ == "__main__":
    # Run the flow
    result = moodle_learning_activities_flow()

    # Print summary for demonstration
    print("\n" + "=" * 80)
    print("MOODLE LEARNING ACTIVITIES DATA PIPELINE - EXECUTION SUMMARY")
    print("=" * 80)
    print(f"Total Records Processed: {result['summary']['total_records']}")
    print(f"Execution Time: {result['execution_metadata']['execution_time_seconds']:.2f} seconds")
    print(f"Concurrency Method: {result['execution_metadata']['concurrency_method']}")
    print(f"Courses: {result['summary']['course_count']}")
    print(f"Students: {result['summary']['student_count']}")
    print("\nActivity Type Distribution:")
    for activity_type, count in result['summary']['activity_type_counts'].items():
        print(f"  {activity_type}: {count}")
    print("\nScore Statistics:")
    stats = result['summary']['score_statistics']
    print(f"  Average Score: {stats['mean']:.2f}")
    print(f"  Score Range: {stats['min']:.2f} - {stats['max']:.2f}")
    print("=" * 80)

    # Display SQL queries that were logged
    print("\n📋 SQL Queries logged during execution:")
    for query_name in result['sql_queries'].keys():
        print(f"  ✓ {query_name}")
    print("\n💡 Check the logs above to see the actual SQL queries that were executed!")
